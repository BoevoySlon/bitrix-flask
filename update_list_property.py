# update_list_property.py
import os
import re
import logging
import calendar
from datetime import datetime, date
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    # Планировщик для фонового режима (вариант с отдельным воркером)
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
except Exception:
    BackgroundScheduler = None
    CronTrigger = None

# ===== ЛОГИРОВАНИЕ =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("update_list_property")

# ===== КОНФИГ =====
BITRIX_URL = os.getenv("BITRIX_OUTGOING_URL", "").rstrip("/") + "/"

# Поддержим оба варианта переменных окружения для совместимости
def _env_int(*keys: str, default: int = 0) -> int:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            try:
                return int(v)
            except Exception:
                pass
    return default

LIST_ID = _env_int("BITRIX_LIST_ID", "LIST_ID", default=68)

def _normalize_prop_code(raw: Optional[str], fallback: str = "PROPERTY_202") -> str:
    s = (raw or fallback).strip()
    if s.isdigit():
        return f"PROPERTY_{s}"
    if not s.upper().startswith("PROPERTY_"):
        return "PROPERTY_" + s.strip("_").upper()
    return s.upper()

DATE_PROP = _normalize_prop_code(os.getenv("BITRIX_PROPERTY_CODE", os.getenv("DATE_PROP", "PROPERTY_202")))
# Список ELEMENT_ID можно передать строкой "1,2,3" или JSON "[1,2,3]"
def _parse_ids(raw: Optional[str]) -> List[int]:
    if not raw:
        return []
    raw = raw.strip()
    if raw.startswith("["):
        try:
            import json
            return [int(x) for x in json.loads(raw)]
        except Exception:
            pass
    return [int(x) for x in re.split(r"[,\s]+", raw) if x]

ELEMENT_IDS_RAW = os.getenv("BITRIX_ELEMENT_IDS") or os.getenv("ELEMENT_IDS") or ""
ELEMENT_IDS: List[int] = _parse_ids(ELEMENT_IDS_RAW)

# Таймауты/ретраи как в твоём коде
CONNECT_TIMEOUT = float(os.getenv("BITRIX_CONNECT_TIMEOUT", "8"))
READ_TIMEOUT    = float(os.getenv("BITRIX_READ_TIMEOUT", "30"))
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

RETRY_TOTAL   = int(os.getenv("BITRIX_RETRY_TOTAL", "6"))
RETRY_CONNECT = int(os.getenv("BITRIX_RETRY_CONNECT", str(RETRY_TOTAL)))
RETRY_READ    = int(os.getenv("BITRIX_RETRY_READ", str(RETRY_TOTAL)))
BACKOFF       = float(os.getenv("BITRIX_BACKOFF", "0.8"))

SESSION = requests.Session()
retry = Retry(
    total=RETRY_TOTAL,
    connect=RETRY_CONNECT,
    read=RETRY_READ,
    backoff_factor=BACKOFF,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET", "POST"}),
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

# ===== УТИЛИТЫ ДАТЫ =====
def _today_moscow() -> date:
    try:
        from zoneinfo import ZoneInfo  # py>=3.9
        return datetime.now(ZoneInfo("Europe/Moscow")).date()
    except Exception:
        # Фоллбэк: берём локальную дату контейнера
        return datetime.now().date()

def last_day_of_current_month_moscow() -> str:
    t = _today_moscow()
    last = calendar.monthrange(t.year, t.month)[1]
    return date(t.year, t.month, last).isoformat()  # YYYY-MM-DD

# ===== ВСПОМОГАТЕЛЬНЫЕ ВЫЗОВЫ REST (form-data, как у тебя) =====
def bx_post_form(method: str, data_pairs: List[tuple[str, str]]) -> Dict[str, Any]:
    r = SESSION.post(f"{BITRIX_URL}{method}.json", data=data_pairs, timeout=TIMEOUT)
    # Развёрнутая диагностика ошибок
    if r.status_code >= 400:
        try:
            js = r.json()
        except Exception:
            js = {"raw": r.text[:1000]}
        raise RuntimeError(f"HTTP {r.status_code} at {method}: {js}")
    try:
        js = r.json()
    except Exception as e:
        raise RuntimeError(f"Non-JSON response at {method}: {r.text[:500]}") from e
    if isinstance(js, dict) and "error" in js:
        raise RuntimeError(f"Bitrix error at {method}: {js.get('error_description') or js.get('error')}")
    return js

def get_element(eid: int) -> Optional[Dict[str, Any]]:
    pairs = [
        ("IBLOCK_TYPE_ID", "lists"),
        ("IBLOCK_ID", str(LIST_ID)),
        ("filter[=ID]", str(eid)),
        ("select[]", "ID"),
        ("select[]", "NAME"),
    ]
    data = bx_post_form("lists.element.get", pairs)
    res = data.get("result")
    if isinstance(res, list) and res:
        return res[0]
    return None

def update_element_date(eid: int, name: str, iso_date: str) -> bool:
    pairs: List[tuple[str, str]] = [
        ("IBLOCK_TYPE_ID", "lists"),
        ("IBLOCK_ID", str(LIST_ID)),
        ("ELEMENT_ID", str(eid)),
        ("fields[NAME]", name or f"ID {eid}"),
        (f"fields[{DATE_PROP}]", iso_date),
    ]
    data = bx_post_form("lists.element.update", pairs)
    # У lists.element.update успешный ответ обычно {"result": true}
    return bool(data.get("result") is True)

# ===== ОСНОВНАЯ ЛОГИКА =====
def run_update_for_all() -> Dict[str, Any]:
    if not ELEMENT_IDS:
        log.warning("BITRIX_ELEMENT_IDS пуст — нечего обновлять.")
        return {"updated": 0, "failed": 0, "value": None}

    target = last_day_of_current_month_moscow()
    ok, fail = 0, 0
    for eid in ELEMENT_IDS:
        try:
            el = get_element(eid)
            if not el:
                raise RuntimeError("element not found")
            name = el.get("NAME") or f"ID {eid}"
            if update_element_date(eid, name, target):
                ok += 1
                log.info("OK: ELEMENT_ID=%s  %s=%s", eid, DATE_PROP, target)
            else:
                fail += 1
                log.error("FAILED (false result): ELEMENT_ID=%s", eid)
        except Exception as e:
            fail += 1
            log.error("FAILED: ELEMENT_ID=%s  error=%s", eid, e)
    return {"updated": ok, "failed": fail, "value": target}

# ===== CLI / SCHEDULER =====
def schedule_job():
    if BackgroundScheduler is None or CronTrigger is None:
        log.error("APScheduler не установлен. Добавь APScheduler в requirements.txt")
        # На всякий случай однократно выполним, чтобы не молчать
        return run_update_for_all()

    sched = BackgroundScheduler(timezone="Europe/Moscow")
    # каждое 1-е число в 00:01 мск
    trigger = CronTrigger(day="1", hour="0", minute="1")
    sched.add_job(run_update_for_all, trigger, id="lists-month-end", replace_existing=True)
    sched.start()
    log.info("Ежемесячная задача запущена (00:01 мск, 1 число).")
    return None

if __name__ == "__main__":
    import sys, time
    if "--once" in sys.argv:
        res = run_update_for_all()
        log.info("Done: %s", res)
    else:
        schedule_job()
        # держим процесс для воркера
        while True:
            time.sleep(3600)
