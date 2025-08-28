# update_list_property.py
import os
import re
import logging
import calendar
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
except Exception:
    BackgroundScheduler = None
    CronTrigger = None

# ========= ЛОГИ =========
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("update_list_property")

# ========= КОНФИГ =========
RAW_URL = os.getenv("BITRIX_OUTGOING_URL", "")
if not RAW_URL or not re.match(r"^https?://", RAW_URL.strip()):
    raise RuntimeError(
        f"BITRIX_OUTGOING_URL пуст или без схемы: {RAW_URL!r}. "
        "Ожидается вида 'https://<portal>.bitrix24.ru/rest/<id>/<token>/'"
    )
BITRIX_URL = RAW_URL.rstrip("/") + "/"

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

ELEMENT_IDS: List[int] = _parse_ids(os.getenv("BITRIX_ELEMENT_IDS") or os.getenv("ELEMENT_IDS") or "")

# ========= HTTP (ретраи/таймауты) =========
CONNECT_TIMEOUT = float(os.getenv("BITRIX_CONNECT_TIMEOUT", "15"))
READ_TIMEOUT    = float(os.getenv("BITRIX_READ_TIMEOUT", "60"))
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

RETRY_TOTAL   = int(os.getenv("BITRIX_RETRY_TOTAL", "6"))
RETRY_CONNECT = int(os.getenv("BITRIX_RETRY_CONNECT", str(RETRY_TOTAL)))
RETRY_READ    = int(os.getenv("BITRIX_RETRY_READ", str(RETRY_TOTAL)))
BACKOFF       = float(os.getenv("BITRIX_BACKOFF", "0.8"))

SESSION = requests.Session()
SESSION.headers.update({"Connection": "close"})  # меньше залипов keep-alive
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

# ========= ДАТЫ =========
def _today_moscow() -> date:
    try:
        from zoneinfo import ZoneInfo  # py>=3.9
        return datetime.now(ZoneInfo("Europe/Moscow")).date()
    except Exception:
        return datetime.now().date()

def last_day_of_current_month_moscow_ddmmyyyy() -> str:
    t = _today_moscow()
    last = calendar.monthrange(t.year, t.month)[1]
    d = date(t.year, t.month, last)
    # dd.mm.YYYY
    return f"{d.day:02d}.{d.month:02d}.{d.year:04d}"

# ========= REST helpers (form-data) =========
def bx_post_form(method: str, data_pairs: List[Tuple[str, str]]) -> Dict[str, Any]:
    url = f"{BITRIX_URL}{method}.json"
    r = SESSION.post(url, data=data_pairs, timeout=TIMEOUT)
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

# ========= Получение обязательных полей и текущих значений =========
def get_required_field_codes() -> List[Dict[str, Any]]:
    """Возвращает объекты полей с IS_REQUIRED=Y (кроме NAME)."""
    pairs = [("IBLOCK_TYPE_ID", "lists"), ("IBLOCK_ID", str(LIST_ID))]
    js = bx_post_form("lists.field.get", pairs)
    out: List[Dict[str, Any]] = []
    for f in (js.get("result") or []):
        if f.get("IS_REQUIRED") == "Y":
            code = f.get("ID")
            if code and code != "NAME":
                out.append({"ID": code, "MULTIPLE": f.get("MULTIPLE", "N")})
    return out

def _value_from_element(el: Dict[str, Any], code: str) -> Optional[Any]:
    """Достаёт значение поля из ответа lists.element.get: либо *_VALUE, либо dict{'VALUE':..}, либо словарь {id: value}."""
    v = el.get(f"{code}_VALUE")
    if v is not None:
        return v
    raw = el.get(code)
    if isinstance(raw, dict):
        # варианты: {'VALUE': '...'} или {'123': '...','124':'...'}
        if "VALUE" in raw:
            return raw["VALUE"]
        if raw:
            # множество значений – вернём список по порядку ключей
            try:
                # сохранить порядок по ключам-числам
                items = sorted(((int(k), val) for k, val in raw.items()), key=lambda x: x[0])
                return [val for _, val in items]
            except Exception:
                return list(raw.values())
    return raw

def get_element_subset(eid: int, req_fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Читает NAME и требуемые поля, возвращает {'NAME':..., 'PROPERTY_XXX': <str|list> , ...}."""
    pairs: List[Tuple[str, str]] = [
        ("IBLOCK_TYPE_ID", "lists"),
        ("IBLOCK_ID", str(LIST_ID)),
        ("filter[=ID]", str(eid)),
        ("select[]", "ID"),
        ("select[]", "NAME"),
    ]
    for f in req_fields:
        code = f["ID"]
        pairs.append(("select[]", code))
        pairs.append(("select[]", f"{code}_VALUE"))
    # также запросим поле даты на всякий случай (не обязательно, но дёшево)
    pairs.extend([("select[]", DATE_PROP), ("select[]", f"{DATE_PROP}_VALUE")])

    js = bx_post_form("lists.element.get", pairs)
    el = (js.get("result") or [None])[0] or {}
    out: Dict[str, Any] = {"NAME": el.get("NAME") or f"ID {eid}"}
    for f in req_fields:
        code = f["ID"]
        val = _value_from_element(el, code)
        if val not in (None, ""):
            out[code] = val
    return out

# ========= Обновление элемента =========
def _pairs_for_field(code: str, value: Any) -> List[Tuple[str, str]]:
    """Генерирует пары form-data для одиночных и множественных значений: fields[CODE]=... или fields[CODE][i]=..."""
    pairs: List[Tuple[str, str]] = []
    if isinstance(value, (list, tuple)):
        for i, v in enumerate(value):
            if v in (None, ""): 
                continue
            pairs.append((f"fields[{code}][{i}]", str(v)))
    elif isinstance(value, dict):
        # крайне редко сюда попадём; на всякий — разложим значения
        i = 0
        for v in value.values():
            if v in (None, ""): 
                continue
            pairs.append((f"fields[{code}][{i}]", str(v)))
            i += 1
    else:
        if value not in (None, ""):
            pairs.append((f"fields[{code}]", str(value)))
    return pairs

def update_element_with_required(eid: int, date_ddmmyyyy: str, req_fields_meta: List[Dict[str, Any]]) -> bool:
    """Подтягивает значения обязательных полей + NAME и делает апдейт с новой датой."""
    snapshot = get_element_subset(eid, req_fields_meta)  # {'NAME':..., 'PROPERTY_204': <...>, ...}

    pairs: List[Tuple[str, str]] = [
        ("IBLOCK_TYPE_ID", "lists"),
        ("IBLOCK_ID", str(LIST_ID)),
        ("ELEMENT_ID", str(eid)),
        ("fields[NAME]", snapshot["NAME"]),
        (f"fields[{DATE_PROP}]", date_ddmmyyyy),
    ]
    # добавим обязательные как есть
    for k, v in snapshot.items():
        if k == "NAME":
            continue
        pairs.extend(_pairs_for_field(k, v))

    js = bx_post_form("lists.element.update", pairs)
    return bool(js.get("result") is True)

# ========= Основной цикл =========
def run_update_for_all() -> Dict[str, Any]:
    if not ELEMENT_IDS:
        log.warning("BITRIX_ELEMENT_IDS пуст — нечего обновлять.")
        return {"updated": 0, "failed": 0, "value": None}

    target = last_day_of_current_month_moscow_ddmmyyyy()
    ok, fail = 0, 0

    # Снимем список обязательных полей один раз
    try:
        req_fields_meta = get_required_field_codes()
        log.info("Обязательные поля: %s", [f["ID"] for f in req_fields_meta])
    except Exception as e:
        log.error("Не удалось получить список обязательных полей: %s", e)
        req_fields_meta = []

    for eid in ELEMENT_IDS:
        try:
            if update_element_with_required(eid, target, req_fields_meta):
                ok += 1
                log.info("OK: ELEMENT_ID=%s  %s=%s", eid, DATE_PROP, target)
            else:
                fail += 1
                log.error("FAILED (false result): ELEMENT_ID=%s", eid)
        except Exception as e:
            fail += 1
            log.error("FAILED: ELEMENT_ID=%s  error=%s", eid, e)

    return {"updated": ok, "failed": fail, "value": target}

# ========= Планировщик =========
def schedule_job():
    if BackgroundScheduler is None or CronTrigger is None:
        log.error("APScheduler не установлен. Установи APScheduler или запусти с --once.")
        return

    sched = BackgroundScheduler(timezone="Europe/Moscow")
    trigger = CronTrigger(day="1", hour="0", minute="1")  # 1-е число, 00:01 мск
    sched.add_job(run_update_for_all, trigger, id="lists-month-end", replace_existing=True)
    sched.start()
    log.info("Ежемесячная задача запущена (00:01 мск, 1 число).")

# ========= CLI =========
if __name__ == "__main__":
    import sys, time
    if "--once" in sys.argv:
        res = run_update_for_all()
        log.info("Done: %s", res)
    else:
        schedule_job()
        while True:
            time.sleep(3600)
