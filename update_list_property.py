# update_list_property.py
# Гибридный воркер для Bitrix:
# - lists.field.get -> через requests (таймауты/ретраи)
# - lists.element.get / lists.element.update -> через fast_bitrix24 (batch)
# Обновляет поле даты DATE_PROP (по умолчанию PROPERTY_202) у ELEMENT_IDS.
# Уважает обязательные поля (IS_REQUIRED=Y): подхватывает их текущие значения и «пронозит» через апдейт.
# --once: однократный запуск; без параметров — ежемесячно 1-го числа в 00:01 (MSK) через APScheduler.

import os
import re
import json
import logging
import calendar
import socket
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple

# ===== логирование =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("update_list_property")
# приглушим болтливость библиотеки
logging.getLogger("fast_bitrix24").setLevel(logging.WARNING)

# ===== опционально форсируем IPv4 (иногда убирает подвисания TLS/DNS) =====
try:
    import urllib3.util.connection as urllib3_cn  # type: ignore
    if os.getenv("FORCE_IPV4", "0").lower() in ("1", "true", "yes"):
        urllib3_cn.allowed_gai_family = lambda: socket.AF_INET
except Exception:
    pass

# ===== ENV / конфиг =====
WEBHOOK = os.getenv("BITRIX_OUTGOING_URL", "").rstrip("/")
if not WEBHOOK or not re.match(r"^https?://", WEBHOOK):
    raise RuntimeError("BITRIX_OUTGOING_URL пуст или без схемы (жду https://.../rest/<id>/<token>/)")
WEBHOOK += "/"

def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    try:
        return int(v) if v not in (None, "") else default
    except Exception:
        return default

IBLOCK_ID = _env_int("BITRIX_LIST_ID", 68)

def _normalize_prop_code(s: Optional[str], fallback: str = "PROPERTY_202") -> str:
    if not s:
        return fallback
    s = s.strip()
    if s.isdigit():
        return f"PROPERTY_{s}"
    up = s.upper()
    if up == "NAME":
        return "NAME"
    if not up.startswith("PROPERTY_"):
        return "PROPERTY_" + s.strip("_").upper()
    return up

DATE_PROP = _normalize_prop_code(os.getenv("BITRIX_PROPERTY_CODE") or os.getenv("DATE_PROP"), "PROPERTY_202")

def _parse_ids(raw: Optional[str]) -> List[int]:
    if not raw:
        return []
    raw = raw.strip()
    if raw.startswith("["):
        try:
            return [int(x) for x in json.loads(raw)]
        except Exception:
            pass
    return [int(x) for x in re.split(r"[,\s]+", raw) if x]

ELEMENT_IDS = _parse_ids(os.getenv("BITRIX_ELEMENT_IDS") or os.getenv("ELEMENT_IDS") or "")

# ===== requests с таймаутами/ретраями для одиночных вызовов =====
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CONNECT_TIMEOUT = float(os.getenv("BITRIX_CONNECT_TIMEOUT", "15"))
READ_TIMEOUT    = float(os.getenv("BITRIX_READ_TIMEOUT", "60"))
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

RETRY_TOTAL   = int(os.getenv("BITRIX_RETRY_TOTAL", "6"))
RETRY_CONNECT = int(os.getenv("BITRIX_RETRY_CONNECT", str(RETRY_TOTAL)))
RETRY_READ    = int(os.getenv("BITRIX_RETRY_READ", str(RETRY_TOTAL)))
BACKOFF       = float(os.getenv("BITRIX_BACKOFF", "0.8"))

REQ_SESSION = requests.Session()
REQ_SESSION.headers.update({"Connection": "close"})
req_retry = Retry(
    total=RETRY_TOTAL,
    connect=RETRY_CONNECT,
    read=RETRY_READ,
    backoff_factor=BACKOFF,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset({"GET", "POST"}),
    raise_on_status=False,
    respect_retry_after_header=True,
)
REQ_SESSION.mount("https://", HTTPAdapter(max_retries=req_retry, pool_connections=50, pool_maxsize=50))
REQ_SESSION.mount("http://", HTTPAdapter(max_retries=req_retry, pool_connections=50, pool_maxsize=50))

def bx_post_form(method: str, data_pairs: List[Tuple[str, str]]) -> Any:
    url = f"{WEBHOOK}{method}.json"
    r = REQ_SESSION.post(url, data=data_pairs, timeout=TIMEOUT)
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

# ===== утилиты парсинга =====
def _extract_fields_list(js: Any) -> List[Dict[str, Any]]:
    """Унифицируем lists.field.get: список, словарь, fields/result — всё в список полей."""
    if isinstance(js, dict):
        res = js.get("result")
        if isinstance(res, list):
            return res
        if isinstance(res, dict):
            return list(res.values())
        fields = js.get("fields")
        if isinstance(fields, list):
            return fields
        if isinstance(fields, dict):
            return list(fields.values())
    if isinstance(js, list):
        return js
    raise RuntimeError(f"Неожиданный ответ lists.field.get: {type(js).__name__} {str(js)[:140]}")

def _norm_field_code_from_fields_list(code_raw: Any) -> str:
    s = str(code_raw).strip()
    if s.upper() == "NAME":
        return "NAME"
    if s.isdigit():
        return f"PROPERTY_{s}"
    su = s.upper()
    if su.startswith("PROPERTY_"):
        return su
    return su

def _value_from_element(el: Dict[str, Any], code: str):
    """Возвращает значение поля из ответа lists.element.get: *_VALUE, {'VALUE': ...} или {'123': '...'}."""
    v = el.get(f"{code}_VALUE")
    if v is not None:
        return v
    raw = el.get(code)
    if isinstance(raw, dict):
        if "VALUE" in raw:
            return raw["VALUE"]
        vals = list(raw.values())
        return vals[0] if len(vals) == 1 else vals
    return raw

# ===== дата конца месяца по Москве =====
def month_end_ddmmyyyy() -> str:
    try:
        from zoneinfo import ZoneInfo
        t = datetime.now(ZoneInfo("Europe/Moscow")).date()
    except Exception:
        t = datetime.now().date()
    last = calendar.monthrange(t.year, t.month)[1]
    d = date(t.year, t.month, last)
    return f"{d.day:02d}.{d.month:02d}.{d.year:04d}"

# ===== fast_bitrix24 для батчей =====
from fast_bitrix24 import Bitrix  # type: ignore

FBX_POOL  = float(os.getenv("FBX_POOL", "20"))   # request_pool_size
FBX_RPS   = float(os.getenv("FBX_RPS", "1.5"))   # requests_per_second
FBX_BATCH = int(os.getenv("FBX_BATCH", "50"))    # batch_size (макс. 50)
FBX_OPTL  = int(os.getenv("FBX_OPTL", "300"))    # operating_time_limit, сек

# --- обязательные поля (через requests, чтобы не висло) ---
def fetch_required_codes_raw() -> List[str]:
    js = bx_post_form("lists.field.get", [("IBLOCK_TYPE_ID", "lists"), ("IBLOCK_ID", str(IBLOCK_ID))])
    fields = _extract_fields_list(js)
    codes: List[str] = []
    for f in fields:
        try:
            if str(f.get("IS_REQUIRED", "")).upper() == "Y":
                code = _norm_field_code_from_fields_list(f.get("ID") or f.get("FIELD_ID"))
                if code and code != "NAME":
                    codes.append(code)
        except Exception:
            continue
    # нормализуем и убираем дубли
    codes = list(dict.fromkeys(codes))
    log.info("Обязательные поля: %s", codes)
    return codes

# --- чтение элементов батчами; поддержка ОБОИХ форматов ответа fast_bitrix24 ---
def read_elements_batch(bx: Bitrix, ids: List[int], req_codes: List[str]) -> List[Dict[str, Any]]:
    selects = ["ID", "NAME", DATE_PROP, f"{DATE_PROP}_VALUE"]
    for c in req_codes:
        selects.append(c)
        selects.append(f"{c}_VALUE")
    # dedup select на всякий
    selects = list(dict.fromkeys(selects))

    tasks = []
    for eid in ids:
        tasks.append({
            "IBLOCK_TYPE_ID": "lists",
            "IBLOCK_ID": IBLOCK_ID,
            "filter": {"ID": eid},
            "select": selects
        })

    res = bx.call("lists.element.get", tasks)
    recs: List[Dict[str, Any]] = []

    # Вариант А: список ответов (по одному на задачу)
    if isinstance(res, list):
        for r in res:
            if isinstance(r, dict) and r.get("result"):
                rec = (r["result"] or [None])[0] or {}
                recs.append(rec)
            else:
                recs.append({})
        return recs

    # Вариант Б: единый batch-ответ
    if isinstance(res, dict):
        mapping = None
        inner = res.get("result")
        if isinstance(inner, dict) and "result" in inner and isinstance(inner["result"], dict):
            mapping = inner["result"]
        elif isinstance(inner, dict):
            mapping = inner
        if isinstance(mapping, dict):
            for key in sorted(mapping.keys()):  # порядок не критичен, но читаемее
                lst = mapping[key]
                rec = (lst or [None])[0] or {}
                recs.append(rec)
            return recs

    # fallback
    return recs

# --- подготовка апдейтов ---
def build_update_tasks(recs: List[Dict[str, Any]], req_codes: List[str], new_date_ddmmyyyy: str) -> List[Dict[str, Any]]:
    tasks = []
    for rec in recs:
        if not rec or not rec.get("ID"):
            continue
        fields: Dict[str, Any] = {
            "NAME": rec.get("NAME") or f"ID {rec.get('ID')}",
            DATE_PROP: new_date_ddmmyyyy
        }
        for c in req_codes:
            if c.upper() == DATE_PROP.upper():
                continue  # не перезатираем новую дату старым значением
            v = _value_from_element(rec, c)
            if v not in (None, ""):
                fields[c] = v
        tasks.append({
            "IBLOCK_TYPE_ID": "lists",
            "IBLOCK_ID": IBLOCK_ID,
            "ELEMENT_ID": rec["ID"],
            "fields": fields
        })
    return tasks

# --- апдейт батчами; поддержка ОБОИХ форматов ответа ---
def update_batch(bx: Bitrix, upd_tasks: List[Dict[str, Any]]) -> int:
    if not upd_tasks:
        return 0
    res = bx.call("lists.element.update", upd_tasks)
    ok = 0

    # Вариант А: список результатов
    if isinstance(res, list):
        for r in res:
            if r is True or (isinstance(r, dict) and r.get("result") is True):
                ok += 1
        return ok

    # Вариант Б: единый batch-ответ
    if isinstance(res, dict):
        inner = res.get("result")
        mapping = None
        if isinstance(inner, dict) and "result" in inner and isinstance(inner["result"], dict):
            mapping = inner["result"]
        elif isinstance(inner, dict):
            mapping = inner
        if isinstance(mapping, dict):
            for v in mapping.values():
                if v is True or (isinstance(v, dict) and v.get("result") is True):
                    ok += 1
        return ok

    return ok

# ===== основной прогон =====
def run_once() -> Dict[str, Any]:
    if not ELEMENT_IDS:
        log.warning("BITRIX_ELEMENT_IDS пуст — нечего обновлять.")
        return {"updated": 0, "failed": 0, "value": None}

    # 1) обязательные поля (через requests)
    req_codes = fetch_required_codes_raw()

    # 2) чтение и апдейты — через fast_bitrix24 (batch)
    bx = Bitrix(
        WEBHOOK,
        request_pool_size=int(FBX_POOL),
        requests_per_second=float(FBX_RPS),
        batch_size=int(FBX_BATCH),
        operating_time_limit=int(FBX_OPTL),
    )

    recs = read_elements_batch(bx, ELEMENT_IDS, req_codes)
    new_value = month_end_ddmmyyyy()
    upd_tasks = build_update_tasks(recs, req_codes, new_value)
    ok = update_batch(bx, upd_tasks)
    failed = len(upd_tasks) - ok

    log.info("Отправлено задач: %s; Успешно: %s; Неуспешно: %s", len(upd_tasks), ok, failed)
    log.info("Done: {'updated': %s, 'failed': %s, 'value': '%s'}", ok, failed, new_value)
    return {"updated": ok, "failed": failed, "value": new_value}

# ===== планировщик =====
def schedule_job():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler  # noqa
        from apscheduler.triggers.cron import CronTrigger  # noqa
    except Exception:
        log.error("APScheduler не установлен. Установи APScheduler или запусти с --once.")
        return
    sched = BackgroundScheduler(timezone="Europe/Moscow")
    trigger = CronTrigger(day="1", hour="0", minute="1")  # 1-е число в 00:01 MSK
    sched.add_job(run_once, trigger, id="lists-month-end", replace_existing=True)
    sched.start()
    log.info("Ежемесячная задача запущена (00:01 мск, 1 число).")

if __name__ == "__main__":
    import sys, time
    if "--once" in sys.argv:
        run_once()
    else:
        schedule_job()
        while True:
            time.sleep(3600)
