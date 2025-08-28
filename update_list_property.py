# update_list_property.py
# Обновляет поле даты (по умолчанию PROPERTY_202) у набора элементов списка Bitrix.
# Особенности:
# - fast_bitrix24: батчи (до 50) и троттлинг -> меньше HTTP-кругов и таймаутов
# - Всегда «пронoсит» обязательные поля (IS_REQUIRED=Y) и NAME
# - Дата в формате dd.mm.YYYY, конец текущего месяца по Москве
# - Режимы: --once (однократно) или фоново (ежемесячно в 00:01 мск через APScheduler)

import os
import re
import json
import logging
import calendar
import socket
from datetime import datetime, date
from typing import Any, Dict, List, Optional

# --- опциональный IPv4 (снижает подвисания на некоторых хостингах/сетях)
try:
    import urllib3.util.connection as urllib3_cn  # type: ignore
    if os.getenv("FORCE_IPV4", "0").lower() in ("1", "true", "yes"):
        urllib3_cn.allowed_gai_family = lambda: socket.AF_INET
except Exception:
    pass

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger("update_list_property")

# ========== ENV / конфиг ==========
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
    su = s.upper()
    if su == "NAME":
        return "NAME"
    if not su.startswith("PROPERTY_"):
        return "PROPERTY_" + s.strip("_").upper()
    return su

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

# fast_bitrix24 параметры (настраиваемые)
FBX_POOL = float(os.getenv("FBX_POOL", "20"))                 # request_pool_size
FBX_RPS = float(os.getenv("FBX_RPS", "1.5"))                  # requests_per_second
FBX_BATCH = int(os.getenv("FBX_BATCH", "50"))                 # batch_size (макс 50 у Bitrix)
FBX_OPTL = int(os.getenv("FBX_OPTL", "300"))                  # operating_time_limit сек

# ========== Время/даты ==========
def _today_moscow() -> date:
    try:
        from zoneinfo import ZoneInfo  # py>=3.9
        return datetime.now(ZoneInfo("Europe/Moscow")).date()
    except Exception:
        return datetime.now().date()

def month_end_ddmmyyyy() -> str:
    t = _today_moscow()
    last = calendar.monthrange(t.year, t.month)[1]
    d = date(t.year, t.month, last)
    return f"{d.day:02d}.{d.month:02d}.{d.year:04d}"

# ========== fast_bitrix24 ==========
from fast_bitrix24 import Bitrix  # type: ignore

def _extract_fields_list(raw: Any) -> List[Dict[str, Any]]:
    """
    Унифицирует ответ lists.field.get:
    - {"result":[...]} -> список
    - {"result":{"CODE":{...},...}} -> значения словаря
    - {"fields":[...]} / {"fields":{...}} -> то же
    - [... ] -> список
    """
    js = raw
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
    return su  # иной системный код

def fetch_required_codes(bx: Bitrix) -> List[str]:
    doc = bx.call("lists.field.get", {"IBLOCK_TYPE_ID":"lists","IBLOCK_ID":IBLOCK_ID}, raw=True)
    fields = _extract_fields_list(doc)
    codes: List[str] = []
    for f in fields:
        try:
            if str(f.get("IS_REQUIRED","")).upper() == "Y":
                code = _norm_field_code_from_fields_list(f.get("ID") or f.get("FIELD_ID"))
                if code and code != "NAME":
                    codes.append(code)
        except Exception:
            continue
    # нормализуем и удалим дубли
    codes = list(dict.fromkeys(codes))
    log.info("Обязательные поля: %s", codes)
    return codes

def pick_value(el: Dict[str, Any], code: str):
    """Достаёт значение поля из ответа lists.element.get: *_VALUE, {'VALUE':...} или {'123':'...'}."""
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

def read_elements_batch(bx: Bitrix, ids: List[int], req_codes: List[str]) -> List[Dict[str, Any]]:
    tasks = []
    selects = ["ID","NAME", DATE_PROP, f"{DATE_PROP}_VALUE"]
    for c in req_codes:
        selects.append(c)
        selects.append(f"{c}_VALUE")
    for eid in ids:
        tasks.append({
            "IBLOCK_TYPE_ID":"lists",
            "IBLOCK_ID": IBLOCK_ID,
            "filter": {"ID": eid},
            "select": selects
        })
    results = bx.call("lists.element.get", tasks)  # список ответов (по одной записи)
    out: List[Dict[str, Any]] = []
    for r in results:
        # r обычно dict {"result":[{...}], "total":1, ...} или Exception/False
        if isinstance(r, dict) and r.get("result"):
            rec = (r["result"] or [None])[0] or {}
            out.append(rec)
        else:
            out.append({})
    return out

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
                continue  # не затираем новую дату
            v = pick_value(rec, c)
            if v not in (None, ""):
                fields[c] = v
        tasks.append({
            "IBLOCK_TYPE_ID":"lists",
            "IBLOCK_ID": IBLOCK_ID,
            "ELEMENT_ID": rec["ID"],
            "fields": fields
        })
    return tasks

def update_batch(bx: Bitrix, upd_tasks: List[Dict[str, Any]]) -> int:
    if not upd_tasks:
        return 0
    results = bx.call("lists.element.update", upd_tasks)
    ok = 0
    for r in results:
        # r может быть True, или {"result":true}, или Exception
        if r is True:
            ok += 1
        elif isinstance(r, dict) and r.get("result") is True:
            ok += 1
    return ok

def run_once() -> Dict[str, Any]:
    if not ELEMENT_IDS:
        log.warning("BITRIX_ELEMENT_IDS пуст — нечего обновлять.")
        return {"updated": 0, "failed": 0, "value": None}

    bx = Bitrix(
        WEBHOOK,
        request_pool_size=int(FBX_POOL),
        requests_per_second=float(FBX_RPS),
        batch_size=int(FBX_BATCH),
        operating_time_limit=int(FBX_OPTL),
    )

    # 1) обязательные поля
    req_codes = fetch_required_codes(bx)

    # 2) читаем элементы (батч)
    recs = read_elements_batch(bx, ELEMENT_IDS, req_codes)

    # 3) готовим апдейты
    new_value = month_end_ddmmyyyy()
    upd_tasks = build_update_tasks(recs, req_codes, new_value)

    # 4) шлём апдейты (батч, порциями по 50 fast_bitrix24 сделает сам)
    ok = update_batch(bx, upd_tasks)
    failed = len(upd_tasks) - ok

    # 5) лог по каждому (для читабельности)
    for rec in recs:
        eid = rec.get("ID")
        if not eid:
            continue
        if str(eid) in [str(t["ELEMENT_ID"]) for t in upd_tasks][:ok]:
            log.info("OK: ELEMENT_ID=%s  %s=%s", eid, DATE_PROP, new_value)
    log.info("Done: {'updated': %s, 'failed': %s, 'value': '%s'}", ok, failed, new_value)
    return {"updated": ok, "failed": failed, "value": new_value}

# ========== Планировщик ==========
def schedule_job():
    try:
        from apscheduler.schedulers.background import BackgroundScheduler  # noqa
        from apscheduler.triggers.cron import CronTrigger  # noqa
    except Exception:
        log.error("APScheduler не установлен. Установи APScheduler или запусти с --once.")
        return
    sched = BackgroundScheduler(timezone="Europe/Moscow")
    # 1-е число каждого месяца в 00:01 МСК
    trigger = CronTrigger(day="1", hour="0", minute="1")
    sched.add_job(run_once, trigger, id="lists-month-end", replace_existing=True)
    sched.start()
    log.info("Ежемесячная задача запущена (00:01 мск, 1 число). Дата будет установлена в конец текущего месяца.")

if __name__ == "__main__":
    import sys, time
    if "--once" in sys.argv:
        run_once()
    else:
        schedule_job()
        while True:
            time.sleep(3600)
