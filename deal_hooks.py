# deal_hooks.py
import os
import re
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Blueprint, request, jsonify, abort
from requests.exceptions import (
    Timeout as RequestsTimeout,
    ReadTimeout,
    ConnectTimeout,
    RequestException,
)

bp = Blueprint("deal_hooks", __name__)

# ===== ЛОГИРОВАНИЕ (минимум) =====
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
log = logging.getLogger(__name__)

# ===== Конфигурация =====
BITRIX_URL = os.getenv("BITRIX_OUTGOING_URL", "").rstrip("/") + "/"
INBOUND_SECRET = os.getenv("INBOUND_SHARED_SECRET")

LIST_ID = int(os.getenv("LIST_ID", "68"))
SEARCH_PROP = "PROPERTY_204"             # ищем по этому свойству в списке
DATE_PROP   = "PROPERTY_202"             # берём дату из этого свойства
TARGET_DEAL_FIELD = os.getenv("TARGET_DEAL_FIELD", "UF_CRM_1755600973")

# чекбокс-замок (если включён — поле даты не перезаписываем)
LOCK_FIELD = os.getenv("MANUAL_LOCK_FIELD", "").strip()

# уважать ручные правки N часов
MANUAL_OVERRIDE_TTL_HOURS = int(os.getenv("MANUAL_OVERRIDE_TTL_HOURS", "24"))

# ID интеграционного пользователя (из URL или env)
def _parse_integration_user_id_from_url(url: str) -> Optional[int]:
    m = re.search(r"/rest/(\d+)/", url)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

INTEGRATION_USER_ID = int(
    os.getenv(
        "BITRIX_USER_ID",
        str(_parse_integration_user_id_from_url(BITRIX_URL)) if _parse_integration_user_id_from_url(BITRIX_URL) else "0",
    ) or "0"
)

# Таймауты/ретраи HTTP (через ENV)
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

# ===== Утилиты =====
def is_truthy(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    s = str(v).strip().lower()
    return s in ("y", "yes", "true", "1", "on")

def normalize_date_yyyy_mm_dd(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    m = re.fullmatch(r"(\d{2})\.(\d{2})\.(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return None

def _first_entry_value(obj: Any) -> Optional[Any]:
    if obj is None:
        return None
    if isinstance(obj, dict):
        for _, v in obj.items():
            return v
    if isinstance(obj, (list, tuple)) and obj:
        return obj[0]
    return obj

def _flatten_scalar(x: Any) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, (list, tuple)):
        for item in x:
            s = _flatten_scalar(item)
            if s:
                return s
        return None
    if isinstance(x, dict):
        if "TEXT" in x and x["TEXT"]:
            return str(x["TEXT"]).strip()
        if "text" in x and x["text"]:
            return str(x["text"]).strip()
        if "VALUE" in x and x["VALUE"] not in (None, ""):
            return _flatten_scalar(x["VALUE"])
        if "value" in x and x["value"] not in (None, ""):
            return _flatten_scalar(x["value"])
        v = _first_entry_value(x)  # ассоц: {"1616": "31.08.2025"}
        return _flatten_scalar(v)
    s = str(x).strip()
    return s or None

# ===== REST =====
def bx_get(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = SESSION.get(f"{BITRIX_URL}{method}", params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def bx_post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = SESSION.post(f"{BITRIX_URL}{method}", json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def bx_post_form(method: str, data_pairs: List[tuple[str, str]]) -> Dict[str, Any]:
    r = SESSION.post(f"{BITRIX_URL}{method}", data=data_pairs, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

# ===== CRM =====
def get_deal_products(deal_id: int) -> List[Dict[str, Any]]:
    data = bx_get("crm.deal.productrows.get", {"id": deal_id})
    return data.get("result") or []

def get_deal_info(deal_id: int) -> Dict[str, Any]:
    data = bx_get("crm.deal.get", {"id": deal_id})
    return data.get("result") or {}

def update_deal_field(deal_id: int, field: str, value: Any) -> bool:
    data = bx_post("crm.deal.update", {"id": deal_id, "fields": {field: value}})
    return bool(data.get("result") is True)

# ===== Lists =====
def fetch_elements_by_product_id(product_id: Any) -> List[Dict[str, Any]]:
    pid = str(product_id).strip()
    data_pairs: List[tuple[str, str]] = [
        ("IBLOCK_TYPE_ID", "lists"),
        ("IBLOCK_ID", str(LIST_ID)),
        (f"filter[={SEARCH_PROP}]", pid),
        ("select[]", "ID"),
        ("select[]", "NAME"),
        ("select[]", DATE_PROP),
        ("select[]", f"{DATE_PROP}_VALUE"),
        ("select[]", SEARCH_PROP),
        ("select[]", f"{SEARCH_PROP}_VALUE"),
    ]
    data = bx_post_form("lists.element.get", data_pairs)
    return data.get("result") or []

def extract_date_for_product(elements: List[Dict[str, Any]], product_id: Any) -> Optional[str]:
    pid = str(product_id).strip()
    for el in elements:
        raw_pid = el.get(f"{SEARCH_PROP}_VALUE")
        if raw_pid is None:
            raw_pid = el.get(SEARCH_PROP)
            if isinstance(raw_pid, dict) and "VALUE" in raw_pid:
                raw_pid = raw_pid["VALUE"]
        flat_pid = _flatten_scalar(raw_pid)
        if flat_pid != pid:
            if isinstance(el.get(SEARCH_PROP), dict):
                alt = _first_entry_value(el.get(SEARCH_PROP))
                if _flatten_scalar(alt) != pid:
                    continue
            else:
                continue

        raw_date = el.get(f"{DATE_PROP}_VALUE")
        if raw_date is None:
            raw_date = el.get(DATE_PROP)
            if isinstance(raw_date, dict) and "VALUE" in raw_date:
                raw_date = raw_date["VALUE"]

        date_scalar = _flatten_scalar(raw_date)
        date_norm = normalize_date_yyyy_mm_dd(date_scalar)
        if date_norm:
            return date_norm
    return None

# ===== Парсинг входа (JSON и form-urlencoded) =====
def _extract_deal_id_from_request() -> Optional[int]:
    payload = request.get_json(silent=True) or {}
    if isinstance(payload, dict):
        for getter in (
            lambda p: p.get("data", {}).get("FIELDS", {}).get("ID"),
            lambda p: p.get("FIELDS", {}).get("ID"),
            lambda p: p.get("deal_id"),
            lambda p: p.get("ID"),
        ):
            try:
                v = getter(payload)
            except Exception:
                v = None
            if v:
                try:
                    return int(v)
                except Exception:
                    return None
    form = request.form or {}
    if form:
        for key in ("data[FIELDS][ID]", "FIELDS[ID]", "data[ID]", "ID", "deal_id"):
            v = form.get(key)
            if v:
                try:
                    return int(v)
                except Exception:
                    return None
    return None

# ===== Основной хук =====
@bp.route("/hooks/deal-update", methods=["POST"])
def on_deal_update():
    secret = request.args.get("secret")
    if INBOUND_SECRET and secret != INBOUND_SECRET:
        abort(403, description="forbidden")

    try:
        deal_id = _extract_deal_id_from_request()
        if not deal_id:
            return jsonify({"status": "skip", "reason": "no deal id"}), 200

        # читаем сделку целиком (и для LOCK, и для инфо о редакторе)
        deal = get_deal_info(deal_id)

        # 1) LOCK: если выставлен чекбокс — не трогаем поле
        if LOCK_FIELD:
            lock_val = deal.get(LOCK_FIELD)
            if is_truthy(lock_val):
                return jsonify({"status": "skip", "reason": "locked"}), 200

        # 2) получаем товары сделки
        rows = get_deal_products(deal_id)
        if not rows:
            return jsonify({"status": "skip", "reason": "no products"}), 200

        # 3) вычисляем минимальную дату на основе списка
        def parse_iso(d: str) -> Optional[datetime]:
            try:
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
                    return datetime.fromisoformat(d)
                return datetime.fromisoformat(d.replace("Z", "+00:00"))
            except Exception:
                return None

        dates: List[str] = []
        for row in rows:
            pid = row.get("PRODUCT_ID")
            if not pid:
                continue
            els = fetch_elements_by_product_id(pid)
            d = extract_date_for_product(els, pid)
            if d:
                dates.append(d)
        if not dates:
            return jsonify({"status": "skip", "reason": "date_property_missing"}), 200
        parsed = [p for p in (parse_iso(d) for d in dates) if p]
        if not parsed:
            return jsonify({"status": "skip", "reason": "dates invalid"}), 200
        final_date = min(parsed).date().isoformat()

        # 4) решаем — ставить или уважить недавнюю ручную правку
        current = deal.get(TARGET_DEAL_FIELD)
        current_norm = normalize_date_yyyy_mm_dd(str(current) if current else "")

        if not current_norm:
            ok = update_deal_field(deal_id, TARGET_DEAL_FIELD, final_date)
            return jsonify({"status": "ok", "updated": ok, "value": final_date}), 200

        if current_norm == final_date:
            return jsonify({"status": "ok", "updated": False, "value": final_date}), 200

        modify_by = deal.get("MODIFY_BY_ID") or deal.get("UPDATED_BY_ID")
        date_modify_raw = deal.get("DATE_MODIFY") or deal.get("DATE_UPDATE") or ""
        dm = None
        for s in (date_modify_raw, date_modify_raw.replace(" ", "T")):
            try:
                if s:
                    dm = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    break
            except Exception:
                dm = None

        # если последним был интеграционный пользователь — перезаписываем
        try:
            if modify_by and int(modify_by) == INTEGRATION_USER_ID:
                ok = update_deal_field(deal_id, TARGET_DEAL_FIELD, final_date)
                return jsonify({"status": "ok", "updated": ok, "value": final_date}), 200
        except Exception:
            pass

        # иначе уважаем ручную правку N часов
        if dm:
            age = datetime.utcnow() - dm.replace(tzinfo=None)
            if age <= timedelta(hours=MANUAL_OVERRIDE_TTL_HOURS):
                return jsonify({"status": "skip", "reason": "manual_recent_override", "value": current_norm}), 200

        # TTL прошёл — перезаписываем
        ok = update_deal_field(deal_id, TARGET_DEAL_FIELD, final_date)
        return jsonify({"status": "ok", "updated": ok, "value": final_date}), 200

    except (RequestsTimeout, ReadTimeout, ConnectTimeout) as e:
        log.warning("Bitrix REST timeout: %s", e)
        return jsonify({"status": "retry_later", "reason": "bitrix timeout"}), 200
    except RequestException as e:
        log.exception("Bitrix REST request error")
        return jsonify({"status": "error_remote", "detail": str(e)}), 200
    except Exception as e:
        log.exception("Unhandled")
        return jsonify({"status": "error", "detail": str(e)}), 500
