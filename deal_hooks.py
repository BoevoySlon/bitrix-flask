# deal_hooks.py
import os
import re
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

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

# ===== Конфигурация =====
BITRIX_URL = os.getenv("BITRIX_OUTGOING_URL", "").rstrip("/") + "/"
INBOUND_SECRET = os.getenv("INBOUND_SHARED_SECRET")

LIST_ID = int(os.getenv("LIST_ID", "68"))
SEARCH_PROP = "PROPERTY_204"             # ищем по этому свойству в списке
DATE_PROP   = "PROPERTY_202"             # берём дату из этого свойства
TARGET_DEAL_FIELD = os.getenv("TARGET_DEAL_FIELD", "UF_CRM_1755600973")

# Таймауты/ретраи HTTP (перенастраиваются через ENV)
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

log = logging.getLogger(__name__)

# ===== Низкоуровневые вызовы REST =====
def bx_get(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = SESSION.get(f"{BITRIX_URL}{method}", params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def bx_post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = SESSION.post(f"{BITRIX_URL}{method}", json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def bx_post_form(method: str, data_pairs: List[Tuple[str, str]]) -> Dict[str, Any]:
    """
    application/x-www-form-urlencoded, чтобы честно передавать select[]=... и filter[...]=...
    """
    r = SESSION.post(f"{BITRIX_URL}{method}", data=data_pairs, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

# ===== CRM =====
def get_deal_products(deal_id: int) -> List[Dict[str, Any]]:
    data = bx_get("crm.deal.productrows.get", {"id": deal_id})
    return data.get("result") or []

def get_deal_field(deal_id: int, field: str) -> Optional[Any]:
    data = bx_get("crm.deal.get", {"id": deal_id})
    res = data.get("result") or {}
    return res.get(field)

def update_deal_field(deal_id: int, field: str, value: Any) -> bool:
    data = bx_post("crm.deal.update", {"id": deal_id, "fields": {field: value}})
    return bool(data.get("result") is True)

# ===== Вспомогательные функции для списков =====
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
    """
    Универсальная распаковка значений Bitrix:
    - строка -> строка
    - {"VALUE": "..."} / {"TEXT":"..."} / вложенные dict -> строка
    - {"1616":"..."} (assoc) -> значение
    - [ ... ] -> первый непустой элемент
    """
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
        v = _first_entry_value(x)  # ассоциативный словарь вида {"1616": "31.08.2025"}
        return _flatten_scalar(v)
    s = str(x).strip()
    return s or None

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

def fetch_elements_by_product_id(product_id: Any) -> List[Dict[str, Any]]:
    """
    Form-POST: filter[=PROPERTY_204]=<product_id>, select[]=PROPERTY_202/204
    Возвращаем элементы как есть (уже со свойствами).
    """
    pid = str(product_id).strip()
    data_pairs: List[Tuple[str, str]] = [
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

def extract_date_for_product(elements: List[Dict[str, Any]], product_id: Any, debug: bool = False) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Из списка элементов (уже с PROPERTY_202/204) находим тот, где PROPERTY_204 == product_id,
    вытаскиваем дату из PROPERTY_202 и нормализуем.
    """
    pid = str(product_id).strip()
    dbg: Dict[str, Any] = {"checked": 0, "matches": []}

    for el in elements:
        dbg["checked"] += 1

        # значение PROPERTY_204
        raw_pid = el.get(f"{SEARCH_PROP}_VALUE", None)
        if raw_pid is None:
            raw_pid = el.get(SEARCH_PROP, None)
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

        # значение PROPERTY_202
        raw_date = el.get(f"{DATE_PROP}_VALUE", None)
        if raw_date is None:
            raw_date = el.get(DATE_PROP, None)
            if isinstance(raw_date, dict) and "VALUE" in raw_date:
                raw_date = raw_date["VALUE"]

        date_scalar = _flatten_scalar(raw_date)
        date_norm = normalize_date_yyyy_mm_dd(date_scalar)

        if debug:
            dbg["matches"].append({
                "element_id": el.get("ID"),
                "name": el.get("NAME"),
                "raw_pid": raw_pid,
                "flat_pid": flat_pid,
                "raw_date": raw_date,
                "date_scalar": date_scalar,
                "date_norm": date_norm,
            })

        if date_norm:
            return date_norm, dbg

    return None, dbg

# ===== HTTP-хук =====
@bp.route("/hooks/deal-update", methods=["POST"])
def on_deal_update():
    # защита секретом (?secret=...)
    secret = request.args.get("secret")
    if INBOUND_SECRET and secret != INBOUND_SECRET:
        abort(403, description="forbidden")

    debug_mode = (request.args.get("debug", "").lower() in ("1", "true", "yes", "y"))
    dry_run    = (request.args.get("dry_run", "").lower() in ("1", "true", "yes", "y"))

    try:
        payload = request.get_json(silent=True) or {}
        deal_id = (
            payload.get("data", {}).get("FIELDS", {}).get("ID")
            or payload.get("FIELDS", {}).get("ID")
            or payload.get("deal_id")
        )
        if not deal_id:
            return jsonify({"status": "skip", "reason": "no deal id"}), 200
        deal_id = int(deal_id)

        rows = get_deal_products(deal_id)
        if not rows:
            return jsonify({"status": "skip", "reason": "no products"}), 200

        found_dates: List[str] = []
        dbg_all: List[Dict[str, Any]] = []

        for row in rows:
            product_id = row.get("PRODUCT_ID")
            if not product_id:
                continue

            els = fetch_elements_by_product_id(product_id)
            date_norm, dbg = extract_date_for_product(els, product_id, debug=debug_mode)
            if debug_mode:
                dbg["product_id"] = product_id
                dbg_all.append(dbg)
            if date_norm:
                found_dates.append(date_norm)

        if not found_dates:
            body = {"status": "skip", "reason": "date_property_missing"}
            if debug_mode:
                body["debug"] = dbg_all
            return jsonify(body), 200

        # минимальная дата
        def parse_iso(d: str) -> Optional[datetime]:
            try:
                if re.fullmatch(r"\d{4}-\d{2}-\d{2}", d):
                    return datetime.fromisoformat(d)
                return datetime.fromisoformat(d.replace("Z", "+00:00"))
            except Exception:
                return None

        parsed = [p for p in (parse_iso(d) for d in found_dates) if p]
        if not parsed:
            return jsonify({"status": "skip", "reason": "dates invalid"}), 200
        final_date = min(parsed).date().isoformat()

        current = get_deal_field(deal_id, TARGET_DEAL_FIELD)
        current_norm = normalize_date_yyyy_mm_dd(str(current) if current else "")
        if (current_norm or "") == final_date:
            resp = {"status": "ok", "updated": False, "note": "no change", "value": final_date}
            if debug_mode:
                resp["debug"] = dbg_all
            return jsonify(resp), 200

        if dry_run:
            resp = {"status": "ok", "updated": False, "note": "dry_run", "value": final_date}
            if debug_mode:
                resp["debug"] = dbg_all
            return jsonify(resp), 200

        ok = update_deal_field(deal_id, TARGET_DEAL_FIELD, final_date)
        resp = {"status": "ok", "updated": ok, "value": final_date}
        if debug_mode:
            resp["debug"] = dbg_all
        return jsonify(resp), 200

    except (RequestsTimeout, ReadTimeout, ConnectTimeout) as e:
        # мягкая деградация при таймаутах
        log.warning("Bitrix REST timeout: %s", e)
        return jsonify({"status": "retry_later", "reason": "bitrix timeout"}), 200
    except RequestException as e:
        log.exception("Bitrix REST request error")
        return jsonify({"status": "error_remote", "detail": str(e)}), 200
    except Exception as e:
        log.exception("Unhandled")
        return jsonify({"status": "error", "detail": str(e)}), 500
