# deal_hooks.py
import os
import re
import logging
from datetime import datetime
from typing import Any, Dict, Optional, List, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Blueprint, request, jsonify, abort
from requests.exceptions import Timeout as RequestsTimeout, RequestException

bp = Blueprint("deal_hooks", __name__)

# ====== Конфигурация ======
BITRIX_URL = os.getenv("BITRIX_OUTGOING_URL", "").rstrip("/") + "/"
INBOUND_SECRET = os.getenv("INBOUND_SHARED_SECRET")

# Список и поля
LIST_ID = 68
SEARCH_PROP = "PROPERTY_204"            # ключ поиска в списке
VALUE_PROP = "PROPERTY_202"             # берём дату из этого свойства
TARGET_DEAL_FIELD = "UF_CRM_1755600973" # целевое поле сделки (тип: дата)

# Таймауты и ретраи для REST Bitrix24
CONNECT_TIMEOUT = float(os.getenv("BITRIX_CONNECT_TIMEOUT", "5"))   # сек handshake
READ_TIMEOUT    = float(os.getenv("BITRIX_READ_TIMEOUT", "25"))     # сек чтение
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

SESSION = requests.Session()
retry = Retry(
    total=4, connect=4, read=4,
    backoff_factor=0.6,
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

log = logging.getLogger(__name__)


# ====== HTTP-обёртки ======
def bx_get(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BITRIX_URL}{method}"
    r = SESSION.get(url, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def bx_post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    url = f"{BITRIX_URL}{method}"
    r = SESSION.post(url, json=payload, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


# ====== CRM ======
def get_deal_products(deal_id: int) -> List[Dict[str, Any]]:
    data = bx_get("crm.deal.productrows.get", {"id": deal_id})
    return data.get("result", []) or []

def get_deal_field(deal_id: int, field: str) -> Optional[Any]:
    data = bx_get("crm.deal.get", {"id": deal_id})
    res = data.get("result") or {}
    return res.get(field)

def update_deal_field(deal_id: int, field: str, value: Any) -> bool:
    resp = bx_post("crm.deal.update", {"id": deal_id, "fields": {field: value}})
    return bool(resp.get("result") is True)

def get_product_info(product_id: Any) -> Dict[str, Any]:
    """crm.product.get: берём расширенную инфу по товару (ID, XML_ID, CODE, NAME и т.д.)"""
    try:
        data = bx_get("crm.product.get", {"id": product_id})
        return data.get("result") or {}
    except Exception as e:
        log.warning("crm.product.get failed for %s: %s", product_id, e)
        return {}


# ====== Даты ======
def normalize_date_yyyy_mm_dd(value: str) -> Optional[str]:
    if not value:
        return None
    s = str(value).strip()

    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return None

def extract_value_prop(el: Dict[str, Any], prop_code: str) -> Optional[str]:
    raw = el.get(prop_code)
    if raw is None:
        return None

    def to_scalar(v: Any) -> Optional[str]:
        if isinstance(v, dict):
            v = v.get("VALUE") or v.get("value")
        return str(v).strip() if v is not None else None

    if isinstance(raw, list):
        if not raw:
            return None
        val = to_scalar(raw[0])
    else:
        val = to_scalar(raw)

    return normalize_date_yyyy_mm_dd(val or "")

def parse_iso_date(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return datetime.fromisoformat(s)
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# ====== Lists: поиск элемента по PROPERTY_204 ======
def lists_get_with_filters(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for p in payloads:
        try:
            data = bx_post("lists.element.get", p)
            res = data.get("result") or []
            if res:
                return res
        except requests.HTTPError:
            continue
    return []

def lists_get_query_style(params_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    for params in params_list:
        try:
            data = bx_get("lists.element.get", params)
            res = data.get("result") or []
            if res:
                return res
        except requests.HTTPError:
            continue
    return []

def find_list_element_by_any_key(product_id: Any, xml_id: Optional[str], code: Optional[str]) -> Tuple[Optional[Dict[str, Any]], str]:
    """
    Пытаемся найти элемент списка №68 по очереди:
    1) PROPERTY_204 == PRODUCT_ID
    2) PROPERTY_204 == XML_ID (если есть)
    3) PROPERTY_204 == CODE (если есть)
    Возвращаем (элемент|None, чем_совпало).
    """
    pid = str(product_id).strip()
    base = {"IBLOCK_TYPE_ID": "lists", "IBLOCK_ID": LIST_ID}

    def try_value(val: str, label: str) -> Optional[Dict[str, Any]]:
        # POST-варианты
        attempts_post = [
            {**base, "FILTER": {f"={SEARCH_PROP}": val}},
            {**base, "FILTER": {SEARCH_PROP: val}},
            {**base, "filter": {f"={SEARCH_PROP}": val}},
            {**base, "filter": {SEARCH_PROP: val}},
        ]
        res = lists_get_with_filters(attempts_post)
        if res:
            return res[0]

        # GET query-style
        attempts_get = [
            {**base, f"filter[={SEARCH_PROP}]": val},
            {**base, f"filter[{SEARCH_PROP}]": val},
        ]
        res = lists_get_query_style(attempts_get)
        if res:
            return res[0]
        return None

    # 1) PRODUCT_ID
    el = try_value(pid, "PRODUCT_ID")
    if el:
        return el, "PRODUCT_ID"

    # 2) XML_ID
    if xml_id:
        el = try_value(str(xml_id).strip(), "XML_ID")
        if el:
            return el, "XML_ID"

    # 3) CODE
    if code:
        el = try_value(str(code).strip(), "CODE")
        if el:
            return el, "CODE"

    return None, ""


# ====== HTTP-хук ======
@bp.route("/hooks/deal-update", methods=["POST"])
def on_deal_update():
    # Простейшая защита секретом (?secret=...)
    secret = request.args.get("secret")
    if INBOUND_SECRET and secret != INBOUND_SECRET:
        abort(403, description="forbidden")

    # Режим отладки: ?debug=1|true|yes|y
    debug_mode = (request.args.get("debug", "").lower() in ("1", "true", "yes", "y"))

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

        # 1) Товарные строки сделки
        rows = get_deal_products(deal_id)
        if not rows:
            return jsonify({"status": "skip", "reason": "no products"}), 200

        # 2) Ищем даты по всем товарам с fallback'ами ключей
        found_dates: List[str] = []
        debug_matches: List[Dict[str, Any]] = []

        for row in rows:
            product_id = row.get("PRODUCT_ID")
            if not product_id:
                continue

            p = get_product_info(product_id)
            xml_id = p.get("XML_ID")
            code   = p.get("CODE")

            el, matched_by = find_list_element_by_any_key(product_id, xml_id, code)
            if not el:
                debug_matches.append({
                    "product_id": product_id,
                    "xml_id": xml_id,
                    "code": code,
                    "match": None
                })
                continue

            date_value = extract_value_prop(el, VALUE_PROP)
            debug_matches.append({
                "product_id": product_id,
                "xml_id": xml_id,
                "code": code,
                "match": matched_by,
                "date": date_value
            })
            if date_value:
                found_dates.append(date_value)

        if not found_dates:
            body = {"status": "skip", "reason": "no matches in list"}
            if debug_mode:
                body["candidate_product_ids"] = [m.get("product_id") for m in debug_matches]
                body["debug"] = debug_matches
            return jsonify(body), 200

        # 3) Минимальная дата
        parsed = [parse_iso_date(d) for d in found_dates if d]
        parsed = [p for p in parsed if p]
        if not parsed:
            return jsonify({"status": "skip", "reason": "dates invalid"}), 200

        final_date = min(parsed).date().isoformat()

        # 4) Обновляем сделку только если меняется
        current = get_deal_field(deal_id, TARGET_DEAL_FIELD)
        current_norm = normalize_date_yyyy_mm_dd(current) if current else ""
        if (current_norm or "") == final_date:
            resp = {"status": "ok", "updated": False, "note": "no change", "value": final_date}
            if debug_mode:
                matched = [m for m in debug_matches if m.get("match")]
                resp["matched_product_ids"] = [m["product_id"] for m in matched]
                resp["matched_products"] = matched
            return jsonify(resp), 200

        ok = update_deal_field(deal_id, TARGET_DEAL_FIELD, final_date)
        resp = {"status": "ok", "updated": ok, "value": final_date}
        if debug_mode:
            matched = [m for m in debug_matches if m.get("match")]
            resp["matched_product_ids"] = [m["product_id"] for m in matched]
            resp["matched_products"] = matched
        return jsonify(resp), 200

    except RequestsTimeout as e:
        log.warning("Bitrix REST timeout: %s", e)
        return jsonify({"status": "retry_later", "reason": "bitrix timeout"}), 200
    except RequestException as e:
        log.exception("Bitrix REST request error")
        return jsonify({"status": "error_remote", "detail": str(e)}), 200
    except Exception as e:
        log.exception("Unhandled")
        return jsonify({"status": "error", "detail": str(e)}), 500
