# deal_hooks.py
import os
import re
import logging
from datetime import datetime
from typing import Any, Dict, Optional, List

import requests
from flask import Blueprint, request, jsonify, abort

bp = Blueprint("deal_hooks", __name__)

BITRIX_URL = os.getenv("BITRIX_OUTGOING_URL", "").rstrip("/") + "/"
INBOUND_SECRET = os.getenv("INBOUND_SHARED_SECRET")
LIST_ID = 68
SEARCH_PROP = "PROPERTY_204"         # ищем по этому свойству в списке
VALUE_PROP = "PROPERTY_202"          # берем значение даты из этого свойства
TARGET_DEAL_FIELD = "UF_CRM_1755600973"  # целевое поле сделки (тип: дата)

def bx_get(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(f"{BITRIX_URL}{method}", params=params, timeout=10)
    r.raise_for_status()
    return r.json()

def bx_post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(f"{BITRIX_URL}{method}", json=payload, timeout=10)
    r.raise_for_status()
    return r.json()

def get_deal_products(deal_id: int) -> List[Dict[str, Any]]:
    data = bx_get("crm.deal.productrows.get", {"id": deal_id})
    return data.get("result", []) or []

def get_deal_field(deal_id: int, field: str) -> Optional[Any]:
    data = bx_get("crm.deal.get", {"id": deal_id})
    res = data.get("result") or {}
    return res.get(field)

def normalize_date_yyyy_mm_dd(value: str) -> Optional[str]:
    """Приводим к формату YYYY-MM-DD, как требует REST."""
    if not value:
        return None
    s = str(value).strip()

    # 1) ISO 8601/или содержит дату в начале (YYYY-MM-DD[...])
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # 2) DD.MM.YYYY
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # 3) Попытка распарсить как ISO с часовым поясом
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        pass

    return None  # пусть лучше упадёт в "skip", чем писать мусор

def extract_value_prop(el: Dict[str, Any], prop_code: str) -> Optional[str]:
    raw = el.get(prop_code)
    if raw is None:
        return None

    # Значение свойства может приходить строкой/словарем/массивом
    # Берем первый VALUE, затем приводим к YYYY-MM-DD.
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

def find_list_element_by_prop(product_id: Any) -> Optional[Dict[str, Any]]:
    payload = {
        "IBLOCK_TYPE_ID": "lists",
        "IBLOCK_ID": LIST_ID,
        "FILTER": {f"={SEARCH_PROP}": str(product_id)},  # точное сравнение
    }
    data = bx_post("lists.element.get", payload)
    elements = data.get("result", []) or []
    return elements[0] if elements else None

def update_deal_field(deal_id: int, field: str, value: Any) -> bool:
    resp = bx_post("crm.deal.update", {"id": deal_id, "fields": {field: value}})
    return bool(resp.get("result") is True)

@bp.route("/hooks/deal-update", methods=["POST"])
def on_deal_update():
    # простая защита: секрет в query (?secret=...)
    secret = request.args.get("secret")
    if INBOUND_SECRET and secret != INBOUND_SECRET:
        abort(403, description="forbidden")

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

        # Берем последний добавленный товар (по максимальному ID строки, если есть)
        try:
            rows_sorted = sorted(rows, key=lambda r: int(r.get("ID", 0)), reverse=True)
        except Exception:
            rows_sorted = rows
        product_id = rows_sorted[0].get("PRODUCT_ID")
        if not product_id:
            return jsonify({"status": "skip", "reason": "no product_id"}), 200

        # 2) Ищем элемент списка №68: PROPERTY_204 == PRODUCT_ID
        el = find_list_element_by_prop(product_id)
        if not el:
            return jsonify({"status": "skip", "reason": "list element not found"}), 200

        # 3) Дата из PROPERTY_202 → YYYY-MM-DD
        date_value = extract_value_prop(el, VALUE_PROP)
        if not date_value:
            return jsonify({"status": "skip", "reason": f"{VALUE_PROP} empty or invalid"}), 200

        # 4) Обновляем сделку только если значение меняется
        current = get_deal_field(deal_id, TARGET_DEAL_FIELD)
        if (current or "") == date_value:
            return jsonify({"status": "ok", "updated": False, "note": "no change"}), 200

        ok = update_deal_field(deal_id, TARGET_DEAL_FIELD, date_value)
        return jsonify({"status": "ok", "updated": ok, "value": date_value}), 200

    except requests.HTTPError as e:
        logging.exception("Bitrix HTTP error")
        return jsonify({"status": "error", "detail": str(e), "body": getattr(e.response, "text", "")}), 500
    except Exception as e:
        logging.exception("Unhandled")
        return jsonify({"status": "error", "detail": str(e)}), 500
