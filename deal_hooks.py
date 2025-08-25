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

LIST_ID = int(os.getenv("LIST_ID", "68"))

# Названия/коды полей (на случай, если numeric-ID в памяти ускакал)
SEARCH_FIELD_ID_FALLBACK = "PROPERTY_204"                # ID услуги (число)
SEARCH_FIELD_CODE_FALLBACK = "ID_uslugi"                 # код из скрина
SEARCH_FIELD_NAME_FALLBACK = "ID услуги"                 # имя поля

VALUE_FIELD_ID_FALLBACK = "PROPERTY_202"                 # дата УПД (если верно)
VALUE_FIELD_CODE_FALLBACK = os.getenv("VALUE_FIELD_CODE", "")     # например: data_upd
VALUE_FIELD_NAME_FALLBACK = os.getenv("VALUE_FIELD_NAME", "Дата выставления УПД")

TARGET_DEAL_FIELD = os.getenv("TARGET_DEAL_FIELD", "UF_CRM_1755600973")

# Таймауты и ретраи
CONNECT_TIMEOUT = float(os.getenv("BITRIX_CONNECT_TIMEOUT", "5"))
READ_TIMEOUT    = float(os.getenv("BITRIX_READ_TIMEOUT", "25"))
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


# ====== HTTP ======
def bx_get(method: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = SESSION.get(f"{BITRIX_URL}{method}", params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def bx_post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = SESSION.post(f"{BITRIX_URL}{method}", json=payload, timeout=TIMEOUT)
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
    try:
        data = bx_get("crm.product.get", {"id": product_id})
        return data.get("result") or {}
    except Exception as e:
        log.warning("crm.product.get failed for %s: %s", product_id, e)
        return {}


# ====== Lists: метаданные полей ======
def get_list_fields() -> List[Dict[str, Any]]:
    data = bx_get("lists.field.get", {"IBLOCK_TYPE_ID": "lists", "IBLOCK_ID": LIST_ID})
    return data.get("result", []) or []

def resolve_field_ids() -> Tuple[List[str], List[str], Dict[str, Any]]:
    """
    Возвращает:
      - список возможных ТЕГОВ свойства для поиска (['PROPERTY_204', 'PROPERTY_ID_uslugi', ...])
      - список возможных ТЕГОВ свойства даты (['PROPERTY_202', 'PROPERTY_data_upd', ...])
      - словарь debug с тем, что обнаружили
    """
    fields = get_list_fields()
    search_tags: List[str] = []
    value_tags: List[str]  = []

    # Нормируем к верхнему регистру для сравнения
    def norm(s: Optional[str]) -> str:
        return (s or "").strip()

    # заранее добавим фолбэки
    if SEARCH_FIELD_ID_FALLBACK:
        search_tags.append(SEARCH_FIELD_ID_FALLBACK)
    if SEARCH_FIELD_CODE_FALLBACK:
        search_tags.append(f"PROPERTY_{SEARCH_FIELD_CODE_FALLBACK}")

    if VALUE_FIELD_ID_FALLBACK:
        value_tags.append(VALUE_FIELD_ID_FALLBACK)
    if VALUE_FIELD_CODE_FALLBACK:
        value_tags.append(f"PROPERTY_{VALUE_FIELD_CODE_FALLBACK}")

    # пробегаем поля из API и добавляем варианты
    for f in fields:
        field_id = norm(f.get("FIELD_ID"))          # например, 'PROPERTY_204'
        code     = norm(f.get("CODE"))              # например, 'ID_uslugi'
        name     = norm(f.get("NAME"))              # например, 'Дата выставления УПД'
        t        = norm(f.get("TYPE"))

        # Для поля поиска (ID услуги)
        if field_id == SEARCH_FIELD_ID_FALLBACK or code == SEARCH_FIELD_CODE_FALLBACK or name == SEARCH_FIELD_NAME_FALLBACK:
            if field_id and field_id not in search_tags:
                search_tags.append(field_id)
            if code and f"PROPERTY_{code}" not in search_tags:
                search_tags.append(f"PROPERTY_{code}")

        # Для поля даты (ищем по id/code/name и по типу даты)
        if field_id == VALUE_FIELD_ID_FALLBACK or code == VALUE_FIELD_CODE_FALLBACK or name == VALUE_FIELD_NAME_FALLBACK or t.lower() in ("s:date", "s:datetime", "date"):
            if field_id and field_id not in value_tags:
                value_tags.append(field_id)
            if code and f"PROPERTY_{code}" not in value_tags:
                value_tags.append(f"PROPERTY_{code}")

    debug = {"search_tags": search_tags, "value_tags": value_tags}
    return search_tags, value_tags, debug


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

def extract_value_prop(el: Dict[str, Any], prop_keys: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Пробуем вытащить дату по любому ключу из prop_keys.
    Возвращает (нормализованная_дата|None, ключ_из_которого_взяли|None)
    """
    for key in prop_keys:
        if key not in el:
            continue
        raw = el.get(key)
        # Значение бывает строкой / словарём / массивом словарей
        def to_scalar(v: Any) -> Optional[str]:
            if isinstance(v, dict):
                v = v.get("VALUE") or v.get("value") or v.get("TEXT")
            return str(v).strip() if v is not None else None

        if isinstance(raw, list):
            if not raw:
                continue
            val = to_scalar(raw[0])
        else:
            val = to_scalar(raw)

        norm = normalize_date_yyyy_mm_dd(val or "")
        if norm:
            return norm, key
    return None, None


# ====== Поиск элемента списка по нескольким тегам свойства ======
def lists_element_get_by_prop(prop_tag: str, value: str) -> List[Dict[str, Any]]:
    base = {"IBLOCK_TYPE_ID": "lists", "IBLOCK_ID": LIST_ID}

    # POST payloads
    attempts_post = [
        {**base, "FILTER": {f"={prop_tag}": value}},
        {**base, "FILTER": {prop_tag: value}},
        {**base, "filter": {f"={prop_tag}": value}},
        {**base, "filter": {prop_tag: value}},
        {**base, "FILTER": {prop_tag.replace("PROPERTY_", "", 1): value}},   # иногда принимают голый CODE
        {**base, "filter": {prop_tag.replace("PROPERTY_", "", 1): value}},
    ]
    for p in attempts_post:
        try:
            data = bx_post("lists.element.get", p)
            res = data.get("result") or []
            if res:
                return res
        except requests.HTTPError:
            continue

    # GET query-style
    attempts_get = [
        {**base, f"filter[={prop_tag}]": value},
        {**base, f"filter[{prop_tag}]": value},
        {**base, f"filter[{prop_tag.replace('PROPERTY_', '', 1)}]": value},
        {**base, f"filter[={prop_tag.replace('PROPERTY_', '', 1)}]": value},
    ]
    for params in attempts_get:
        try:
            data = bx_get("lists.element.get", params)
            res = data.get("result") or []
            if res:
                return res
        except requests.HTTPError:
            continue

    return []


def find_list_element_by_keys(product_id: Any, search_keys: List[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Перебираем возможные ключи свойства (ID и CODE) для поиска.
    Возвращаем (элемент|None, какой_ключ_сработал|None).
    """
    val = str(product_id).strip()
    for tag in search_keys:
        res = lists_element_get_by_prop(tag, val)
        if res:
            return res[0], tag
    return None, None


# ====== HTTP-хук ======
@bp.route("/hooks/deal-update", methods=["POST"])
def on_deal_update():
    # Секрет
    secret = request.args.get("secret")
    if INBOUND_SECRET and secret != INBOUND_SECRET:
        abort(403, description="forbidden")

    # Отладка
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

        # 0) Разрешаем теги полей по метаданным
        search_tags, value_tags, fields_dbg = resolve_field_ids()

        # 1) Товары сделки
        rows = get_deal_products(deal_id)
        if not rows:
            body = {"status": "skip", "reason": "no products"}
            if debug_mode:
                body["fields_dbg"] = fields_dbg
            return jsonify(body), 200

        found_dates: List[str] = []
        debug_matches: List[Dict[str, Any]] = []

        for row in rows:
            product_id = row.get("PRODUCT_ID")
            if not product_id:
                continue

            p = get_product_info(product_id)
            xml_id = p.get("XML_ID")
            code   = p.get("CODE")

            el, matched_tag = find_list_element_by_keys(product_id, search_tags)
            if not el:
                debug_matches.append({
                    "product_id": product_id, "xml_id": xml_id, "code": code,
                    "match": None
                })
                continue

            date_value, date_from_key = extract_value_prop(el, value_tags)
            debug_entry = {
                "product_id": product_id,
                "xml_id": xml_id,
                "code": code,
                "match": matched_tag,
                "date": date_value,
                "date_from_key": date_from_key
            }
            # Если в debug хочется видеть заголовки свойств элемента:
            if debug_mode:
                # покажем только ключи свойств, чтобы не раздувать ответ
                debug_entry["el_keys"] = [k for k in el.keys() if k.startswith("PROPERTY_") or k in ("ID","NAME")]

            debug_matches.append(debug_entry)

            if date_value:
                found_dates.append(date_value)

        if not found_dates:
            body = {
                "status": "skip",
                "reason": "date_property_missing",  # элемент есть, но даты не достали
                "hint": "Проверьте FIELD_ID/CODE поля даты в списке №%d" % LIST_ID
            }
            if debug_mode:
                body["debug"] = debug_matches
                body["fields_dbg"] = fields_dbg
            return jsonify(body), 200

        # берём минимальную дату
        def parse_iso_date(s: str) -> Optional[datetime]:
            try:
                if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
                    return datetime.fromisoformat(s)
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None

        parsed = [parse_iso_date(d) for d in found_dates if d]
        parsed = [p for p in parsed if p]
        if not parsed:
            return jsonify({"status": "skip", "reason": "dates invalid"}), 200

        final_date = min(parsed).date().isoformat()

        current = get_deal_field(deal_id, TARGET_DEAL_FIELD)
        current_norm = normalize_date_yyyy_mm_dd(current) if current else ""
        if (current_norm or "") == final_date:
            resp = {"status": "ok", "updated": False, "note": "no change", "value": final_date}
            if debug_mode:
                matched = [m for m in debug_matches if m.get("date")]
                resp["matched_product_ids"] = [m["product_id"] for m in matched]
                resp["matched_products"] = matched
            return jsonify(resp), 200

        if dry_run:
            resp = {"status": "ok", "updated": False, "note": "dry_run", "value": final_date}
            if debug_mode:
                matched = [m for m in debug_matches if m.get("date")]
                resp["matched_product_ids"] = [m["product_id"] for m in matched]
                resp["matched_products"] = matched
            return jsonify(resp), 200

        ok = update_deal_field(deal_id, TARGET_DEAL_FIELD, final_date)
        resp = {"status": "ok", "updated": ok, "value": final_date}
        if debug_mode:
            matched = [m for m in debug_matches if m.get("date")]
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
