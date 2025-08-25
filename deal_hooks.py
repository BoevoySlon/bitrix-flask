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

# Поля списка (фолбэки, если авторазбор не подскажет лучше)
SEARCH_FIELD_ID_FALLBACK = "PROPERTY_204"          # ключ поиска (ID услуги)
SEARCH_FIELD_CODE_FALLBACK = "ID_uslugi"           # код по интерфейсу
SEARCH_FIELD_NAME_FALLBACK = "ID услуги"           # имя поля

VALUE_FIELD_ID_FALLBACK = "PROPERTY_202"           # дата в списке
VALUE_FIELD_CODE_FALLBACK = os.getenv("VALUE_FIELD_CODE", "")
VALUE_FIELD_NAME_FALLBACK = os.getenv("VALUE_FIELD_NAME", "Дата выставления УПД")

TARGET_DEAL_FIELD = os.getenv("TARGET_DEAL_FIELD", "UF_CRM_1755600973")  # поле сделки (дата)

# Таймауты/ретраи
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
def _result_to_field_list(result_obj: Any) -> List[Dict[str, Any]]:
    """
    lists.field.get может вернуть:
      - список словарей
      - словарь вида { "PROPERTY_XXX": {...}, "PROPERTY_YYY": {...} }
    Приводим к списку словарей.
    """
    if isinstance(result_obj, list):
        return [x for x in result_obj if isinstance(x, dict)]
    if isinstance(result_obj, dict):
        return [v for v in result_obj.values() if isinstance(v, dict)]
    return []

def get_list_fields() -> List[Dict[str, Any]]:
    data = bx_get("lists.field.get", {"IBLOCK_TYPE_ID": "lists", "IBLOCK_ID": LIST_ID})
    return _result_to_field_list(data.get("result"))

def resolve_field_ids() -> Tuple[List[str], List[str], Dict[str, Any]]:
    """
    Возвращает:
      - варианты тегов свойства для поиска (search_tags)
      - варианты тегов свойства даты (value_tags)
      - debug-словарь с тем, что нашли
    """
    fields = get_list_fields()
    search_tags: List[str] = []
    value_tags: List[str]  = []

    def norm(s: Optional[str]) -> str:
        return (s or "").strip()

    # Базовые фолбэки
    if SEARCH_FIELD_ID_FALLBACK:
        search_tags.append(SEARCH_FIELD_ID_FALLBACK)
    if SEARCH_FIELD_CODE_FALLBACK:
        search_tags.append(f"PROPERTY_{SEARCH_FIELD_CODE_FALLBACK}")

    if VALUE_FIELD_ID_FALLBACK:
        value_tags.append(VALUE_FIELD_ID_FALLBACK)
    if VALUE_FIELD_CODE_FALLBACK:
        value_tags.append(f"PROPERTY_{VALUE_FIELD_CODE_FALLBACK}")

    # Пробегаем реальные поля
    for f in fields:
        field_id = norm(f.get("FIELD_ID"))  # 'PROPERTY_204'
        code     = norm(f.get("CODE"))      # 'ID_uslugi'
        name     = norm(f.get("NAME"))      # 'Дата выставления УПД'
        ftype    = norm(f.get("TYPE")).lower()

        # Поле поиска (ID услуги)
        if field_id == SEARCH_FIELD_ID_FALLBACK or code == SEARCH_FIELD_CODE_FALLBACK or name == SEARCH_FIELD_NAME_FALLBACK:
            if field_id and field_id not in search_tags:
                search_tags.append(field_id)
            if code and f"PROPERTY_{code}" not in search_tags:
                search_tags.append(f"PROPERTY_{code}")

        # Поле даты (по id/code/name/типу)
        if (
            field_id == VALUE_FIELD_ID_FALLBACK
            or (VALUE_FIELD_CODE_FALLBACK and code == VALUE_FIELD_CODE_FALLBACK)
            or name == VALUE_FIELD_NAME_FALLBACK
            or ftype in ("s:date", "s:datetime", "date", "datetime")
        ):
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

def _to_scalar_date(value: Any) -> Optional[str]:
    """
    Аккуратно извлекаем скаляр даты из разных оболочек Bitrix:
    - строка
    - dict с VALUE/TEXT (в т.ч. VALUE -> dict с TEXT/ VALUE)
    - dict-ассoц. вида {"1616":"31.08.2025"} (ключ = PROPERTY_VALUE_ID)
    - список/кортеж из вышеперечисленного
    """
    def drill(v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, (list, tuple)):
            for item in v:
                s = drill(item)
                if s:
                    return s
            return None
        if isinstance(v, dict):
            # 1) TEXT приоритетнее
            if "TEXT" in v and v["TEXT"]:
                return str(v["TEXT"]).strip()
            if "text" in v and v["text"]:
                return str(v["text"]).strip()
            # 2) VALUE/value могут быть строкой или вложенным словарём
            if "VALUE" in v and v["VALUE"] not in (None, ""):
                return drill(v["VALUE"])
            if "value" in v and v["value"] not in (None, ""):
                return drill(v["value"])
            # 3) Ассоциативный словарь {value_id: "значение"}
            for vv in v.values():
                s = drill(vv)
                if s:
                    return s
            return None
        # скаляр
        s = str(v).strip()
        return s or None

    return drill(value)

def extract_value_prop(el: Dict[str, Any], prop_keys: List[str]) -> Tuple[Optional[str], Optional[str], Dict[str, Any]]:
    """
    Пробуем вытащить дату по любому ключу из prop_keys (и по ключу с суффиксом _VALUE).
    Возвращаем (дата_YYYY-MM-DD|None, ключ|None, raw_map по ключам).
    """
    raw_seen: Dict[str, Any] = {}
    for base_key in prop_keys:
        for key in (base_key, f"{base_key}_VALUE"):
            if key not in el:
                continue
            raw = el.get(key)
            raw_seen[key] = raw
            scalar = _to_scalar_date(raw)
            if not scalar:
                continue
            norm = normalize_date_yyyy_mm_dd(scalar)
            if norm:
                return norm, key, raw_seen
    return None, None, raw_seen


# ====== lists.element.get с гарантированным select ======
def lists_element_get_by_prop(prop_tag: str, value: str) -> List[Dict[str, Any]]:
    base = {"IBLOCK_TYPE_ID": "lists", "IBLOCK_ID": LIST_ID}
    select_all = ["ID", "NAME", "*", "PROPERTY_*"]

    # POST payloads (предпочтительно)
    attempts_post = [
        {**base, "FILTER": {f"={prop_tag}": value}, "select": select_all},
        {**base, "FILTER": {prop_tag: value},       "select": select_all},
        {**base, "filter": {f"={prop_tag}": value}, "select": select_all},
        {**base, "filter": {prop_tag: value},       "select": select_all},
        # на всякий случай фильтрация по голому CODE
        {**base, "FILTER": {prop_tag.replace("PROPERTY_", "", 1): value}, "select": select_all},
        {**base, "filter": {prop_tag.replace("PROPERTY_", "", 1): value}, "select": select_all},
    ]
    for p in attempts_post:
        try:
            data = bx_post("lists.element.get", p)
            res = data.get("result") or []
            if res:
                return res
        except requests.HTTPError:
            continue

    # GET fallback с ручной сборкой select[] (иногда ведёт себя нестабильно — используем в крайнем случае)
    try:
        from urllib.parse import urlencode
        attempts_get = [
            {**base, f"filter[={prop_tag}]": value},
            {**base, f"filter[{prop_tag}]": value},
            {**base, f"filter[{prop_tag.replace('PROPERTY_', '', 1)}]": value},
            {**base, f"filter[={prop_tag.replace('PROPERTY_', '', 1)}]": value},
        ]
        for params in attempts_get:
            pairs = list(params.items()) + [("select[]", s) for s in select_all]
            url = f"{BITRIX_URL}lists.element.get?{urlencode(pairs)}"
            r = SESSION.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            data = r.json()
            res = data.get("result") or []
            if res:
                return res
    except requests.HTTPError:
        pass

    return []

def find_list_element_by_keys(product_id: Any, search_keys: List[str]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
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

    # Отладка/«сухой прогон»
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

        # 0) Получаем теги полей
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

            # берём дату по любому из известных ключей (и *_VALUE)
            date_value, date_from_key, raw_seen = extract_value_prop(el, value_tags)

            debug_entry = {
                "product_id": product_id,
                "xml_id": xml_id,
                "code": code,
                "match": matched_tag,
                "date": date_value,
                "date_from_key": date_from_key,
            }
            if debug_mode:
                # сырые содержимое ключей даты, которые пробовали
                debug_entry["raw_date_props"] = {k: raw_seen.get(k) for k in raw_seen}
                # список ключей элемента для наглядности
                debug_entry["el_keys"] = [k for k in el.keys() if k.startswith("PROPERTY_") or k in ("ID", "NAME")]

            debug_matches.append(debug_entry)

            if date_value:
                found_dates.append(date_value)

        if not found_dates:
            body = {
                "status": "skip",
                "reason": "date_property_missing",
                "hint": f"Проверьте поле даты в списке #{LIST_ID} (FIELD_ID/CODE/имя)."
            }
            if debug_mode:
                body["debug"] = debug_matches
                body["fields_dbg"] = fields_dbg
            return jsonify(body), 200

        # 2) Берём минимальную дату
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
                resp["fields_dbg"] = fields_dbg
            return jsonify(resp), 200

        if dry_run:
            resp = {"status": "ok", "updated": False, "note": "dry_run", "value": final_date}
            if debug_mode:
                matched = [m for m in debug_matches if m.get("date")]
                resp["matched_product_ids"] = [m["product_id"] for m in matched]
                resp["matched_products"] = matched
                resp["fields_dbg"] = fields_dbg
            return jsonify(resp), 200

        ok = update_deal_field(deal_id, TARGET_DEAL_FIELD, final_date)
        resp = {"status": "ok", "updated": ok, "value": final_date}
        if debug_mode:
            matched = [m for m in debug_matches if m.get("date")]
            resp["matched_product_ids"] = [m["product_id"] for m in matched]
            resp["matched_products"] = matched
            resp["fields_dbg"] = fields_dbg
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
