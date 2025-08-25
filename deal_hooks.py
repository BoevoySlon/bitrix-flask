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

# Поля списка (фолбэки)
SEARCH_FIELD_ID_FALLBACK = "PROPERTY_204"         # ID услуги (число)
SEARCH_FIELD_CODE_FALLBACK = "ID_uslugi"          # код по скрину
SEARCH_FIELD_NAME_FALLBACK = "ID услуги"          # имя поля

VALUE_FIELD_ID_FALLBACK = "PROPERTY_202"          # дата (если верно)
VALUE_FIELD_CODE_FALLBACK = os.getenv("VALUE_FIELD_CODE", "")
VALUE_FIELD_NAME_FALLBACK = os.getenv("VALUE_FIELD_NAME", "Дата выставления УПД")

TARGET_DEAL_FIELD = os.getenv("TARGET_DEAL_FIELD", "UF_CRM_1755600973")

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

def extract_value_prop(el: Dict[str, Any], prop_keys: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Пробуем вытащить дату по любому ключу из prop_keys.
    Возвращает (дата_YYYY-MM-DD|None, ключ|None).
    """
    for key in prop_keys:
        if key not in el:
            continue
        raw = el.get(key)

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


# ====== Поиск элемента списка по свойству ======
def lists_element_get_by_prop(prop_tag: str, value: str) -> List[Dict[str, Any]]:
    base = {"IBLOCK_TYPE_ID": "lists", "IBLOCK_ID": LIST_ID}

    # POST payloads
    attempts_post = [
        {**base, "FILTER": {f"={prop_tag}": value}},
        {**base, "FILTER": {prop_tag: value}},
        {**base, "filter": {f"={prop_tag}": value}},
        {**base, "filter": {prop_tag: value}},
        {**base, "FILTER": {prop_tag.replace("PROPERTY_", "", 1): value}},
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
    attempts_get_
