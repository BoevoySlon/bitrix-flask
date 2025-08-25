# deal_hooks.py
import os
import re
import logging
from datetime import datetime
from typing import Any, Dict, Optional, List

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
SEARCH_PROP = "PROPERTY_204"            # ищем по этому свойству в списке
VALUE_PROP = "PROPERTY_202"             # берём дату из этого свойства
TARGET_DEAL_FIELD = "UF_CRM_1755600973" # целевое поле сделки (тип: дата)

# Таймауты и ретраи для REST Bitrix24
CONNECT_TIMEOUT = float(os.getenv("BITRIX_CONNECT_TIMEOUT", "5"))   # сек на TLS handshake
READ_TIMEOUT    = float(os.getenv("BITRIX_READ_TIMEOUT", "25"))     # сек на чтение ответа
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

SESSION = requests.Session()
retry = Retry(
    total=4,                # до 4 повторов
    connect=4,
    read=4,
    backoff_factor=0.6,     # 0.6, 1.2, 2.4, 4.8 сек между попытками
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "POST"]),
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
SESSION.mount("https://", adapter)
SESSION.mount("http://", adapter)

log = logging.getLogger(__name__)


# ====== Вспомогательные HTTP-обёртки ======
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


# ====== Логика работы с Битрикс24 ======
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


# ====== Парсинг/нормализация дат из списка ======
def normalize_date_yyyy_mm_dd(value: str) -> Optional[str]:
    """
    Приводим строку к формату YYYY-MM-DD (дата без времени), как ожидает Битрикс24.
    Поддерживаем варианты:
    - YYYY-MM-DD...
    - DD.MM.YYYY
    - ISO-подобные строки (с временем/часовым поясом)
    """
    if not value:
        return None
    s = str(value).strip()

    # 1) Уже YYYY-MM-DD (или начало ISO с такой датой)
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # 2) DD.MM.YYYY
    m = re.match(r"^(\d{2})\.(\d{2})\.(\d{4})$", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    # 3) Попытка распарсить ISO-дату/датвремя
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        pass

    return None

def extract_value_prop(el: Dict[str, Any], prop_code: str) -> Optional[str]:
    """
    Унифицировано вытаскиваем значение PROPERTY_202 из ответа lists.element.get,
    учитывая разные форматы (строка/словарь/массив словарей), и нормализуем в YYYY-MM-DD.
    """
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
    """Преобразуем YYYY-MM-DD или ISO-дату в datetime (для сравнения/минимума)."""
    if not s:
        return None
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
            return datetime.fromisoformat(s)
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


# ====== Поиск элемента в списке №68 по PRODUCT_ID ======
def lists_get_with_filters(payloads: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Пробуем несколько форм фильтра и возвращаем первый ненулевой результат."""
    for p in payloads:
        try:
            data = bx_post("lists.element.get", p)
            res = data.get("result") or []
            if res:
                return res
        except requests.HTTPError:
            continue
    return []

def find_list_element_by_prop(product_id: Any) -> Optional[Dict[str, Any]]:
    """
    Ищем элемент списка, у которого PROPERTY_204 == product_id.
    B24 иногда капризничает к форме фильтра, поэтому пробуем несколько вариантов.
    """
    pid = str(product_id).strip()
    base = {"IBLOCK_TYPE_ID": "lists", "IBLOCK_ID": LIST_ID}

    attempts_post = [
        {**base, "FILTER": {f"={SEARCH_PROP}": pid}},  # 1) FILTER + '='
        {**base, "FILTER": {SEARCH_PROP: pid}},        # 2) FILTER без '='
        {**base, "filter": {f"={SEARCH_PROP}": pid}},  # 3) filter (lowercase)
        {**base, "filter": {SEARCH_PROP: pid}},        # 4) filter без '='
    ]
    res = lists_get_with_filters(attempts_post)
    if res:
        return res[0]

    # Fallback: GET с query-style фильтром
    # Пробуем с '=' и без '='
    try:
        params = {**base, f"filter[={SEARCH_PROP}]": pid}
        data = bx_get("lists.element.get", params)
        res = data.get("result") or []
        if res:
            return res[0]
    except requests.HTTPError:
        pass

    try:
        params = {**base, f"filter[{SEARCH_PROP}]": pid}
        data = bx_get("lists.element.get", params)
        res = data.get("result") or []
        if res:
            return res[0]
    except requests.HTTPError:
        pass

    return None


# ====== HTTP-хук ======
@bp.route("/hooks/deal-update", methods=["POST"])
def on_deal_update():
    # Простейшая защита секретом (?secret=...)
    secret = request.args.get("secret")
    if INBOUND_SECRET and secret != INBOUND_SECRET:
        abort(403, description="forbidden")

    try:
        payload = request.get_json(silent=True) or {}
        # Bitrix обычно шлёт {"data":{"FIELDS":{"ID":"123"}}}
        deal_id = (
            payload.get("data", {}).get("FIELDS", {}).get("ID")
            or payload.get("FIELDS", {}).get("ID")
            or payload.get("deal_id")
        )
        if not deal_id:
            return jsonify({"status": "skip", "reason": "no deal id"}), 200

        deal_id = int(deal_id)

        # 1) Забираем все товарные строки сделки
        rows = get_deal_products(deal_id)
        if not rows:
            return jsonify({"status": "skip", "reason": "no products"}), 200

        # 2) Для каждого товара ищем элемент списка и вытаскиваем дату
        found_dates: List[str] = []
        for row in rows:
            product_id = row.get("PRODUCT_ID")
            if not product_id:
                continue
            el = find_list_element_by_prop(product_id)
            if not el:
                continue
            date_value = extract_value_prop(el, VALUE_PROP)
            if date_value:
                found_dates.append(date_value)

        if not found_dates:
            return jsonify({"status": "skip", "reason": "no matches in list"}), 200

        # 3) Выбираем минимальную дату (раньшую)
        parsed = [parse_iso_date(d) for d in found_dates if d]
        parsed = [p for p in parsed if p]
        if not parsed:
            return jsonify({"status": "skip", "reason": "dates invalid"}), 200

        final_date = min(parsed).date().isoformat()

        # 4) Обновляем поле сделки только при изменении
        current = get_deal_field(deal_id, TARGET_DEAL_FIELD)
        current_norm = normalize_date_yyyy_mm_dd(current) if current else ""
        if (current_norm or "") == final_date:
            return jsonify({"status": "ok", "updated": False, "note": "no change", "value": final_date}), 200

        ok = update_deal_field(deal_id, TARGET_DEAL_FIELD, final_date)
        return jsonify({"status": "ok", "updated": ok, "value": final_date}), 200

    except RequestsTimeout as e:
        # Таймауты сети/чтения — не роняем вебхук, чтобы не множить ретраи у Б24
        log.warning("Bitrix REST timeout: %s", e)
        return jsonify({"status": "retry_later", "reason": "bitrix timeout"}), 200
    except RequestException as e:
        # Любые сетевые ошибки requests — тоже возвращаем 200 с описанием
        log.exception("Bitrix REST request error")
        return jsonify({"status": "error_remote", "detail": str(e)}), 200
    except Exception as e:
        log.exception("Unhandled")
        return jsonify({"status": "error", "detail": str(e)}), 500
