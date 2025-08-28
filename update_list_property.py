# scripts/update_list_property.py
import os, sys, logging, calendar, json
from datetime import datetime, date
from zoneinfo import ZoneInfo
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
except ImportError:
    BackgroundScheduler = None

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("monthly-list-updater")

BITRIX_BASE = os.environ["BITRIX_OUTGOING_URL"].rstrip("/") + "/"
LIST_ID = int(os.getenv("BITRIX_LIST_ID", "68"))
# Можно передать: "123,456,789" или JSON: "[123,456,789]"
TARGET_IDS_RAW = os.getenv("BITRIX_ELEMENT_IDS", "")
PROP_RAW = os.getenv("BITRIX_PROPERTY_CODE", "PROPERTY_202")
CONNECT_TIMEOUT = float(os.getenv("BITRIX_CONNECT_TIMEOUT", "8"))
READ_TIMEOUT = float(os.getenv("BITRIX_READ_TIMEOUT", "30"))
RETRY_TOTAL = int(os.getenv("BITRIX_RETRY_TOTAL", "6"))
RETRY_CONNECT = int(os.getenv("BITRIX_RETRY_CONNECT", "6"))
RETRY_READ = int(os.getenv("BITRIX_RETRY_READ", "6"))
BACKOFF = float(os.getenv("BITRIX_BACKOFF", "0.8"))

def _parse_ids(raw: str):
    raw = raw.strip()
    if not raw:
        return []
    try:
        if raw.startswith("["):
            return [int(x) for x in json.loads(raw)]
    except Exception:
        pass
    return [int(x.strip()) for x in raw.split(",") if x.strip()]

def _normalize_prop(code: str) -> str:
    code = code.strip()
    if code.isdigit():
        return f"PROPERTY_{code}"
    if not code.upper().startswith("PROPERTY_"):
        return "PROPERTY_" + code.strip("_").upper()
    return code.upper()

TARGET_IDS = _parse_ids(TARGET_IDS_RAW)
PROP_CODE = _normalize_prop(PROP_RAW)

def _requests_session():
    sess = requests.Session()
    retry = Retry(
        total=RETRY_TOTAL,
        connect=RETRY_CONNECT,
        read=RETRY_READ,
        backoff_factor=BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["POST", "GET"])
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

def _last_day_of_current_month_moscow() -> date:
    tz = ZoneInfo("Europe/Moscow")
    today = datetime.now(tz).date()
    last_day = calendar.monthrange(today.year, today.month)[1]
    return date(today.year, today.month, last_day)

def _bitrix_call(method: str, payload: dict):
    url = f"{BITRIX_BASE}{method}.json"
    sess = _requests_session()
    # По Bitrix REST формату обновления полей массив передаётся как fields/FIELDS
    # оставим обе записи на случай различий в методах
    resp = sess.post(url, json=payload, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Bitrix error {data.get('error')}: {data.get('error_description')}")
    return data.get("result", data)

def update_elements_to_month_end():
    if not TARGET_IDS:
        log.warning("Переменная BITRIX_ELEMENT_IDS не задана — нечего обновлять.")
        return {"updated": 0, "skipped": 0}

    target_date = _last_day_of_current_month_moscow().isoformat()  # YYYY-MM-DD
    updated, skipped = 0, 0
    for eid in TARGET_IDS:
        try:
            payload = {
                "IBLOCK_TYPE_ID": "lists",
                "IBLOCK_ID": LIST_ID,
                "ELEMENT_ID": eid,
                "FIELDS": {PROP_CODE: target_date},
                "fields": {PROP_CODE: target_date},
            }
            _bitrix_call("lists.element.update", payload)
            updated += 1
            log.info("ELEMENT_ID=%s -> %s=%s [OK]", eid, PROP_CODE, target_date)
        except Exception as e:
            skipped += 1
            log.error("ELEMENT_ID=%s FAILED: %s", eid, e)
    return {"updated": updated, "skipped": skipped, "value": target_date}

def run_once_cli():
    res = update_elements_to_month_end()
    log.info("Done: %s", res)

def schedule_monthly_job():
    if BackgroundScheduler is None:
        log.error("APScheduler не установлен. Добавь APScheduler в requirements.txt")
        return
    sched = BackgroundScheduler(timezone=ZoneInfo("Europe/Moscow"))
    # 1-е число каждого месяца в 00:01 по Москве
    trigger = CronTrigger(day="1", hour="0", minute="1")
    sched.add_job(update_elements_to_month_end, trigger, id="bitrix-list-monthly", replace_existing=True)
    sched.start()
    log.info("Ежемесячная задача запущена (00:01 мск, 1 число).")

if __name__ == "__main__":
    # Позволяет ручной запуск внутри контейнера:
    #   python -m scripts.update_list_property --once
    if "--once" in sys.argv:
        run_once_cli()
    else:
        schedule_monthly_job()
        # держим процесс (на случай отдельного запуска без gunicorn)
        import time
        while True:
            time.sleep(3600)
