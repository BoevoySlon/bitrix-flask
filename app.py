import os
import hmac
import hashlib
import importlib.util
from flask import Flask, request, jsonify, abort
from werkzeug.middleware.proxy_fix import ProxyFix
import requests

from deal_hooks import bp as deal_hooks_bp
app.register_blueprint(deal_hooks_bp)

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False

# ==== конфигурация ====
SCRIPTS_DIR = os.getenv("SCRIPTS_DIR", "/app/scripts")
BITRIX_OUTGOING_URL = os.getenv("BITRIX_OUTGOING_URL", "")

def _get_secret(name: str, default: str = "") -> str:
    """Секрет из ENV или *_FILE (совместимо с Docker secrets)."""
    if os.getenv(name):
        return os.getenv(name, default)
    file_var = os.getenv(f"{name}_FILE")
    if file_var and os.path.exists(file_var):
        with open(file_var, "r", encoding="utf-8") as f:
            return f.read().strip()
    return default

INBOUND_SHARED_SECRET = _get_secret("INBOUND_SHARED_SECRET", "")

# ==== утилиты ====
def verify_signature(req) -> bool:
    """Проверяем HMAC подпись (если секрет задан).
    Хедер: X-Signature: <hex of HMAC_SHA256(body, secret)>
    """
    if not INBOUND_SHARED_SECRET:
        return True
    supplied = req.headers.get("X-Signature")
    if not supplied:
        return False
    computed = hmac.new(INBOUND_SHARED_SECRET.encode("utf-8"), req.get_data(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(supplied, computed)

def load_script(module_name: str):
    """Динамически грузим скрипт scripts/<name>.py, ожидаем функцию handle(payload)."""
    path = os.path.join(SCRIPTS_DIR, f"{module_name}.py")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Script not found: {path}")
    spec = importlib.util.spec_from_file_location(module_name, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore
    if not hasattr(mod, "handle"):
        raise AttributeError(f"{module_name}.py must define handle(payload)")
    return mod.handle

# ==== маршруты ====
@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.post("/webhook/inbound")
def inbound():
    if not verify_signature(request):
        abort(401, description="Invalid signature")

    payload = request.get_json(silent=True) or request.form.to_dict() or {}

    script_name = request.args.get("script")
    if script_name:
        try:
            handler = load_script(script_name)
            result = handler(payload)
            return jsonify({"ok": True, "script": script_name, "result": result})
        except Exception as e:  # noqa: BLE001
            app.logger.exception("Script error")
            abort(500, description=str(e))

    return jsonify({"ok": True})

@app.post("/api/send_to_bitrix")
def send_to_bitrix():
    if not BITRIX_OUTGOING_URL:
        abort(500, description="BITRIX_OUTGOING_URL is not set")
    data = request.get_json(force=True, silent=False)
    try:
        r = requests.post(BITRIX_OUTGOING_URL, json=data, timeout=20)
        try:
            body = r.json()
        except ValueError:
            body = r.text
        return jsonify({"status": r.status_code, "body": body})
    except requests.RequestException as e:  # noqa: BLE001
        abort(502, description=f"Bitrix request failed: {e}")

# корректная работа за обратным прокси (nginx-proxy/traefik)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)  # type: ignore
