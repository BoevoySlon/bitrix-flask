def handle(payload):
    # Пример: просто эхо с добавлением поля
    return {
        "received": payload,
        "message": "hello from scripts/hello.py",
    }
