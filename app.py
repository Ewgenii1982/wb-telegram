import os
import time
import threading
import requests
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

# === ENV (Render -> Environment) ===
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "")
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "")

# Ты добавил токен как WB_TOKEN
WB_TOKEN = os.getenv("WB_TOKEN", "")

# Секрет для вебхуков (защита). Если не задашь — проверка выключена.
WB_WEBHOOK_SECRET = os.getenv("WB_WEBHOOK_SECRET", "")

# Как часто опрашивать заказы (сек)
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))

# Память от дублей (в идеале потом заменим на БД, но пока ок)
seen = set()

def tg_send(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": TG_CHAT_ID, "text": text}, timeout=10)
    except Exception:
        pass

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/test-telegram")
def test_telegram():
    tg_send("✅ Тест: сообщение из облачного сервера Render")
    return {"ok": True}

@app.post("/wb/webhook")
async def wb_webhook(req: Request):
    """
    WB -> наш сервер (отзывы и др. события).
    Секрет WB обычно приходит в заголовке Authorization.
    """
    auth = req.headers.get("authorization") or req.headers.get("Authorization")
    if WB_WEBHOOK_SECRET:
        if auth != WB_WEBHOOK_SECRET:
            raise HTTPException(status_code=401, detail="bad webhook secret")

    payload = await req.json()

    # WB может прислать список событий или объект с events/data
    if isinstance(payload, list):
        events = payload
    else:
        events = payload.get("events") or payload.get("data") or []

    for ev in events:
        event_type = ev.get("eventType") or ev.get("type")

        # Основное: отзыв
        if event_type == "feedback_updated":
            fb = ev.get("feedback") or ev.get("data") or {}
            rating = fb.get("rating", "—")
            text = (fb.get("text") or fb.get("comment") or "").strip()
            nm_id = fb.get("nmId") or fb.get("nm_id") or "—"

            msg = (
                "📝 WB отзыв\n"
                f"Товар (nmId): {nm_id}\n"
                f"Оценка: {rating}\n"
                f"Текст: {text[:900]}"
            )
            tg_send(msg)

    return {"ok": True}

def poll_orders_fbs():
    """
    FBS: новые сборочные задания
    https://marketplace-api.wildberries.ru/api/v3/orders/new
    """
    if not WB_TOKEN:
        tg_send("⚠️ WB_TOKEN не задан — опрос заказов не запущен")
        return

    url = "https://marketplace-api.wildberries.ru/api/v3/orders/new"
    headers = {"Authorization": WB_TOKEN}

    while True:
        try:
            r = requests.get(url, headers=headers, timeout=20)

            # Если токен/права не подходят — просто пропускаем (чтобы не спамить)
            if r.status_code == 200:
                data = r.json()
                orders = data.get("orders", []) if isinstance(data, dict) else []

                for o in orders:
                    order_id = o.get("id") or o.get("orderId") or o.get("rid")
                    if not order_id:
                        continue

                    key = f"FBS:{order_id}"
                    if key in seen:
                        continue
                    seen.add(key)

                    article = o.get("article") or o.get("supplierArticle") or "—"
                    price = o.get("price") or o.get("convertedPrice") or "—"

                    tg_send(
                        "✅ Новый заказ WB (FBS)\n"
                        f"ID: {order_id}\n"
                        f"Артикул: {article}\n"
                        f"Цена: {price}"
                    )
        except Exception as e:
            tg_send(f"⚠️ Ошибка опроса FBS: {e}")

        time.sleep(POLL_SECONDS)

@app.on_event("startup")
def startup():
    tg_send("✅ WB→Telegram запущен. Жду отзывы (webhook) и заказы (FBS polling).")
    threading.Thread(target=poll_orders_fbs, daemon=True).start()
