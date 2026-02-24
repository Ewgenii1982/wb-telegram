import os
import json
import time
from typing import Dict, Any, List
import requests
from fastapi import FastAPI

app = FastAPI()

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
WB_TOKEN = os.getenv("WB_TOKEN")

SENT_FILE = "sent_ids.json"

def load_sent_ids():
    if os.path.exists(SENT_FILE):
        with open(SENT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"orders": [], "feedbacks": []}

def save_sent_ids(data):
    with open(SENT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def send_telegram(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return {"error": "No TG_BOT_TOKEN or TG_CHAT_ID"}

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    r = requests.post(url, json=payload, timeout=30)
    return r.json()

def format_order_message(order: Dict[str, Any]) -> str:
    return (
        f"🆕 Новый заказ\n"
        f"Артикул: {order.get('nmId')}\n"
        f"Заказ №: {order.get('id')}\n"
        f"Дата: {order.get('dateCreated')}"
    )

def format_feedback_message(fb: Dict[str, Any]) -> str:
    fb_id = fb.get("id") or "?"
    nm_id = fb.get("nmId") or ""
    rating = fb.get("productValuation") or fb.get("valuation") or fb.get("rate") or 0
    text = fb.get("text") or fb.get("feedbackText") or ""
    created = fb.get("createdDate") or fb.get("createdAt") or ""
    user = fb.get("userName") or fb.get("buyerName") or ""

    try:
        rating_num = int(rating)
    except Exception:
        rating_num = 0

    if rating_num >= 4:
        label = "🟢 Хороший отзыв"
    else:
        label = "🔴 Плохой отзыв"

    if isinstance(text, str) and len(text) > 800:
        text = text[:800] + "…"

    return (
        f"{label}\n"
        f"⭐ Оценка: {rating_num}\n"
        f"Артикул: {nm_id}\n"
        f"Покупатель: {user}\n"
        f"Дата: {created}\n\n"
        f"{text}"
    ).strip()

def fetch_orders() -> List[Dict[str, Any]]:
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers = {"Authorization": WB_TOKEN}
    params = {"dateFrom": "2024-01-01"}
    r = requests.get(url, headers=headers, params=params, timeout=30)
    return r.json() if r.status_code == 200 else []

def fetch_feedbacks() -> List[Dict[str, Any]]:
    url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
    headers = {"Authorization": WB_TOKEN}
    r = requests.get(url, headers=headers, timeout=30)
    return r.json().get("data", {}).get("feedbacks", [])

def poll_once():
    sent = load_sent_ids()

    orders = fetch_orders()
    for o in orders:
        oid = str(o.get("id"))
        if oid not in sent["orders"]:
            send_telegram(format_order_message(o))
            sent["orders"].append(oid)

    feedbacks = fetch_feedbacks()
    for fb in feedbacks:
        fid = str(fb.get("id"))
        if fid not in sent["feedbacks"]:
            send_telegram(format_feedback_message(fb))
            sent["feedbacks"].append(fid)

    save_sent_ids(sent)
    return {"orders_checked": len(orders), "feedbacks_checked": len(feedbacks)}

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/test-telegram")
def test_telegram():
    return {"telegram_result": send_telegram("✅ Тест: сообщение из облачного сервера Render")}

@app.get("/poll-once")
def poll_once_route():
    result = poll_once()
    return {"ok": True, "result": result}
