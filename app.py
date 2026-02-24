import os
import time
import json
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List
import requests
from fastapi import FastAPI

app = FastAPI()

# ====== ENV ======
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")
TG_CHAT_ID = os.getenv("TG_CHAT_ID")
WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN")   # токен WB для marketplace-api
WB_FEEDBACKS_TOKEN = os.getenv("WB_FEEDBACKS_TOKEN") or WB_STATS_TOKEN

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))  # раз в 2 минуты

DAILY_HOUR_MSK = int(os.getenv("DAILY_HOUR_MSK", "23"))
DAILY_MINUTE_MSK = int(os.getenv("DAILY_MINUTE_MSK", "59"))

STATE_FILE = "state.json"

# ====== STATE (anti-duplicate) ======
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"sent": {}, "daily_last_sent": None}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

state = load_state()

def was_sent(key: str) -> bool:
    return key in state["sent"]

def mark_sent(key: str):
    state["sent"][key] = int(time.time())
    save_state(state)

# ====== TELEGRAM ======
def tg_send(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return {"error": "No TG_BOT_TOKEN or TG_CHAT_ID"}
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text}
    try:
        r = requests.post(url, json=payload, timeout=30)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

# ====== FORMATTERS ======
def format_order(o: Dict[str, Any], model: str) -> str:
    oid = o.get("id") or o.get("orderId") or o.get("rid") or o.get("srid") or "—"
    nm = o.get("nmId") or o.get("nmID") or "—"
    art = o.get("supplierArticle") or o.get("article") or "—"
    price = o.get("price") or o.get("convertedPrice") or o.get("priceWithDisc") or "—"
    return (
        f"🛒 Новый заказ ({model})\n"
        f"ID: {oid}\n"
        f"nmId: {nm}\n"
        f"Артикул продавца: {art}\n"
        f"Цена: {price}"
    )

def format_feedback(fb: Dict[str, Any]) -> str:
    fid = fb.get("id") or "—"
    nm_id = fb.get("nmId") or "—"
    rating = fb.get("productValuation") or fb.get("valuation") or fb.get("rate") or 0
    text = fb.get("text") or fb.get("feedbackText") or ""
    user = fb.get("userName") or fb.get("buyerName") or "—"
    created = fb.get("createdDate") or fb.get("createdAt") or ""

    try:
        rating_num = int(rating)
    except Exception:
        rating_num = 0

    label = "🟢 Хороший отзыв" if rating_num >= 4 else "🔴 Плохой отзыв"
    if isinstance(text, str) and len(text) > 800:
        text = text[:800] + "…"

    return (
        f"{label}\n"
        f"⭐ Оценка: {rating_num}\n"
        f"nmId: {nm_id}\n"
        f"Покупатель: {user}\n"
        f"Дата: {created}\n\n"
        f"{text}"
    )

# ====== WB API ======
def wb_get_new_orders(url: str):
    headers = {"Authorization": (WB_STATS_TOKEN or "").strip()}

    r = requests.get(url, headers=headers, timeout=25)

    if r.status_code != 200:
        return {
            "__error__": True,
            "status_code": r.status_code,
            "url": url,
            "response_text": r.text[:2000],
        }

    data = r.json()
    if isinstance(data, dict) and "orders" in data:
        return data.get("orders") or []
    if isinstance(data, list):
        return data
    return []
def wb_get_feedbacks():
    headers = {"Authorization": (WB_FEEDBACKS_TOKEN or "").strip()}
    url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"

    params = {
        "isAnswered": "false",   # можно true/false, нам не важно, лишь бы работало
        "take": 100,
        "skip": 0,
        "order": "dateDesc"
    }

    r = requests.get(url, headers=headers, params=params, timeout=25)

    if r.status_code != 200:
        return {
            "__error__": True,
            "status_code": r.status_code,
            "url": str(r.url),
            "response_text": r.text[:2000],
        }

    data = r.json()
    return (data.get("data") or {}).get("feedbacks") or []

# ====== POLLING ======
async def poll_orders_loop():
    FBS_URL = "https://marketplace-api.wildberries.ru/api/v3/orders/new"
    DBS_URL = "https://marketplace-api.wildberries.ru/api/v3/dbs/orders/new"
    DBW_URL = "https://marketplace-api.wildberries.ru/api/v3/dbw/orders/new"

    while True:
        try:
            if WB_STATS_TOKEN:
                for model, url in [("FBS", FBS_URL), ("DBS", DBS_URL), ("DBW", DBW_URL)]:
                    orders = wb_get_new_orders(url)
                    for o in orders:
                        oid = o.get("id") or o.get("orderId") or o.get("rid") or o.get("srid")
                        if not oid:
                            continue
                        key = f"order:{model}:{oid}"
                        if was_sent(key):
                            continue
                        tg_send(format_order(o, model))
                        mark_sent(key)
        except Exception as e:
            tg_send(f"⚠️ Ошибка заказов: {e}")

        await asyncio.sleep(POLL_SECONDS)

async def poll_feedbacks_loop():
    while True:
        try:
            if WB_FEEDBACKS_TOKEN:
                feedbacks = wb_get_feedbacks()
                for fb in feedbacks:
                    fid = fb.get("id")
                    if not fid:
                        continue
                    key = f"feedback:{fid}"
                    if was_sent(key):
                        continue
                    tg_send(format_feedback(fb))
                    mark_sent(key)
        except Exception as e:
            tg_send(f"⚠️ Ошибка отзывов: {e}")

        await asyncio.sleep(POLL_SECONDS)

# ====== DAILY SUMMARY ======
def is_time_for_daily():
    msk = timezone(timedelta(hours=3))
    now = datetime.now(msk)
    last = state.get("daily_last_sent")
    if now.hour == DAILY_HOUR_MSK and now.minute >= DAILY_MINUTE_MSK:
        if not last:
            return True
        last_dt = datetime.fromisoformat(last)
        return last_dt.date() != now.date()
    return False

async def daily_summary_loop():
    while True:
        try:
            if is_time_for_daily():
                # Заглушка сводки (продажи/выкупы/отказы можно расширить)
                text = "📊 Суточная сводка WB\n(продажи/выкупы/отказы — можно расширить следующим шагом)"
                tg_send(text)
                state["daily_last_sent"] = datetime.now(timezone.utc).isoformat()
                save_state(state)
        except Exception as e:
            tg_send(f"⚠️ Ошибка суточной сводки: {e}")
        await asyncio.sleep(60)

# ====== ROUTES ======
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/test-telegram")
def test_tg():
    return tg_send("✅ Тест: сообщение из Render")

@app.get("/poll-once")
def poll_once():
    sent_orders = 0
    sent_feedbacks = 0

    try:
        FBS_URL = "https://marketplace-api.wildberries.ru/api/v3/orders/new"
        DBS_URL = "https://marketplace-api.wildberries.ru/api/v3/dbs/orders/new"
        DBW_URL = "https://marketplace-api.wildberries.ru/api/v3/dbw/orders/new"

        for model, url in [("FBS", FBS_URL), ("DBS", DBS_URL), ("DBW", DBW_URL)]:
            orders = wb_get_new_orders(url)

if isinstance(orders, dict) and orders.get("__error__"):
    return {"orders": "error", "wb": orders}
            for o in orders:
                oid = o.get("id") or o.get("orderId") or o.get("rid") or o.get("srid")
                if not oid:
                    continue
                key = f"order:{model}:{oid}"
                if was_sent(key):
                    continue
                tg_send(format_order(o, model))
                mark_sent(key)
                sent_orders += 1
    except Exception as e:
        return {"orders": "error", "error": str(e)}

    try:
        feedbacks = wb_get_feedbacks()
        for fb in feedbacks:
            fid = fb.get("id")
            if not fid:
                continue
            key = f"feedback:{fid}"
            if was_sent(key):
                continue
            tg_send(format_feedback(fb))
            mark_sent(key)
            sent_feedbacks += 1
    except Exception as e:
        return {"feedbacks": "error", "error": str(e)}

    return {"orders": "ok", "sent_orders": sent_orders, "feedbacks": "ok", "sent_feedbacks": sent_feedbacks}

# ====== STARTUP ======
@app.on_event("startup")
async def startup():
    # Убрали уведомление о старте, чтобы не спамило при перезапусках Render
    asyncio.create_task(poll_orders_loop())
    asyncio.create_task(poll_feedbacks_loop())
    asyncio.create_task(daily_summary_loop())
