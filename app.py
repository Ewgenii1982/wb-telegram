import os
import time
import json
import sqlite3
import asyncio
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI

app = FastAPI()

# -----------------------------
# ENV (Render -> Environment)
# -----------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

# Один токен на "заказы" (marketplace-api)
WB_TOKEN = os.getenv("WB_TOKEN", "").strip()

# Отдельный токен на "отзывы/вопросы" (feedbacks-api).
# Если не задашь — будет использовать WB_TOKEN.
WB_FEEDBACKS_TOKEN = os.getenv("WB_FEEDBACKS_TOKEN", "").strip() or WB_TOKEN

# Частота опроса (секунды)
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "60"))          # заказы
FEEDBACKS_POLL_SECONDS = int(os.getenv("FEEDBACKS_POLL_SECONDS", "60"))  # отзывы

# Включатели (если надо временно выключить)
ENABLE_ORDERS = os.getenv("ENABLE_ORDERS", "1") == "1"
ENABLE_FEEDBACKS = os.getenv("ENABLE_FEEDBACKS", "1") == "1"

# Какие модели заказов проверять:
# FBS (обычные сборочные задания), DBW, DBS
ENABLE_FBS = os.getenv("ENABLE_FBS", "1") == "1"
ENABLE_DBW = os.getenv("ENABLE_DBW", "1") == "1"
ENABLE_DBS = os.getenv("ENABLE_DBS", "1") == "1"

# -----------------------------
# WB API endpoints
# -----------------------------
WB_MARKETPLACE_BASE = "https://marketplace-api.wildberries.ru"
WB_FEEDBACKS_BASE = "https://feedbacks-api.wildberries.ru"

FBS_NEW_ORDERS_URL = f"{WB_MARKETPLACE_BASE}/api/v3/orders/new"
DBW_NEW_ORDERS_URL = f"{WB_MARKETPLACE_BASE}/api/v3/dbw/orders/new"
DBS_NEW_ORDERS_URL = f"{WB_MARKETPLACE_BASE}/api/v3/dbs/orders/new"

FEEDBACKS_LIST_URL = f"{WB_FEEDBACKS_BASE}/api/v1/feedbacks"

# -----------------------------
# Dedup storage (sqlite)
# -----------------------------
DB_PATH = "state.db"

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen (
            key TEXT PRIMARY KEY,
            ts  INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    return conn

_conn = db()

def seen_before(key: str) -> bool:
    cur = _conn.execute("SELECT 1 FROM seen WHERE key = ?", (key,))
    return cur.fetchone() is not None

def mark_seen(key: str) -> None:
    _conn.execute(
        "INSERT OR IGNORE INTO seen(key, ts) VALUES(?, ?)",
        (key, int(time.time())),
    )
    _conn.commit()

def cleanup_old(days: int = 30) -> None:
    # чтобы база не росла вечно
    cutoff = int(time.time()) - days * 24 * 3600
    _conn.execute("DELETE FROM seen WHERE ts < ?", (cutoff,))
    _conn.commit()

# -----------------------------
# Telegram
# -----------------------------
def tg_send(text: str) -> Dict[str, Any]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return {"error": "No TG_BOT_TOKEN or TG_CHAT_ID"}

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=20)
    try:
        return r.json()
    except Exception:
        return {"error": "Bad telegram response", "status_code": r.status_code, "text": r.text}

# -----------------------------
# WB helpers
# -----------------------------
def wb_headers(token: str) -> Dict[str, str]:
    # WB в документации пишет HeaderApiKey (обычно Authorization: <token>)
    return {"Authorization": token}

def safe_get(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Any:
    r = requests.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    # WB часто возвращает JSON
    return r.json()

# -----------------------------
# POLL: Orders (FBS/DBW/DBS)
# -----------------------------
def format_order_message(model: str, order: Dict[str, Any]) -> str:
    # поля у разных моделей могут отличаться — делаем “человеческое” сообщение из того, что есть
    order_id = order.get("id") or order.get("orderId") or order.get("order") or "?"
    created = order.get("createdAt") or order.get("createdAtUtc") or order.get("created") or ""
    status = order.get("status") or order.get("state") or ""
    wb_article = order.get("nmId") or order.get("nmID") or order.get("nm") or ""
    return (
        f"🛒 Новый заказ ({model})\n"
        f"ID: {order_id}\n"
        f"Статус: {status}\n"
        f"Артикул (если есть): {wb_article}\n"
        f"Дата (если есть): {created}"
    ).strip()

def poll_orders_once() -> List[Dict[str, Any]]:
    if not WB_TOKEN:
        return [{"error": "No WB_TOKEN (for orders)"}]

    headers = wb_headers(WB_TOKEN)
    results: List[Dict[str, Any]] = []

    def handle_orders(model: str, url: str):
        try:
            data = safe_get(url, headers=headers)
            # обычно это список
            if isinstance(data, dict) and "orders" in data:
                orders = data.get("orders", [])
            else:
                orders = data if isinstance(data, list) else []

            sent = 0
            for o in orders:
                oid = o.get("id") or o.get("orderId") or o.get("order")
                if oid is None:
                    # если нет id — на всякий случай хэшируем весь объект
                    oid = json.dumps(o, ensure_ascii=False, sort_keys=True)
                key = f"order:{model}:{oid}"
                if seen_before(key):
                    continue
                msg = format_order_message(model, o)
                tg_send(msg)
                mark_seen(key)
                sent += 1

            results.append({"model": model, "found": len(orders), "sent_new": sent})
        except Exception as e:
            results.append({"model": model, "error": str(e)})

    if ENABLE_FBS:
        handle_orders("FBS", FBS_NEW_ORDERS_URL)
    if ENABLE_DBW:
        handle_orders("DBW", DBW_NEW_ORDERS_URL)
    if ENABLE_DBS:
        handle_orders("DBS", DBS_NEW_ORDERS_URL)

    return results

# -----------------------------
# POLL: Feedbacks
# -----------------------------
def format_feedback_message(fb: Dict[str, Any]) -> str:
    fb_id = fb.get("id") or "?"
    nm_id = fb.get("nmId") or ""
    rating = fb.get("productValuation") or fb.get("valuation") or fb.get("rate") or ""
    text = fb.get("text") or fb.get("feedbackText") or ""
    created = fb.get("createdDate") or fb.get("createdAt") or ""
    user = fb.get("userName") or fb.get("buyerName") or ""

    # ограничим длину, чтобы не улетать простынёй
    if isinstance(text, str) and len(text) > 800:
        text = text[:800] + "…"

    return (
        f"⭐️ Новый отзыв\n"
        f"ID: {fb_id}\n"
        f"Артикул: {nm_id}\n"
        f"Оценка: {rating}\n"
        f"Покупатель: {user}\n"
        f"Дата: {created}\n\n"
        f"{text}"
    ).strip()

def poll_feedbacks_once() -> Dict[str, Any]:
    if not WB_FEEDBACKS_TOKEN:
        return {"error": "No WB_FEEDBACKS_TOKEN (or WB_TOKEN) for feedbacks"}

    headers = wb_headers(WB_FEEDBACKS_TOKEN)

    # ВАЖНО: у /api/v1/feedbacks есть фильтры/пагинация, но даже без них
    # можно получать последние и дедупить у себя.
    params = {
        "take": 50,        # сколько взять (если API поддержит)
        "skip": 0,
        "order": "dateDesc"  # если поддержит
    }

    try:
        data = safe_get(FEEDBACKS_LIST_URL, headers=headers, params=params)

        # бывает формат list или dict с data
        if isinstance(data, dict):
            feedbacks = data.get("data") or data.get("feedbacks") or data.get("result") or []
        else:
            feedbacks = data if isinstance(data, list) else []

        sent = 0
        for fb in feedbacks:
            fb_id = fb.get("id")
            if fb_id is None:
                fb_id = json.dumps(fb, ensure_ascii=False, sort_keys=True)
            key = f"feedback:{fb_id}"
            if seen_before(key):
                continue
            tg_send(format_feedback_message(fb))
            mark_seen(key)
            sent += 1

        return {"found": len(feedbacks), "sent_new": sent}
    except Exception as e:
        return {"error": str(e)}

# -----------------------------
# Background loops
# -----------------------------
async def orders_loop():
    while True:
        try:
            if ENABLE_ORDERS:
                poll_orders_once()
                cleanup_old(30)
        except Exception:
            pass
        await asyncio.sleep(POLL_SECONDS)

async def feedbacks_loop():
    while True:
        try:
            if ENABLE_FEEDBACKS:
                poll_feedbacks_once()
                cleanup_old(30)
        except Exception:
            pass
        await asyncio.sleep(FEEDBACKS_POLL_SECONDS)

@app.on_event("startup")
async def on_startup():
    # Сообщим, что сервис стартовал (1 раз) — тоже без дублей
    start_key = "service:started"
    if not seen_before(start_key):
        tg_send("✅ WB→Telegram запущен. Жду отзывы и новые заказы (polling).")
        mark_seen(start_key)

    asyncio.create_task(orders_loop())
    asyncio.create_task(feedbacks_loop())

# -----------------------------
# HTTP endpoints (для проверки)
# -----------------------------
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/test-telegram")
def test_telegram():
    res = tg_send("✅ Тест: сообщение из облачного сервера Render")
    return {"telegram_result": res}

@app.get("/poll-once")
def poll_once():
    orders = poll_orders_once() if ENABLE_ORDERS else {"disabled": True}
    feedbacks = poll_feedbacks_once() if ENABLE_FEEDBACKS else {"disabled": True}
    return {"orders": orders, "feedbacks": feedbacks}
