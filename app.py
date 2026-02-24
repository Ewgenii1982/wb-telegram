import os
import time
import json
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Union

import requests
from fastapi import FastAPI

app = FastAPI()

# =========================
# ENV
# =========================
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

# Разделяем токены, чтобы не путаться
WB_MARKETPLACE_TOKEN = os.getenv("WB_MARKETPLACE_TOKEN", "").strip()  # marketplace-api (FBS/DBS/DBW)
WB_FEEDBACKS_TOKEN = os.getenv("WB_FEEDBACKS_TOKEN", "").strip()      # feedbacks-api (отзывы)

POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))

DAILY_HOUR_MSK = int(os.getenv("DAILY_HOUR_MSK", "23"))
DAILY_MINUTE_MSK = int(os.getenv("DAILY_MINUTE_MSK", "59"))

STATE_FILE = "state.json"


# =========================
# STATE (anti-duplicate)
# =========================
def load_state() -> Dict[str, Any]:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {"sent": {}, "daily_last_sent": None}


def save_state(st: Dict[str, Any]) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, indent=2)


state = load_state()


def was_sent(key: str) -> bool:
    return key in state.get("sent", {})


def mark_sent(key: str) -> None:
    state.setdefault("sent", {})[key] = int(time.time())
    save_state(state)


# =========================
# TELEGRAM
# =========================
def tg_send(text: str) -> Dict[str, Any]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return {"error": "No TG_BOT_TOKEN or TG_CHAT_ID"}

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text}
    try:
        r = requests.post(url, json=payload, timeout=30)
        return r.json()
    except Exception as e:
        return {"error": str(e)}


# =========================
# HELPERS / FORMAT
# =========================
def safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


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
    rating = safe_int(
        fb.get("productValuation")
        or fb.get("valuation")
        or fb.get("rate")
        or 0
    )
    label = "🟢 Хороший отзыв" if rating >= 4 else "🔴 Плохой отзыв"

    nm_id = fb.get("nmId") or "—"
    user = fb.get("userName") or fb.get("buyerName") or "—"
    created = fb.get("createdDate") or fb.get("createdAt") or "—"
    text = fb.get("text") or fb.get("feedbackText") or ""
    if isinstance(text, str) and len(text) > 800:
        text = text[:800] + "…"

    return (
        f"{label}\n"
        f"⭐ Оценка: {rating}\n"
        f"nmId: {nm_id}\n"
        f"Покупатель: {user}\n"
        f"Дата: {created}\n\n"
        f"{text}"
    )


# =========================
# WB API (safe)
# =========================
def wb_get_new_orders(url: str) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Возвращает:
    - список заказов (list[dict]) при успехе
    - dict с __error__ при ошибке (чтобы показать диагностику)
    """
    headers = {"Authorization": WB_MARKETPLACE_TOKEN}
    try:
        r = requests.get(url, headers=headers, timeout=25)
    except Exception as e:
        return {"__error__": True, "status_code": None, "url": url, "response_text": str(e)}

    if r.status_code != 200:
        return {
            "__error__": True,
            "status_code": r.status_code,
            "url": r.url,
            "response_text": r.text[:2000],
        }

    # Парсим JSON аккуратно
    try:
        data = r.json()
    except Exception:
        return {
            "__error__": True,
            "status_code": r.status_code,
            "url": r.url,
            "response_text": "Response is not JSON: " + (r.text[:500] if r.text else ""),
        }

    # Нормальные варианты:
    if isinstance(data, dict) and isinstance(data.get("orders"), list):
        # фильтруем только dict-элементы
        return [x for x in data["orders"] if isinstance(x, dict)]

    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    # Если WB вернул что-то странное — просто пусто
    return []


def wb_get_feedbacks() -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Возвращает:
    - список отзывов (list[dict]) при успехе
    - dict с __error__ при ошибке
    """
    headers = {"Authorization": WB_FEEDBACKS_TOKEN}
    url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"

    params = {
        "isAnswered": "false",
        "take": 100,
        "skip": 0,
        "order": "dateDesc",
    }

    try:
        r = requests.get(url, headers=headers, params=params, timeout=25)
    except Exception as e:
        return {"__error__": True, "status_code": None, "url": url, "response_text": str(e)}

    if r.status_code != 200:
        return {
            "__error__": True,
            "status_code": r.status_code,
            "url": r.url,
            "response_text": r.text[:2000],
        }

    try:
        data = r.json()
    except Exception:
        return {
            "__error__": True,
            "status_code": r.status_code,
            "url": r.url,
            "response_text": "Response is not JSON: " + (r.text[:500] if r.text else ""),
        }

    feedbacks = (data.get("data") or {}).get("feedbacks") or []
    if not isinstance(feedbacks, list):
        return []
    return [x for x in feedbacks if isinstance(x, dict)]


# =========================
# POLLING LOOPS
# =========================
FBS_URL = "https://marketplace-api.wildberries.ru/api/v3/orders/new"
DBS_URL = "https://marketplace-api.wildberries.ru/api/v3/dbs/orders/new"
DBW_URL = "https://marketplace-api.wildberries.ru/api/v3/dbw/orders/new"


async def poll_orders_loop():
    while True:
        try:
            if WB_MARKETPLACE_TOKEN:
                for model, url in [("FBS", FBS_URL), ("DBS", DBS_URL), ("DBW", DBW_URL)]:
                    orders = wb_get_new_orders(url)

                    # если ошибка — просто в логи (не в TG), чтобы не спамить
                    if isinstance(orders, dict) and orders.get("__error__"):
                        # можно включить tg_send если хочешь видеть ошибки
                        # tg_send(f"⚠️ WB orders error: {orders}")
                        continue

                    for o in orders:
                        if not isinstance(o, dict):
                            continue
                        oid = o.get("id") or o.get("orderId") or o.get("rid") or o.get("srid")
                        if not oid:
                            continue

                        key = f"order:{model}:{oid}"
                        if was_sent(key):
                            continue

                        tg_send(format_order(o, model))
                        mark_sent(key)

        except Exception:
            # молча, чтобы не спамить в TG
            pass

        await asyncio.sleep(POLL_SECONDS)


async def poll_feedbacks_loop():
    while True:
        try:
            if WB_FEEDBACKS_TOKEN:
                feedbacks = wb_get_feedbacks()

                if isinstance(feedbacks, dict) and feedbacks.get("__error__"):
                    # tg_send(f"⚠️ WB feedbacks error: {feedbacks}")
                    await asyncio.sleep(POLL_SECONDS)
                    continue

                for fb in feedbacks:
                    if not isinstance(fb, dict):
                        continue
                    fid = fb.get("id")
                    if not fid:
                        continue

                    key = f"feedback:{fid}"
                    if was_sent(key):
                        continue

                    tg_send(format_feedback(fb))
                    mark_sent(key)

        except Exception:
            pass

        await asyncio.sleep(POLL_SECONDS)


# =========================
# DAILY SUMMARY (простая, без продаж пока)
# =========================
def is_time_for_daily() -> bool:
    msk = timezone(timedelta(hours=3))
    now = datetime.now(msk)

    last = state.get("daily_last_sent")
    if now.hour == DAILY_HOUR_MSK and now.minute >= DAILY_MINUTE_MSK:
        if not last:
            return True
        try:
            last_dt = datetime.fromisoformat(last)
            return last_dt.date() != now.date()
        except Exception:
            return True
    return False


async def daily_summary_loop():
    while True:
        try:
            if TG_BOT_TOKEN and TG_CHAT_ID and is_time_for_daily():
                tg_send("📊 Суточная сводка WB (следующим шагом добавим продажи/выкупы/отказы суммами).")
                state["daily_last_sent"] = datetime.now(timezone.utc).isoformat()
                save_state(state)
        except Exception:
            pass

        await asyncio.sleep(60)


# =========================
# ROUTES
# =========================
@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/test-telegram")
def test_telegram():
    return tg_send("✅ Тест: сообщение из Render")


@app.get("/poll-once")
def poll_once():
    sent_orders = 0
    sent_feedbacks = 0

    # --- orders
    try:
        for model, url in [("FBS", FBS_URL), ("DBS", DBS_URL), ("DBW", DBW_URL)]:
            orders = wb_get_new_orders(url)

            if isinstance(orders, dict) and orders.get("__error__"):
                return {"orders": "error", "wb": orders}

            for o in orders:
                if not isinstance(o, dict):
                    continue
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

    # --- feedbacks
    try:
        feedbacks = wb_get_feedbacks()

        if isinstance(feedbacks, dict) and feedbacks.get("__error__"):
            return {"feedbacks": "error", "wb": feedbacks}

        for fb in feedbacks:
            if not isinstance(fb, dict):
                continue
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

    return {
        "orders": "ok",
        "sent_orders": sent_orders,
        "feedbacks": "ok",
        "sent_feedbacks": sent_feedbacks,
    }


# =========================
# STARTUP
# =========================
@app.on_event("startup")
async def startup():
    # Никаких сообщений "запущен", чтобы не спамило на Render free
    asyncio.create_task(poll_orders_loop())
    asyncio.create_task(poll_feedbacks_loop())
    asyncio.create_task(daily_summary_loop())
