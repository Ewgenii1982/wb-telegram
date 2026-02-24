import os
import time
import json
import sqlite3
import asyncio
from datetime import datetime, timedelta, timezone

import requests
from fastapi import FastAPI

app = FastAPI()

# =========================
# НАСТРОЙКИ И ПЕРЕМЕННЫЕ
# =========================
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

# Токен WB "Статистика" (заказы/продажи/возвраты)
WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()

# Токен WB "Отзывы и вопросы"
WB_FEEDBACKS_TOKEN = os.getenv("WB_FEEDBACKS_TOKEN", "").strip()

# Как часто опрашиваем WB (сек)
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "120"))

# В какое время слать суточную сводку (по МСК)
DAILY_HOUR_MSK = int(os.getenv("DAILY_HOUR_MSK", "23"))
DAILY_MINUTE_MSK = int(os.getenv("DAILY_MINUTE_MSK", "59"))

# SQLite база для антидублей (persist в рамках инстанса)
DB_PATH = os.getenv("DB_PATH", "state.db")

MSK = timezone(timedelta(hours=3))  # WB в документации пишет, что время МСК (UTC+3) :contentReference[oaicite:0]{index=0}


# =========================
# БАЗА (АНТИДУБЛИ)
# =========================
def db_init():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_events (
            event_type TEXT NOT NULL,
            event_id TEXT NOT NULL,
            sent_at INTEGER NOT NULL,
            PRIMARY KEY (event_type, event_id)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS kv (
            k TEXT PRIMARY KEY,
            v TEXT NOT NULL
        )
    """)
    con.commit()
    con.close()


def db_was_sent(event_type: str, event_id: str) -> bool:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT 1 FROM sent_events WHERE event_type=? AND event_id=? LIMIT 1", (event_type, event_id))
    row = cur.fetchone()
    con.close()
    return row is not None


def db_mark_sent(event_type: str, event_id: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO sent_events(event_type, event_id, sent_at) VALUES (?, ?, ?)",
        (event_type, event_id, int(time.time()))
    )
    con.commit()
    con.close()


def kv_get(k: str, default: str = "") -> str:
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT v FROM kv WHERE k=? LIMIT 1", (k,))
    row = cur.fetchone()
    con.close()
    return row[0] if row else default


def kv_set(k: str, v: str):
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, v))
    con.commit()
    con.close()


# =========================
# TELEGRAM
# =========================
def tg_send(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return {"error": "No TG_BOT_TOKEN or TG_CHAT_ID"}

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True
    }
    r = requests.post(url, json=payload, timeout=25)
    try:
        return r.json()
    except Exception:
        return {"error": "Bad telegram response", "status_code": r.status_code, "text": r.text}


# =========================
# WB API: ОТЗЫВЫ
# Док: /api/v1/feedbacks и поле productValuation (оценка) :contentReference[oaicite:1]{index=1}
# =========================
def wb_get_feedbacks_since(ts_from_unix: int, ts_to_unix: int):
    """
    Берём отзывы за период, сортировка по дате (новые сверху),
    и берём пачку (take) побольше.
    """
    if not WB_FEEDBACKS_TOKEN:
        return {"error": "No WB_FEEDBACKS_TOKEN"}

    url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
    headers = {"Authorization": WB_FEEDBACKS_TOKEN}

    params = {
        "isAnswered": "true",     # нам нужны все, не только неотвеченные
        "take": 100,
        "skip": 0,
        "order": "dateDesc",
        "dateFrom": ts_from_unix,
        "dateTo": ts_to_unix
    }

    r = requests.get(url, headers=headers, params=params, timeout=25)
    r.raise_for_status()
    return r.json()


def format_feedback_message(fb: dict) -> str:
    rating = fb.get("productValuation", 0)  # поле из доков :contentReference[oaicite:2]{index=2}
    tag = "✅ ХОРОШИЙ отзыв" if rating >= 4 else "❌ ПЛОХОЙ отзыв"

    text = (fb.get("text") or "").strip()
    pros = (fb.get("pros") or "").strip()
    cons = (fb.get("cons") or "").strip()

    product = fb.get("productDetails") or {}
    product_name = product.get("productName") or "Товар"
    supplier_article = product.get("supplierArticle") or ""

    created = fb.get("createdDate") or ""

    parts = [
        f"{tag} ({rating}⭐)",
        f"Товар: {product_name}" + (f" / {supplier_article}" if supplier_article else ""),
        f"Дата: {created}",
    ]
    if text:
        parts.append(f"Текст: {text}")
    if pros:
        parts.append(f"Плюсы: {pros}")
    if cons:
        parts.append(f"Минусы: {cons}")

    return "\n".join(parts)


# =========================
# WB API: ЗАКАЗЫ (FBS) и ПРОДАЖИ/ВОЗВРАТЫ (для сводки)
# Sales метод возвращает и продажи, и возвраты :contentReference[oaicite:3]{index=3}
# =========================
def wb_get_orders_changed_since(date_from_rfc3339: str):
    """
    Заказы из статистики. Здесь логика "оперативного мониторинга".
    (Эндпоинт в статистике: /api/v1/supplier/orders — рядом с sales в этом же разделе доков) :contentReference[oaicite:4]{index=4}
    """
    if not WB_STATS_TOKEN:
        return {"error": "No WB_STATS_TOKEN"}

    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    headers = {"Authorization": WB_STATS_TOKEN}
    params = {"dateFrom": date_from_rfc3339, "flag": 0}

    r = requests.get(url, headers=headers, params=params, timeout=25)
    r.raise_for_status()
    return r.json()


def wb_get_sales_for_date(date_yyyy_mm_dd: str):
    """
    Берём продажи/возвраты за конкретную дату: flag=1, dateFrom=YYYY-MM-DD
    Док по sales: /api/v1/supplier/sales :contentReference[oaicite:5]{index=5}
    """
    if not WB_STATS_TOKEN:
        return {"error": "No WB_STATS_TOKEN"}

    url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
    headers = {"Authorization": WB_STATS_TOKEN}
    params = {"dateFrom": date_yyyy_mm_dd, "flag": 1}

    r = requests.get(url, headers=headers, params=params, timeout=25)
    r.raise_for_status()
    return r.json()


def is_return_sale_row(row: dict) -> bool:
    """
    В sales отчёте одна строка = одна продажа/возврат (item) :contentReference[oaicite:6]{index=6}
    У WB обычно возвраты отличаются saleID (часто начинается на 'R').
    Если вдруг у тебя будет иначе — скажешь, подстроим правило.
    """
    sale_id = str(row.get("saleID", "")).upper()
    return sale_id.startswith("R")


def format_order_message(order: dict) -> str:
    # Поля могут отличаться, поэтому делаем "мягко"
    g_number = order.get("gNumber") or order.get("srid") or "—"
    nm_id = order.get("nmId") or "—"
    vendor = order.get("supplierArticle") or "—"
    wh = order.get("warehouseName") or order.get("warehouse") or "—"
    created = order.get("date") or order.get("lastChangeDate") or "—"

    return "\n".join([
        "🛒 Новый заказ (FBS)",
        f"Заказ: {g_number}",
        f"Артикул WB (nmId): {nm_id}",
        f"Артикул продавца: {vendor}",
        f"Склад: {wh}",
        f"Дата: {created}",
    ])


# =========================
# ПОЛЛИНГ ЛОГИКА
# =========================
async def poll_feedbacks_loop():
    """
    Раз в POLL_SECONDS смотрим отзывы за последние N минут
    и отправляем только новые (по id).
    """
    while True:
        try:
            if WB_FEEDBACKS_TOKEN and TG_BOT_TOKEN and TG_CHAT_ID:
                now = int(time.time())
                window_from = now - 6 * 60 * 60  # смотрим последние 6 часов (чтобы не пропустить)
                data = wb_get_feedbacks_since(window_from, now)
                feedbacks = ((data or {}).get("data") or {}).get("feedbacks") or []

                # новые отзывы обычно сверху, но нам всё равно
                for fb in reversed(feedbacks):
                    fb_id = fb.get("id")
                    if not fb_id:
                        continue
                    if db_was_sent("feedback", fb_id):
                        continue

                    msg = format_feedback_message(fb)
                    tg_send(msg)
                    db_mark_sent("feedback", fb_id)
        except Exception as e:
            # чтобы сервис не падал
            tg_send(f"⚠️ Ошибка poll_feedbacks: {e}")
        await asyncio.sleep(POLL_SECONDS)


async def poll_orders_loop():
    """
    Заказы: используем lastChangeDate "курсор":
    - при первом старте берём 'сейчас - 10 минут'
    - дальше берём сохранённый lastChangeDate
    """
    while True:
        try:
            if WB_STATS_TOKEN and TG_BOT_TOKEN and TG_CHAT_ID:
                cursor = kv_get("orders_cursor", "")
                if not cursor:
                    dt = datetime.now(MSK) - timedelta(minutes=10)
                    cursor = dt.isoformat()
                orders = wb_get_orders_changed_since(cursor)

                if isinstance(orders, list) and orders:
                    # отправим новые
                    for o in orders:
                        # srid/odid/gNumber — что есть, тем и идентифицируем
                        oid = str(o.get("srid") or o.get("odid") or o.get("gNumber") or "")
                        if not oid:
                            continue
                        if db_was_sent("order", oid):
                            continue

                        tg_send(format_order_message(o))
                        db_mark_sent("order", oid)

                    # обновляем курсор по последней строке (lastChangeDate)
                    last = orders[-1].get("lastChangeDate") or orders[-1].get("date") or cursor
                    kv_set("orders_cursor", str(last))
        except Exception as e:
            tg_send(f"⚠️ Ошибка poll_orders: {e}")
        await asyncio.sleep(POLL_SECONDS)


async def daily_summary_loop():
    """
    Раз в минуту проверяем: наступило ли время DAILY_HOUR_MSK:DAILY_MINUTE_MSK (по МСК).
    Если да — шлём сводку за текущую дату.
    """
    while True:
        try:
            now_msk = datetime.now(MSK)
            hh = now_msk.hour
            mm = now_msk.minute

            # защита от повторной отправки в тот же день
            today_key = now_msk.strftime("%Y-%m-%d")
            last_sent = kv_get("daily_summary_date", "")

            if hh == DAILY_HOUR_MSK and mm == DAILY_MINUTE_MSK and last_sent != today_key:
                date_str = today_key
                sales = wb_get_sales_for_date(date_str)

                sold_cnt = 0
                sold_sum = 0.0
                buyout_sum = 0.0

                return_cnt = 0
                return_sum = 0.0

                if isinstance(sales, list):
                    for row in sales:
                        price = float(row.get("priceWithDisc") or 0)   # оперативная сумма :contentReference[oaicite:7]{index=7}
                        forpay = float(row.get("forPay") or 0)         # к выплате :contentReference[oaicite:8]{index=8}

                        if is_return_sale_row(row):
                            return_cnt += 1
                            return_sum += price
                        else:
                            sold_cnt += 1
                            sold_sum += price
                            buyout_sum += forpay

                # Были ли отзывы сегодня?
                start_day = int(datetime(now_msk.year, now_msk.month, now_msk.day, 0, 0, 0, tzinfo=MSK).timestamp())
                end_day = int((datetime(now_msk.year, now_msk.month, now_msk.day, 23, 59, 59, tzinfo=MSK)).timestamp())

                fb_data = wb_get_feedbacks_since(start_day, end_day)
                fb_list = ((fb_data or {}).get("data") or {}).get("feedbacks") or []
                has_feedbacks = "есть ✅" if len(fb_list) > 0 else "нет ❌"

                msg = "\n".join([
                    f"📊 Сводка за {date_str} (МСК)",
                    f"Продано: {sold_cnt} шт на {sold_sum:,.0f} ₽",
                    f"Выкуп (к выплате): {buyout_sum:,.0f} ₽",
                    f"Отказы/возвраты: {return_cnt} шт на {return_sum:,.0f} ₽",
                    f"Отзывы сегодня: {has_feedbacks}",
                ]).replace(",", " ")

                tg_send(msg)
                kv_set("daily_summary_date", today_key)

        except Exception as e:
            tg_send(f"⚠️ Ошибка daily_summary: {e}")

        await asyncio.sleep(60)


# =========================
# FASTAPI ROUTES
# =========================
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/test-telegram")
def test_telegram():
    res = tg_send("✅ Тест: сообщение из облачного сервера Render")
    return {"telegram_result": res}

@app.get("/poll-once")
def poll_once():
    """
    Ручной запуск одного прохода (удобно для проверки).
    """
    result = {"orders": "skipped", "feedbacks": "skipped"}

    # Заказы
    try:
        cursor = kv_get("orders_cursor", "")
        if not cursor:
            dt = datetime.now(MSK) - timedelta(minutes=30)
            cursor = dt.isoformat()
        orders = wb_get_orders_changed_since(cursor)
        sent = 0
        if isinstance(orders, list):
            for o in orders:
                oid = str(o.get("srid") or o.get("odid") or o.get("gNumber") or "")
                if oid and not db_was_sent("order", oid):
                    tg_send(format_order_message(o))
                    db_mark_sent("order", oid)
                    sent += 1
            if orders:
                last = orders[-1].get("lastChangeDate") or orders[-1].get("date") or cursor
                kv_set("orders_cursor", str(last))
        result["orders"] = f"ok, sent={sent}"
    except Exception as e:
        result["orders"] = f"error: {e}"

    # Отзывы
    try:
        now = int(time.time())
        window_from = now - 24 * 60 * 60
        data = wb_get_feedbacks_since(window_from, now)
        feedbacks = ((data or {}).get("data") or {}).get("feedbacks") or []
        sent = 0
        for fb in reversed(feedbacks):
            fb_id = fb.get("id")
            if fb_id and not db_was_sent("feedback", fb_id):
                tg_send(format_feedback_message(fb))
                db_mark_sent("feedback", fb_id)
                sent += 1
        result["feedbacks"] = f"ok, sent={sent}"
    except Exception as e:
        result["feedbacks"] = f"error: {e}"

    return result


# =========================
# STARTUP
# =========================
@app.on_event("startup")
async def on_startup():
    db_init()
    # маленькое стартовое сообщение
    if TG_BOT_TOKEN and TG_CHAT_ID:
        tg_send("✅ WB→Telegram запущен. Жду отзывы и заказы. Сводка будет раз в сутки.")

    # запускаем фоновые циклы
    asyncio.create_task(poll_feedbacks_loop())
    asyncio.create_task(poll_orders_loop())
    asyncio.create_task(daily_summary_loop())
