import os
import json
import time
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Request, HTTPException

app = FastAPI()

# -------------------------
# Config
# -------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()         # statistics-api (FBW)
WB_FEEDBACKS_TOKEN = os.getenv("WB_FEEDBACKS_TOKEN", "").strip() # feedbacks-api (reviews)
WB_WEBHOOK_SECRET = os.getenv("WB_WEBHOOK_SECRET", "").strip()

WB_CONTENT_TOKEN = os.getenv("WB_CONTENT_TOKEN", "").strip()     # content-api (для title)
SHOP_NAME = os.getenv("SHOP_NAME", "Bright Shop").strip()

DB_PATH = os.getenv("DB_PATH", "/tmp/wb_telegram.sqlite").strip()

POLL_FBW_SECONDS = int(os.getenv("POLL_FBW_SECONDS", "1800"))
POLL_FEEDBACKS_SECONDS = int(os.getenv("POLL_FEEDBACKS_SECONDS", "60"))

DISABLE_STARTUP_HELLO = os.getenv("DISABLE_STARTUP_HELLO", "0").strip() == "1"

WB_STATISTICS_BASE = "https://statistics-api.wildberries.ru"
WB_FEEDBACKS_BASE = "https://feedbacks-api.wildberries.ru"
WB_CONTENT_BASE = "https://content-api.wildberries.ru"


# -------------------------
# Helpers: misc
# -------------------------
def _safe_str(x) -> str:
    return "" if x is None else str(x).strip()

def _rub(x) -> str:
    try:
        v = float(x)
        if abs(v - int(v)) < 1e-9:
            return f"{int(v)} ₽"
        return f"{v:.2f} ₽"
    except Exception:
        return "-"

def _format_dt_ru(iso: str) -> str:
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso

def _stars(rating: int) -> str:
    rating = max(0, min(5, int(rating)))
    return "★" * rating + "☆" * (5 - rating)

def tg_word_stars(n: int) -> str:
    n = abs(int(n))
    if 11 <= (n % 100) <= 14:
        return "звёзд"
    last = n % 10
    if last == 1:
        return "звезда"
    if 2 <= last <= 4:
        return "звезды"
    return "звёзд"


# -------------------------
# Helpers: DB
# -------------------------
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sent_events (
            key TEXT PRIMARY KEY,
            created_at INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cursors (
            name TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    return conn

def was_sent(key: str) -> bool:
    conn = db()
    cur = conn.execute("SELECT 1 FROM sent_events WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    return row is not None

def mark_sent(key: str) -> None:
    conn = db()
    conn.execute(
        "INSERT OR REPLACE INTO sent_events(key, created_at) VALUES(?, ?)",
        (key, int(time.time()))
    )
    conn.commit()
    conn.close()

def get_cursor(name: str, default: str = "") -> str:
    conn = db()
    cur = conn.execute("SELECT value FROM cursors WHERE name = ?", (name,))
    row = cur.fetchone()
    if row is None:
        conn.execute("INSERT OR REPLACE INTO cursors(name, value) VALUES(?, ?)", (name, default))
        conn.commit()
        conn.close()
        return default
    conn.close()
    return row[0] or default

def set_cursor(name: str, value: str) -> None:
    conn = db()
    conn.execute("INSERT OR REPLACE INTO cursors(name, value) VALUES(?, ?)", (name, value))
    conn.commit()
    conn.close()


# -------------------------
# Telegram
# -------------------------
def tg_send(text: str) -> Dict[str, Any]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return {"ok": False, "error": "No TG_BOT_TOKEN or TG_CHAT_ID"}

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text[:3900],
        "disable_web_page_preview": True,
    }
    r = requests.post(url, json=payload, timeout=25)
    try:
        return r.json()
    except Exception:
        return {"ok": False, "status": r.status_code, "text": r.text}


# -------------------------
# WB requests
# -------------------------
def wb_get(url: str, token: str, params: Optional[dict] = None, timeout: int = 25) -> Any:
    headers = {"Authorization": token}
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    if r.status_code >= 400:
        return {"__error__": True, "status_code": r.status_code, "url": r.url, "response_text": r.text}
    try:
        return r.json()
    except Exception:
        return r.text

def wb_post(url: str, token: str, payload: dict, timeout: int = 25) -> Any:
    headers = {"Authorization": token}
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    if r.status_code >= 400:
        return {"__error__": True, "status_code": r.status_code, "url": r.url, "response_text": r.text}
    try:
        return r.json()
    except Exception:
        return r.text


# -------------------------
# Content API: title по nmId (кеш 24ч)
# -------------------------
_TITLE_CACHE: Dict[int, Tuple[float, str]] = {}
_TITLE_TTL = 24 * 3600

def content_get_title(nm_id: Optional[int]) -> str:
    if not WB_CONTENT_TOKEN or not nm_id:
        return ""

    now = time.time()
    if nm_id in _TITLE_CACHE:
        ts, title = _TITLE_CACHE[nm_id]
        if now - ts <= _TITLE_TTL:
            return title

    url = f"{WB_CONTENT_BASE}/content/v2/get/cards/list"
    payload = {
        "settings": {
            "filter": {"textSearch": str(nm_id)},
            "cursor": {"limit": 10},
        }
    }
    data = wb_post(url, WB_CONTENT_TOKEN, payload=payload)
    if not isinstance(data, dict) or data.get("__error__"):
        return ""

    cards = data.get("cards")
    if not isinstance(cards, list) or not cards:
        return ""

    # пытаемся найти точный nmID
    title = ""
    for c in cards:
        if isinstance(c, dict) and str(c.get("nmID")) == str(nm_id):
            title = _safe_str(c.get("title"))
            break

    if not title and isinstance(cards[0], dict):
        title = _safe_str(cards[0].get("title"))

    if title:
        _TITLE_CACHE[nm_id] = (now, title)
    return title


# -------------------------
# FBW stocks (statistics /supplier/stocks) — кеш 120с
# -------------------------
_FBW_STOCKS_CACHE: Tuple[float, List[Dict[str, Any]]] = (0.0, [])
_FBW_STOCKS_TTL = 120

def stats_fetch_fbw_stocks() -> List[Dict[str, Any]]:
    global _FBW_STOCKS_CACHE

    if not WB_STATS_TOKEN:
        return []

    now = time.time()
    ts, cached = _FBW_STOCKS_CACHE
    if cached and (now - ts) <= _FBW_STOCKS_TTL:
        return cached

    date_from = datetime.utcnow().strftime("%Y-%m-%d")
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/stocks"
    data = wb_get(url, WB_STATS_TOKEN, params={"dateFrom": date_from})

    if not isinstance(data, list):
        return []

    _FBW_STOCKS_CACHE = (now, data)
    return data

def fbw_stock_quantity(warehouse_name: str, barcode: str) -> Optional[int]:
    rows = stats_fetch_fbw_stocks()
    wn = _safe_str(warehouse_name)
    bc = _safe_str(barcode)

    if not wn or not bc:
        return None

    for r in rows:
        if not isinstance(r, dict):
            continue
        if _safe_str(r.get("warehouseName")) == wn and _safe_str(r.get("barcode")) == bc:
            try:
                return int(r.get("quantity", 0))
            except Exception:
                return None
    return None


# -------------------------
# FBW orders (statistics /supplier/orders)
# -------------------------
def msk_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))

def iso_msk(dt: datetime) -> str:
    return dt.isoformat()

def stats_fetch_orders_since(cursor_name: str) -> List[Dict[str, Any]]:
    if not WB_STATS_TOKEN:
        return []

    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/orders"
    default_dt = msk_now() - timedelta(hours=2)
    cursor = get_cursor(cursor_name, iso_msk(default_dt))

    data = wb_get(url, WB_STATS_TOKEN, params={"dateFrom": cursor})
    if isinstance(data, dict) and data.get("__error__"):
        return [{"__error__": True, **data}]

    if not isinstance(data, list) or not data:
        return []

    last = data[-1]
    if isinstance(last, dict) and last.get("lastChangeDate"):
        set_cursor(cursor_name, last["lastChangeDate"])

    return data

def format_stats_order(o: Dict[str, Any]) -> str:
    warehouse = _safe_str(o.get("warehouseName") or o.get("warehouse") or o.get("officeName") or "WB")

    # nmId иногда приходит как 537328918 или 537328918.0
    nm_id_raw = o.get("nmId") or o.get("nmID") or o.get("nm_id")
    nm_id: Optional[int] = None
    if nm_id_raw is not None:
        try:
            nm_id = int(float(nm_id_raw))
        except Exception:
            nm_id = None

    barcode = _safe_str(o.get("barcode") or o.get("barCode") or "")
    qty = o.get("quantity") or o.get("qty") or 1
    try:
        qty_int = int(qty)
    except Exception:
        qty_int = 1
    if qty_int <= 0:
        qty_int = 1

    price = (
        o.get("priceWithDisc")
        or o.get("finishedPrice")
        or o.get("forPay")
        or o.get("totalPrice")
        or o.get("price")
        or 0
    )

    # 1) пробуем красивое название из Content API
    product_name = content_get_title(nm_id)

    # 2) если вдруг не получилось — берём то, что пришло
    if not product_name:
        product_name = _safe_str(
            o.get("nmName")
            or o.get("productName")
            or o.get("subjectName")
            or o.get("subject")
            or "Товар"
        )

    # FBW остаток по складу+barcode
    ost_line = "Остаток: -"
    q = fbw_stock_quantity(warehouse, barcode)
    if isinstance(q, int):
        ost_line = f"Остаток: {q} шт"

    header = f"🏬 Заказ товара со склада ({warehouse}) · {SHOP_NAME}"
    body = (
        f"📦 Склад отгрузки: {warehouse}\n"
        f"• {product_name}\n"
        f"  Артикул: {nm_id or '-'}\n"
        f"  — {qty_int} шт • цена покупателя - {_rub(price)}\n"
        f"{ost_line}\n"
        f"Итого позиций: {qty_int}\n"
        f"Сумма: {_rub(price)}"
    )
    return f"{header}\n{body}".strip()

async def poll_fbw_loop():
    while True:
        try:
            rows = stats_fetch_orders_since("stats_orders_cursor")

            if rows and isinstance(rows[0], dict) and rows[0].get("__error__"):
                ek = f"err:stats_orders:{rows[0].get('status_code')}:{rows[0].get('url','')}"
                if not was_sent(ek):
                    tg_send(f"⚠️ statistics orders error: {rows[0].get('status_code')} {rows[0].get('response_text','')[:300]}")
                    mark_sent(ek)
            else:
                for o in rows:
                    if not isinstance(o, dict) or not o.get("lastChangeDate"):
                        continue
                    # ключ дедупа
                    key = f"stats:order:{o.get('srid','-')}:{o.get('lastChangeDate','-')}:{o.get('barcode','-')}"
                    if was_sent(key):
                        continue
                    res = tg_send(format_stats_order(o))
                    if res.get("ok"):
                        mark_sent(key)

        except Exception as e:
            ek = f"err:stats:{type(e).__name__}:{str(e)[:160]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка statistics polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_FBW_SECONDS)


# -------------------------
# Feedbacks
# -------------------------
def feedbacks_fetch_latest() -> List[Dict[str, Any]]:
    if not WB_FEEDBACKS_TOKEN:
        return []

    url = f"{WB_FEEDBACKS_BASE}/api/v1/feedbacks"
    out: List[Dict[str, Any]] = []

    for is_answered in (False, True):
        data = wb_get(
            url,
            WB_FEEDBACKS_TOKEN,
            params={"isAnswered": str(is_answered).lower(), "take": 100, "skip": 0, "order": "dateDesc"},
        )
        if isinstance(data, dict) and data.get("__error__"):
            out.append({"__error__": True, **data, "__stage__": f"feedbacks isAnswered={is_answered}"})
            continue

        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            fb = data["data"].get("feedbacks", [])
            if isinstance(fb, list):
                for x in fb:
                    if isinstance(x, dict):
                        out.append(x)

    return out

def format_feedback(f: Dict[str, Any]) -> str:
    rating = f.get("productValuation")
    try:
        rating_int = int(rating) if rating is not None else 0
    except Exception:
        rating_int = 0

    mood = "Хороший отзыв" if rating_int >= 4 else "Плохой отзыв"
    product_name = _safe_str(f.get("productName") or f.get("nmName") or f.get("subjectName") or "Без названия")
    article = _safe_str(f.get("supplierArticle") or f.get("vendorCode") or f.get("article") or f.get("nmId") or "")
    text = _safe_str(f.get("text") or "")

    text_line = "Отзыв: (без текста, только оценка)" if not text else f"Отзыв: {text}"
    created = _format_dt_ru(_safe_str(f.get("createdDate") or ""))
    stars = _stars(rating_int)
    stars_word = tg_word_stars(rating_int)

    return (
        f"💬 Новый отзыв о товаре · ({SHOP_NAME})\n"
        f"Товар: {product_name} ({article})\n"
        f"Оценка: {stars} {rating_int} {stars_word} ({mood})\n"
        f"{text_line}\n"
        f"Дата: {created}"
    ).strip()

def prime_feedbacks_silently() -> None:
    try:
        items = feedbacks_fetch_latest()
        for it in items:
            if isinstance(it, dict) and it.get("__error__"):
                return
        for f in items:
            if not isinstance(f, dict):
                continue
            fid = _safe_str(f.get("id"))
            if fid and not was_sent(f"feedback:{fid}"):
                mark_sent(f"feedback:{fid}")
    except Exception:
        pass

async def poll_feedbacks_loop():
    while True:
        try:
            items = feedbacks_fetch_latest()

            for it in items:
                if isinstance(it, dict) and it.get("__error__"):
                    ek = f"err:feedbacks:{it.get('status_code')}:{it.get('__stage__','')}"
                    if not was_sent(ek):
                        tg_send(f"⚠️ feedbacks error: {it.get('status_code')} {it.get('response_text','')[:300]}")
                        mark_sent(ek)
                    continue

            for f in items:
                if not isinstance(f, dict) or f.get("__error__"):
                    continue
                fid = _safe_str(f.get("id"))
                if not fid:
                    continue
                key = f"feedback:{fid}"
                if was_sent(key):
                    continue
                res = tg_send(format_feedback(f))
                if res.get("ok"):
                    mark_sent(key)

        except Exception as e:
            ek = f"err:feedbacks:{type(e).__name__}:{str(e)[:160]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка feedbacks polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_FEEDBACKS_SECONDS)


# -------------------------
# Optional: WB webhook receiver
# -------------------------
@app.post("/wb-webhook/{secret}")
async def wb_webhook(secret: str, request: Request):
    if not WB_WEBHOOK_SECRET or secret != WB_WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

    payload = await request.json()
    key = f"webhook:{abs(hash(json.dumps(payload, ensure_ascii=False, sort_keys=True)))}"
    if was_sent(key):
        return {"ok": True, "dedup": True}

    tg_send("📩 WB webhook событие\n" + json.dumps(payload, ensure_ascii=False)[:3500])
    mark_sent(key)
    return {"ok": True}


# -------------------------
# Manual endpoints
# -------------------------
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/test-telegram")
def test_telegram():
    return {"telegram_result": tg_send("✅ Тест: сообщение из Render")}

@app.get("/ping-content")
def ping_content():
    if not WB_CONTENT_TOKEN:
        return {"ok": False, "error": "WB_CONTENT_TOKEN is not set"}
    return wb_get("https://content-api.wildberries.ru/ping", WB_CONTENT_TOKEN)

@app.get("/test-fbw-stocks")
def test_fbw_stocks():
    if not WB_STATS_TOKEN:
        return {"ok": False, "error": "no WB_STATS_TOKEN"}
    date_from = datetime.utcnow().strftime("%Y-%m-%d")
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/stocks"
    return wb_get(url, WB_STATS_TOKEN, params={"dateFrom": date_from})


# -------------------------
# Startup
# -------------------------
@app.on_event("startup")
async def startup():
    _ = db()
    prime_feedbacks_silently()

    asyncio.create_task(poll_fbw_loop())
    asyncio.create_task(poll_feedbacks_loop())

    if not DISABLE_STARTUP_HELLO:
        tg_send("✅ WB→Telegram запущен. FBW заказы (Statistics) + отзывы + полные названия (Content API).")
