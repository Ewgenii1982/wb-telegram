import os
import time
import asyncio
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI

app = FastAPI()

# =========================
# CONFIG
# =========================

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()

SHOP_NAME = os.getenv("SHOP_NAME", "Bright Shop").strip()
DB_PATH = os.getenv("DB_PATH", "/tmp/wb_telegram.sqlite").strip()
POLL_FBW_SECONDS = int(os.getenv("POLL_FBW_SECONDS", "60"))

WB_STATISTICS_BASE = "https://statistics-api.wildberries.ru"
WB_CONTENT_TOKEN = os.getenv("WB_CONTENT_TOKEN", "").strip()
WB_CONTENT_BASE = "https://content-api.wildberries.ru"


# =========================
# HELPERS
# =========================

def _safe_str(x) -> str:
    return "" if x is None else str(x).strip()


def _rub(x) -> str:
    try:
        v = float(x)
        if abs(v - int(v)) < 1e-9:
            return f"{int(v)} ₽"
        return f"{v:.2f} ₽"
    except:
        return "-"


# =========================
# SQLITE (dedupe)
# =========================

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


def get_cursor(name: str, default: str) -> str:
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


# =========================
# TELEGRAM
# =========================

def tg_send(text: str):
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": TG_CHAT_ID,
        "text": text[:3900],
        "disable_web_page_preview": True
    }, timeout=25)

def content_get_title(nm_id: int) -> str:
    if not WB_CONTENT_TOKEN or not nm_id:
        return ""

    url = f"{WB_CONTENT_BASE}/content/v2/get/cards/list"

    payload = {
        "settings": {
            "filter": {"textSearch": str(nm_id)},
            "cursor": {"limit": 1}
        }
    }

    headers = {"Authorization": WB_CONTENT_TOKEN}
    r = requests.post(url, headers=headers, json=payload, timeout=25)

    if r.status_code != 200:
        return ""

    data = r.json()
    cards = data.get("cards")
    if not cards:
        return ""

    return cards[0].get("title", "")
    
# =========================
# WB REQUEST
# =========================

def wb_get(url: str, token: str, params: Optional[dict] = None):
    headers = {"Authorization": token}
    r = requests.get(url, headers=headers, params=params, timeout=25)
    if r.status_code >= 400:
        return None
    try:
        return r.json()
    except:
        return None


# =========================
# FBW STOCKS CACHE
# =========================

_FBW_STOCKS_CACHE = (0.0, [])
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


def fbw_stock_quantity(warehouse: str, barcode: str) -> Optional[int]:
    rows = stats_fetch_fbw_stocks()

    warehouse = _safe_str(warehouse)
    barcode = _safe_str(barcode)

    for r in rows:
        if (
            _safe_str(r.get("warehouseName")) == warehouse and
            _safe_str(r.get("barcode")) == barcode
        ):
            try:
                return int(r.get("quantity", 0))
            except:
                return None
    return None


# =========================
# FBW ORDERS
# =========================

def msk_now():
    return datetime.now(timezone(timedelta(hours=3)))


def stats_fetch_orders_since() -> List[Dict[str, Any]]:
    if not WB_STATS_TOKEN:
        return []

    default_dt = msk_now() - timedelta(hours=2)
    cursor = get_cursor("stats_cursor", default_dt.isoformat())

    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/orders"
    data = wb_get(url, WB_STATS_TOKEN, params={"dateFrom": cursor})

    if not isinstance(data, list) or not data:
        return []

    last = data[-1]
    if last.get("lastChangeDate"):
        set_cursor("stats_cursor", last["lastChangeDate"])

    return data


def format_stats_order(o: Dict[str, Any]) -> str:
    warehouse = _safe_str(o.get("warehouseName"))
    nm_id = o.get("nmId")
    barcode = _safe_str(o.get("barcode"))

    product_name = _safe_str(
        o.get("nmName")
        or o.get("productName")
        or o.get("subjectName")
        or o.get("subject")
        or "Товар"
    )

    qty = int(o.get("quantity") or 1)
    
    price = (
        o.get("priceWithDisc")
        or o.get("finishedPrice")
        or o.get("forPay")
        or o.get("totalPrice")
        or o.get("price")
        or 0
    )

    stock_q = fbw_stock_quantity(warehouse, barcode)
    остаток_line = f"Остаток: {stock_q} шт" if isinstance(stock_q, int) else "Остаток: -"

    return (
        f"🏬 Заказ товара со склада ({warehouse}) · {SHOP_NAME}\n"
        f"📦 Склад отгрузки: {warehouse}\n"
        f"• {product_name}\n"
        f"  Артикул: {nm_id}\n"
        f"  — {qty} шт • цена покупателя - {_rub(price)}\n"
        f"{остаток_line}\n"
        f"Итого позиций: {qty}\n"
        f"Сумма: {_rub(price)}"
    )


async def poll_fbw_loop():
    while True:
        try:
            rows = stats_fetch_orders_since()
            for o in rows:
                if not o.get("srid"):
                    continue

                key = f"order:{o.get('srid')}:{o.get('lastChangeDate')}"
                if was_sent(key):
                    continue

                tg_send(format_stats_order(o))
                mark_sent(key)

        except Exception as e:
            tg_send(f"⚠️ Ошибка FBW polling: {e}")

        await asyncio.sleep(POLL_FBW_SECONDS)


# =========================
# ROUTES
# =========================

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/test-telegram")
def test_telegram():
    tg_send("✅ Тест из Render")
    return {"ok": True}


# =========================
# STARTUP
# =========================

@app.on_event("startup")
async def startup():
    _ = db()
    asyncio.create_task(poll_fbw_loop())
    tg_send("✅ WB→Telegram (FBW only) запущен.")
