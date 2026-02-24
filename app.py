# app.py
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

WB_MP_TOKEN = os.getenv("WB_MP_TOKEN", "").strip()               # marketplace-api (FBS/DBS/DBW)
WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()         # statistics-api (FBW)
WB_FEEDBACKS_TOKEN = os.getenv("WB_FEEDBACKS_TOKEN", "").strip() # feedbacks-api (reviews)
WB_WEBHOOK_SECRET = os.getenv("WB_WEBHOOK_SECRET", "").strip()

SHOP_NAME = os.getenv("SHOP_NAME", "Bright Shop").strip()

POLL_FBS_SECONDS = int(os.getenv("POLL_FBS_SECONDS", "20"))
POLL_FEEDBACKS_SECONDS = int(os.getenv("POLL_FEEDBACKS_SECONDS", "60"))
POLL_FBW_SECONDS = int(os.getenv("POLL_FBW_SECONDS", "1800"))

DAILY_SUMMARY_HOUR_MSK = int(os.getenv("DAILY_SUMMARY_HOUR_MSK", "23"))
DAILY_SUMMARY_MINUTE_MSK = int(os.getenv("DAILY_SUMMARY_MINUTE_MSK", "55"))

WB_MARKETPLACE_BASE = "https://marketplace-api.wildberries.ru"
WB_STATISTICS_BASE = "https://statistics-api.wildberries.ru"
WB_FEEDBACKS_BASE = "https://feedbacks-api.wildberries.ru"

DB_PATH = os.getenv("DB_PATH", "/var/data/wb_telegram.sqlite").strip()

# -------------------------
# Helpers: DB
# -------------------------
def _ensure_db_dir():
    try:
        d = os.path.dirname(DB_PATH)
        if d:
            os.makedirs(d, exist_ok=True)
    except Exception:
        pass

def db() -> sqlite3.Connection:
    _ensure_db_dir()
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
    conn.execute("INSERT OR REPLACE INTO sent_events(key, created_at) VALUES(?, ?)", (key, int(time.time())))
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
    payload = {"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True}
    r = requests.post(url, json=payload, timeout=20)
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

# -------------------------
# Utils
# -------------------------
def _safe_str(x) -> str:
    return "" if x is None else str(x).strip()

def _rub(x) -> str:
    try:
        v = float(x)
        return f"{int(v)} ₽" if abs(v - int(v)) < 1e-9 else f"{v:.2f} ₽"
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

def _stars(n: int) -> str:
    n = max(0, min(5, int(n)))
    return "★" * n + "☆" * (5 - n)

def _ru_stars_word(n: int) -> str:
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
# Feedbacks
# -------------------------
def feedbacks_fetch_latest() -> List[Dict[str, Any]]:
    if not WB_FEEDBACKS_TOKEN:
        return []
    url = f"{WB_FEEDBACKS_BASE}/api/v1/feedbacks"
    out: List[Dict[str, Any]] = []
    for is_answered in (False, True):
        data = wb_get(url, WB_FEEDBACKS_TOKEN, params={"isAnswered": str(is_answered).lower(), "take": 100, "skip": 0, "order": "dateDesc"})
        if isinstance(data, dict) and data.get("__error__"):
            continue
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            for x in data["data"].get("feedbacks", []) or []:
                if isinstance(x, dict):
                    out.append(x)
    return out

def format_feedback(f: Dict[str, Any]) -> str:
    rating = int(f.get("productValuation") or 0)
    mood = "Хороший отзыв" if rating >= 4 else "Плохой отзыв"
    shop_name = (f.get("supplierName") or SHOP_NAME).strip()
    product_name = (f.get("productName") or f.get("nmName") or "Без названия").strip()
    article = str(f.get("supplierArticle") or f.get("nmId") or "").strip()
    text = (f.get("text") or "").strip()
    text_line = f"Отзыв: {text}" if text else "Отзыв: (без текста, только оценка)"
    created = _format_dt_ru(f.get("createdDate") or "")
    stars = _stars(rating)
    stars_word = _ru_stars_word(rating)

    return (
        f"💬 Новый отзыв о товаре · ({shop_name})\n"
        f"Товар: {product_name} ({article})\n"
        f"Оценка: {stars} {rating} {stars_word} ({mood})\n"
        f"{text_line}\n"
        f"Дата: {created}"
    ).strip()

def prime_feedbacks_silently():
    try:
        for f in feedbacks_fetch_latest():
            fid = (f.get("id") or "").strip()
            if fid:
                mark_sent(f"feedback:{fid}")
    except Exception:
        pass

async def poll_feedbacks_loop():
    while True:
        try:
            for f in feedbacks_fetch_latest():
                fid = (f.get("id") or "").strip()
                if not fid:
                    continue
                key = f"feedback:{fid}"
                if was_sent(key):
                    continue
                res = tg_send(format_feedback(f))
                if res.get("ok"):
                    mark_sent(key)
        except Exception as e:
            print("feedbacks error:", e)
        await asyncio.sleep(POLL_FEEDBACKS_SECONDS)

# -------------------------
# Startup
# -------------------------
@app.on_event("startup")
async def startup():
    _ = db()
    prime_feedbacks_silently()
    asyncio.create_task(poll_feedbacks_loop())
    if not was_sent("hello:started"):
        tg_send("✅ WB→Telegram запущен. Жду заказы и отзывы.")
        mark_sent("hello:started")

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"ok": True}
