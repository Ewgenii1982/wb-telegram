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

POLL_FBS_SECONDS = int(os.getenv("POLL_FBS_SECONDS", "20"))
POLL_FEEDBACKS_SECONDS = int(os.getenv("POLL_FEEDBACKS_SECONDS", "60"))
POLL_FBW_SECONDS = int(os.getenv("POLL_FBW_SECONDS", "1800"))

DAILY_SUMMARY_HOUR_MSK = int(os.getenv("DAILY_SUMMARY_HOUR_MSK", "23"))
DAILY_SUMMARY_MINUTE_MSK = int(os.getenv("DAILY_SUMMARY_MINUTE_MSK", "55"))

# WB base URLs
WB_MARKETPLACE_BASE = "https://marketplace-api.wildberries.ru"
WB_STATISTICS_BASE = "https://statistics-api.wildberries.ru"
WB_FEEDBACKS_BASE = "https://feedbacks-api.wildberries.ru"

# SQLite for dedupe + cursors
DB_PATH = "/tmp/wb_telegram.sqlite"


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
# Helpers: Telegram
# -------------------------
def tg_send(text: str) -> Dict[str, Any]:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return {"ok": False, "error": "No TG_BOT_TOKEN or TG_CHAT_ID"}

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
        return {"ok": False, "status": r.status_code, "text": r.text}


# -------------------------
# Helpers: WB requests
# -------------------------
def wb_get(url: str, token: str, params: Optional[dict] = None, timeout: int = 25) -> Any:
    headers = {"Authorization": token}
    r = requests.get(url, headers=headers, params=params, timeout=timeout)
    if r.status_code >= 400:
        # Return structured error to show in /poll-once
        return {
            "__error__": True,
            "status_code": r.status_code,
            "url": r.url,
            "response_text": r.text
        }
    try:
        return r.json()
    except Exception:
        return r.text


# -------------------------
# FBS/DBS/DBW: Marketplace (near real-time)
# -------------------------
def mp_fetch_new_orders() -> List[Tuple[str, Dict[str, Any]]]:
    """
    Tries multiple 'new orders' endpoints.
    We dedupe by a stable id (orderId / id / etc) + endpoint name.
    """
    if not WB_MP_TOKEN:
        return []

    endpoints = [
        ("FBS", f"{WB_MARKETPLACE_BASE}/api/v3/orders/new"),
        ("DBS", f"{WB_MARKETPLACE_BASE}/api/v3/dbs/orders/new"),
        ("DBW", f"{WB_MARKETPLACE_BASE}/api/v3/dbw/orders/new"),
    ]

    found: List[Tuple[str, Dict[str, Any]]] = []

    for kind, url in endpoints:
        data = wb_get(url, WB_MP_TOKEN)
        if isinstance(data, dict) and data.get("__error__"):
            # If endpoint not enabled for account it can be 404/403 — ignore quietly
            continue

        # WB may return {"orders":[...]} or list [...]
        orders = []
        if isinstance(data, dict) and "orders" in data and isinstance(data["orders"], list):
            orders = data["orders"]
        elif isinstance(data, list):
            orders = data
        else:
            continue

        for o in orders:
            if not isinstance(o, dict):
                continue
            # pick any id-like field
            oid = (
                str(o.get("id") or o.get("orderId") or o.get("rid") or o.get("srid") or "")
            ).strip()
            if not oid:
                # fallback: hash the json (last resort)
                oid = str(abs(hash(json.dumps(o, ensure_ascii=False, sort_keys=True))))
            found.append((kind, {"_id": oid, **o}))

    return found


def format_mp_order(kind: str, o: Dict[str, Any]) -> str:
    oid = o.get("_id", "")
    article = o.get("article") or o.get("supplierArticle") or o.get("vendorCode") or ""
    nm_id = o.get("nmId") or o.get("chrtId") or ""
    created = o.get("createdAt") or o.get("createdDate") or o.get("dateCreated") or ""
    warehouse = o.get("warehouseName") or o.get("warehouse") or ""

    return (
        f"🆕 Новый заказ ({kind})\n"
        f"ID: {oid}\n"
        f"Артикул: {article}\n"
        f"nmId/chrtId: {nm_id}\n"
        f"Склад: {warehouse}\n"
        f"Дата: {created}\n"
    ).strip()


async def poll_marketplace_loop():
    # no spam "started" messages anymore
    while True:
        try:
            orders = mp_fetch_new_orders()
            sent = 0
            for kind, o in orders:
                key = f"mp:{kind}:{o.get('_id','')}"
                if was_sent(key):
                    continue
                text = format_mp_order(kind, o)
                res = tg_send(text)
                if res.get("ok"):
                    mark_sent(key)
                    sent += 1

            # silent if nothing new
        except Exception as e:
            # send only if really needed (rare), but still dedupe errors
            ek = f"err:mp:{type(e).__name__}:{str(e)[:80]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка marketplace polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_FBS_SECONDS)


# -------------------------
# FBW: Statistics (updates ~ every 30 minutes)
# -------------------------
def msk_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))

def iso_msk(dt: datetime) -> str:
    # RFC3339 with +03:00
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

    if not isinstance(data, list) or len(data) == 0:
        # ✅ нет новых данных — это нормально
        return []

    # ✅ обновляем курсор только если есть хотя бы 1 строка
    last = data[-1]
    if isinstance(last, dict) and last.get("lastChangeDate"):
        set_cursor(cursor_name, last["lastChangeDate"])

    return data

def stats_fetch_sales_since(cursor_name: str) -> List[Dict[str, Any]]:
    """
    statistics-api sales/returns updated ~ every 30 minutes. :contentReference[oaicite:4]{index=4}
    """
    if not WB_STATS_TOKEN:
        return []

    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/sales"
    default_dt = msk_now() - timedelta(hours=2)
    cursor = get_cursor(cursor_name, iso_msk(default_dt))
    data = wb_get(url, WB_STATS_TOKEN, params={"dateFrom": cursor})
    if isinstance(data, dict) and data.get("__error__"):
        return [{"__error__": True, **data}]

    if not isinstance(data, list):
        return []
    len(data) == 0
    last = data[-1]
    if isinstance(last, dict) and last.get("lastChangeDate"):
        set_cursor(cursor_name, last["lastChangeDate"])

    return data

def format_stats_order(o: Dict[str, Any]) -> str:
    # 1 row = 1 item in order (as docs mention). :contentReference[oaicite:5]{index=5}
    srid = o.get("srid", "")
    nm_id = o.get("nmId", "")
    article = o.get("supplierArticle", "")
    wh = o.get("warehouseName", "")
    price = o.get("finishedPrice") or o.get("priceWithDisc") or o.get("totalPrice") or 0
    is_cancel = o.get("isCancel", False)

    return (
        f"📦 Заказ/движение (FBW/Statistics)\n"
        f"srid: {srid}\n"
        f"Артикул: {article}\n"
        f"nmId: {nm_id}\n"
        f"Склад: {wh}\n"
        f"Цена: {price}\n"
        f"Отмена: {'да' if is_cancel else 'нет'}"
    ).strip()

async def poll_fbw_loop():
    while True:
        try:
            rows = stats_fetch_orders_since("stats_orders_cursor")
            # error row
            if rows and isinstance(rows[0], dict) and rows[0].get("__error__"):
                ek = f"err:stats_orders:{rows[0].get('status_code')}:{rows[0].get('url','')}"
                if not was_sent(ek):
                    tg_send(f"⚠️ statistics orders error: {rows[0].get('status_code')} {rows[0].get('response_text','')[:300]}")
                    mark_sent(ek)
            else:
                sent = 0
                for o in rows:
                    if not isinstance(o, dict):
                        continue
                    key = f"stats:order:{o.get('srid','')}:{o.get('lastChangeDate','')}"
                    if not o.get("srid"):
                        continue
                    if was_sent(key):
                        continue
                    res = tg_send(format_stats_order(o))
                    if res.get("ok"):
                        mark_sent(key)
                        sent += 1
        except Exception as e:
            ek = f"err:stats:{type(e).__name__}:{str(e)[:80]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка statistics polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_FBW_SECONDS)


# -------------------------
# Feedbacks (reviews)
# -------------------------
def feedbacks_fetch_latest() -> List[Dict[str, Any]]:
    """
    GET /api/v1/feedbacks requires: isAnswered, take, skip. :contentReference[oaicite:6]{index=6}
    We'll load latest (answered + unanswered) by pages:
      - first take=100, skip=0, isAnswered=true
      - plus unanswered isAnswered=false
    And dedupe by feedback id.
    """
    if not WB_FEEDBACKS_TOKEN:
        return []

    url = f"{WB_FEEDBACKS_BASE}/api/v1/feedbacks"
    out: List[Dict[str, Any]] = []

    for is_answered in (False, True):
        data = wb_get(
            url,
            WB_FEEDBACKS_TOKEN,
            params={
                "isAnswered": str(is_answered).lower(),
                "take": 100,
                "skip": 0,
                "order": "dateDesc",
            },
        )
        if isinstance(data, dict) and data.get("__error__"):
            out.append({"__error__": True, **data, "__stage__": f"feedbacks isAnswered={is_answered}"})
            continue

        # expected structure: {"data":{"feedbacks":[...]...}, "error":false...} (per samples) :contentReference[oaicite:7]{index=7}
        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            fb = data["data"].get("feedbacks", [])
            if isinstance(fb, list):
                for x in fb:
                    if isinstance(x, dict):
                        out.append(x)

    return out

def format_feedback(f: Dict[str, Any]) -> str:
    fid = f.get("id", "")
    rating = f.get("productValuation", None)  # field shown in docs sample :contentReference[oaicite:8]{index=8}
    text = (f.get("text") or "").strip()
    pros = (f.get("pros") or "").strip()
    cons = (f.get("cons") or "").strip()
    created = f.get("createdDate") or ""

    label = "📝 Отзыв"
    if isinstance(rating, int):
        if rating >= 4:
            label = "✅ Хороший отзыв"
        else:
            label = "⚠️ Плохой отзыв"

    msg = f"{label}\n⭐ {rating}\nID: {fid}\nДата: {created}"
    if text:
        msg += f"\n\nТекст: {text}"
    if pros:
        msg += f"\n\nПлюсы: {pros}"
    if cons:
        msg += f"\n\nМинусы: {cons}"
    return msg.strip()

async def poll_feedbacks_loop():
    while True:
        try:
            items = feedbacks_fetch_latest()

            # handle errors (once)
            for it in items:
                if isinstance(it, dict) and it.get("__error__"):
                    ek = f"err:feedbacks:{it.get('status_code')}:{it.get('__stage__','')}"
                    if not was_sent(ek):
                        tg_send(f"⚠️ feedbacks error: {it.get('status_code')} {it.get('response_text','')[:300]}")
                        mark_sent(ek)
                    continue

            sent = 0
            for f in items:
                if not isinstance(f, dict) or f.get("__error__"):
                    continue
                fid = (f.get("id") or "").strip()
                if not fid:
                    continue
                key = f"feedback:{fid}"
                if was_sent(key):
                    continue
                res = tg_send(format_feedback(f))
                if res.get("ok"):
                    mark_sent(key)
                    sent += 1

        except Exception as e:
            ek = f"err:feedbacks:{type(e).__name__}:{str(e)[:80]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка feedbacks polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_FEEDBACKS_SECONDS)


# -------------------------
# Daily summary (sales + returns)
# -------------------------
def daily_summary_text(today: datetime) -> str:
    """
    Builds daily summary from statistics sales.
    We'll fetch since start of day (MSK), flag=1 by docs (date only matters). :contentReference[oaicite:9]{index=9}
    """
    if not WB_STATS_TOKEN:
        return "⚠️ Суточная сводка: нет WB_STATS_TOKEN"

    day_str = today.strftime("%Y-%m-%d")  # MSK date
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/sales"
    data = wb_get(url, WB_STATS_TOKEN, params={"dateFrom": day_str, "flag": 1})
    if isinstance(data, dict) and data.get("__error__"):
        return f"⚠️ Суточная сводка: ошибка statistics sales {data.get('status_code')}"

    if not isinstance(data, list):
        return "⚠️ Суточная сводка: нет данных"

    sold_sum = 0.0
    buyout_sum = 0.0
    returns_sum = 0.0
    sold_cnt = 0
    returns_cnt = 0

    for row in data:
        if not isinstance(row, dict):
            continue
        # heuristics: sale vs return often is indicated by "saleID" and "return" fields,
        # but to stay robust we use "quantity" sign if exists
        price = row.get("forPay") or row.get("priceWithDisc") or row.get("finishedPrice") or 0
        try:
            price = float(price)
        except Exception:
            price = 0.0

        # Some WB rows have "saleID" and "isCancel" etc; simplest: if "saleID" exists -> sale
        # If you want stricter later, we can refine.
        if row.get("saleID") is not None and price >= 0:
            sold_cnt += 1
            sold_sum += price
            buyout_sum += price
        else:
            # treat as return
            returns_cnt += 1
            returns_sum += abs(price)

    # reviews count for day: we won't call feedbacks here to avoid rate issues; just says "смотри ленту"
    return (
        f"📊 Суточная сводка за {day_str} (МСК)\n"
        f"Продажи (строк): {sold_cnt}\n"
        f"Сумма продаж/выкупа (примерно): {buyout_sum:.2f}\n"
        f"Возвраты/отказы (строк): {returns_cnt}\n"
        f"Сумма возвратов (примерно): {returns_sum:.2f}\n"
        f"Отзывы: см. уведомления (если были — ты их получил)"
    ).strip()

async def daily_summary_loop():
    while True:
        try:
            now = msk_now()
            target = now.replace(hour=DAILY_SUMMARY_HOUR_MSK, minute=DAILY_SUMMARY_MINUTE_MSK, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)

            # sleep until target
            await asyncio.sleep((target - now).total_seconds())

            # avoid duplicate per day
            day_key = f"daily:{target.strftime('%Y-%m-%d')}"
            if not was_sent(day_key):
                tg_send(daily_summary_text(target))
                mark_sent(day_key)

        except Exception as e:
            ek = f"err:daily:{type(e).__name__}:{str(e)[:80]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка суточной сводки: {e}")
                mark_sent(ek)


# -------------------------
# Optional: WB webhook receiver (if later you подключишь)
# -------------------------
@app.post("/wb-webhook/{secret}")
async def wb_webhook(secret: str, request: Request):
    if not WB_WEBHOOK_SECRET or secret != WB_WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="forbidden")

    payload = await request.json()
    # Dedupe by payload hash
    key = f"webhook:{abs(hash(json.dumps(payload, ensure_ascii=False, sort_keys=True)))}"
    if was_sent(key):
        return {"ok": True, "dedup": True}

    tg_send("📩 WB webhook событие\n" + json.dumps(payload, ensure_ascii=False)[:3500])
    mark_sent(key)
    return {"ok": True}


# -------------------------
# Manual test endpoints
# -------------------------
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/test-telegram")
def test_telegram():
    return {"telegram_result": tg_send("✅ Тест: сообщение из облачного сервера Render")}

@app.get("/poll-once")
def poll_once():
    """
    Manually run one polling step to debug quickly.
    """
    result = {}

    # marketplace
    if WB_MP_TOKEN:
        try:
            orders = mp_fetch_new_orders()
            result["marketplace_found"] = len(orders)
        except Exception as e:
            result["marketplace_error"] = str(e)
    else:
        result["marketplace"] = "no WB_MP_TOKEN"

    # feedbacks
    if WB_FEEDBACKS_TOKEN:
        try:
            f = feedbacks_fetch_latest()
            err = [x for x in f if isinstance(x, dict) and x.get("__error__")]
            result["feedbacks_errors"] = err[:1] if err else []
            result["feedbacks_found"] = len([x for x in f if isinstance(x, dict) and x.get("id")])
        except Exception as e:
            result["feedbacks_error"] = str(e)
    else:
        result["feedbacks"] = "no WB_FEEDBACKS_TOKEN"

    # statistics
    if WB_STATS_TOKEN:
        try:
            rows = stats_fetch_orders_since("stats_orders_cursor")
            if rows and isinstance(rows[0], dict) and rows[0].get("__error__"):
                result["stats_orders_error"] = rows[0]
            else:
                result["stats_orders_rows"] = len(rows)
        except Exception as e:
            result["stats_orders_error"] = str(e)
    else:
        result["stats"] = "no WB_STATS_TOKEN"

    return result


# -------------------------
# Startup: background tasks
# -------------------------
@app.on_event("startup")
async def startup():
    # init db
    _ = db()
    # start tasks
    asyncio.create_task(poll_marketplace_loop())
    asyncio.create_task(poll_feedbacks_loop())
    asyncio.create_task(poll_fbw_loop())
    asyncio.create_task(daily_summary_loop())

    # one-time hello (ONLY ONCE)
    hello_key = "hello:started"
    if not was_sent(hello_key):
        tg_send("✅ WB→Telegram запущен. Жду заказы (FBS/DBS/DBW), FBW (с задержкой ~30 мин) и отзывы.")
        mark_sent(hello_key)
