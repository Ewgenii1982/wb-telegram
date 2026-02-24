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

# =========================
# CONFIG (Render Env Vars)
# =========================
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

# WB tokens
WB_MP_TOKEN = os.getenv("WB_MP_TOKEN", "").strip()                     # marketplace-api (FBS/DBS/DBW)
WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()               # statistics-api (FBW)
WB_FEEDBACKS_TOKEN = os.getenv("WB_FEEDBACKS_TOKEN", "").strip()       # feedbacks-api (reviews)
WB_CONTENT_TOKEN = os.getenv("WB_CONTENT_TOKEN", "").strip()           # content-api (product titles)
WB_WEBHOOK_SECRET = os.getenv("WB_WEBHOOK_SECRET", "").strip()

SHOP_NAME = os.getenv("SHOP_NAME", "Bright Shop").strip()

# Poll intervals
POLL_FBS_SECONDS = int(os.getenv("POLL_FBS_SECONDS", "20"))            # marketplace orders
POLL_FEEDBACKS_SECONDS = int(os.getenv("POLL_FEEDBACKS_SECONDS", "60"))# reviews
POLL_FBW_SECONDS = int(os.getenv("POLL_FBW_SECONDS", "1800"))          # statistics orders (FBW) ~30min

# Daily summary time (MSK)
DAILY_SUMMARY_HOUR_MSK = int(os.getenv("DAILY_SUMMARY_HOUR_MSK", "23"))
DAILY_SUMMARY_MINUTE_MSK = int(os.getenv("DAILY_SUMMARY_MINUTE_MSK", "55"))

# WB base URLs
WB_MARKETPLACE_BASE = "https://marketplace-api.wildberries.ru"
WB_STATISTICS_BASE = "https://statistics-api.wildberries.ru"
WB_FEEDBACKS_BASE = "https://feedbacks-api.wildberries.ru"
WB_CONTENT_BASE = "https://content-api.wildberries.ru"

# DB:
# ВАЖНО: На бесплатном Render нет Persistent Disk => между рестартами SQLite не сохранится.
# Чтобы НЕ сыпались старые отзывы/заказы пачкой после рестарта, мы делаем "prime" на старте
# (помечаем текущие последние события как уже отправленные без отправки в TG).
DB_PATH = os.getenv("DB_PATH", "/tmp/wb_telegram.sqlite").strip()

# Content cache in memory (перезапуск очистит)
PRODUCT_NAME_CACHE: Dict[str, str] = {}


# =========================
# TIME HELPERS
# =========================
def msk_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=3)))

def iso_msk(dt: datetime) -> str:
    return dt.isoformat()

def format_dt_ru(iso: str) -> str:
    """ '2025-05-31T10:45:43Z' -> '31.05.2025 10:45' """
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.astimezone(timezone(timedelta(hours=3))).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return iso

def safe_str(x: Any) -> str:
    return "" if x is None else str(x).strip()

def rub(x: Any) -> str:
    try:
        v = float(x)
        if abs(v - int(v)) < 1e-9:
            return f"{int(v)} ₽"
        return f"{v:.2f} ₽"
    except Exception:
        return "-"

def stars_bar(rating: int) -> str:
    rating = max(0, min(5, int(rating)))
    return "★" * rating + "☆" * (5 - rating)

def ru_star_word(n: int) -> str:
    """1 звезда, 2-4 звезды, 5+ звёзд (и 11-14 -> звёзд)"""
    n = abs(int(n))
    if 11 <= (n % 100) <= 14:
        return "звёзд"
    last = n % 10
    if last == 1:
        return "звезда"
    if 2 <= last <= 4:
        return "звезды"
    return "звёзд"


# =========================
# DB (dedupe + cursors)
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
    row = conn.execute("SELECT 1 FROM sent_events WHERE key=?", (key,)).fetchone()
    conn.close()
    return row is not None

def mark_sent(key: str) -> None:
    conn = db()
    conn.execute("INSERT OR REPLACE INTO sent_events(key, created_at) VALUES(?,?)", (key, int(time.time())))
    conn.commit()
    conn.close()

def get_cursor(name: str, default: str = "") -> str:
    conn = db()
    row = conn.execute("SELECT value FROM cursors WHERE name=?", (name,)).fetchone()
    if row is None:
        conn.execute("INSERT OR REPLACE INTO cursors(name,value) VALUES(?,?)", (name, default))
        conn.commit()
        conn.close()
        return default
    conn.close()
    return row[0] or default

def set_cursor(name: str, value: str) -> None:
    conn = db()
    conn.execute("INSERT OR REPLACE INTO cursors(name,value) VALUES(?,?)", (name, value))
    conn.commit()
    conn.close()


# =========================
# TELEGRAM
# =========================
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


# =========================
# WB HTTP
# =========================
_SESSION = requests.Session()

def wb_get(url: str, token: str, params: Optional[dict] = None, timeout: int = 25) -> Any:
    headers = {"Authorization": token}
    try:
        r = _SESSION.get(url, headers=headers, params=params, timeout=timeout)
    except requests.exceptions.SSLError as e:
        return {"__error__": True, "status_code": 0, "url": url, "response_text": f"SSLError: {e}"}
    except requests.exceptions.RequestException as e:
        return {"__error__": True, "status_code": 0, "url": url, "response_text": f"RequestException: {e}"}

    if r.status_code >= 400:
        return {"__error__": True, "status_code": r.status_code, "url": r.url, "response_text": r.text}
    try:
        return r.json()
    except Exception:
        return r.text

def wb_post(url: str, token: str, params: Optional[dict] = None, json_body: Optional[dict] = None, timeout: int = 25) -> Any:
    headers = {"Authorization": token}
    try:
        r = _SESSION.post(url, headers=headers, params=params, json=json_body, timeout=timeout)
    except requests.exceptions.SSLError as e:
        return {"__error__": True, "status_code": 0, "url": url, "response_text": f"SSLError: {e}"}
    except requests.exceptions.RequestException as e:
        return {"__error__": True, "status_code": 0, "url": url, "response_text": f"RequestException: {e}"}

    if r.status_code >= 400:
        return {"__error__": True, "status_code": r.status_code, "url": r.url, "response_text": r.text}
    try:
        return r.json()
    except Exception:
        return r.text


# =========================
# CONTENT API: nmId -> title
# =========================
def wb_get_product_title_by_nmid(nm_id: Any) -> str:
    nm_id = safe_str(nm_id)
    if not nm_id or not WB_CONTENT_TOKEN:
        return ""

    if nm_id in PRODUCT_NAME_CACHE:
        return PRODUCT_NAME_CACHE[nm_id]

    url = f"{WB_CONTENT_BASE}/content/v2/get/cards/list"
    body = {
        "settings": {
            "sort": {"ascending": False},
            "filter": {"textSearch": nm_id, "withPhoto": -1},
            "cursor": {"limit": 20}
        }
    }
    data = wb_post(url, WB_CONTENT_TOKEN, params={"locale": "ru"}, json_body=body)
    if isinstance(data, dict) and data.get("__error__"):
        return ""

    cards = data.get("cards") if isinstance(data, dict) else None
    if isinstance(cards, list):
        # точное совпадение nmID
        for c in cards:
            if isinstance(c, dict) and safe_str(c.get("nmID")) == nm_id and c.get("title"):
                title = safe_str(c.get("title"))
                if title:
                    PRODUCT_NAME_CACHE[nm_id] = title
                    return title
        # fallback
        if cards and isinstance(cards[0], dict) and cards[0].get("title"):
            title = safe_str(cards[0].get("title"))
            if title:
                PRODUCT_NAME_CACHE[nm_id] = title
                return title

    return ""


# =========================
# MARKETPLACE: FBS/DBS/DBW new orders
# =========================
def mp_fetch_new_orders() -> List[Tuple[str, Dict[str, Any]]]:
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

        # 404/403/401/SSL бывают — просто пропускаем, не падаем и не спамим
        if isinstance(data, dict) and data.get("__error__"):
            continue

        orders: List[Any] = []
        if isinstance(data, dict) and isinstance(data.get("orders"), list):
            orders = data["orders"]
        elif isinstance(data, list):
            orders = data
        else:
            continue

        for o in orders:
            if not isinstance(o, dict):
                continue
            oid = safe_str(o.get("id") or o.get("orderId") or o.get("rid") or o.get("srid") or "")
            if not oid:
                oid = str(abs(hash(json.dumps(o, ensure_ascii=False, sort_keys=True))))
            found.append((kind, {"_id": oid, **o}))

    return found

def format_mp_order(kind: str, o: Dict[str, Any]) -> str:
    # В marketplace иногда есть только артикул/склад, без красивого title
    warehouse = safe_str(o.get("warehouseName") or o.get("warehouse") or "")
    address = warehouse or "—"

    nm_id = safe_str(o.get("nmId") or o.get("chrtId") or "")
    title = wb_get_product_title_by_nmid(nm_id) if nm_id else ""
    supplier_article = safe_str(o.get("supplierArticle") or o.get("article") or o.get("vendorCode") or "")
    product_name = title or supplier_article or "Товар"

    qty = 1
    price = o.get("price") or o.get("totalPrice") or o.get("priceWithDisc") or o.get("finishedPrice") or 0

    return (
        f"🏬 Заказ товара со склада ({warehouse or 'Маркетплейс'}) · {SHOP_NAME}\n"
        f"📦 Склад отгрузки: {address}\n"
        f"• {product_name} ({supplier_article or nm_id or '—'})\n"
        f"  — {qty} шт • цена - {rub(price)}\n"
        f"Итого позиций: 1\n"
        f"Сумма: {rub(price)}"
    ).strip()

async def poll_marketplace_loop():
    while True:
        try:
            orders = mp_fetch_new_orders()
            for kind, o in orders:
                key = f"mp:{kind}:{safe_str(o.get('_id'))}"
                if not key.endswith(":") and was_sent(key):
                    continue
                res = tg_send(format_mp_order(kind, o))
                if res.get("ok"):
                    mark_sent(key)
        except Exception as e:
            ek = f"err:mp:{type(e).__name__}:{str(e)[:120]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка marketplace polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_FBS_SECONDS)


# =========================
# STATISTICS: FBW orders (supplier/orders)
# =========================
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
        return []

    # обновляем курсор по последней строке
    last = data[-1]
    if isinstance(last, dict) and last.get("lastChangeDate"):
        set_cursor(cursor_name, last["lastChangeDate"])

    return data

def format_stats_order(o: Dict[str, Any]) -> str:
    warehouse = safe_str(o.get("warehouseName") or o.get("officeName") or "WB")
    address = warehouse

    nm_id = safe_str(o.get("nmId") or "")
    title = wb_get_product_title_by_nmid(nm_id) if nm_id else ""

    # В statistics обычно есть supplierArticle (короткий). Берем его в скобки.
    supplier_article = safe_str(o.get("supplierArticle") or o.get("vendorCode") or o.get("article") or "")
    product_name = title or safe_str(o.get("nmName") or o.get("productName") or o.get("subject") or "Товар")

    qty = o.get("quantity") or 1
    try:
        qty = int(qty)
    except Exception:
        qty = 1

    price = (
        o.get("finishedPrice")
        or o.get("priceWithDisc")
        or o.get("totalPrice")
        or o.get("forPay")
        or o.get("price")
        or 0
    )

    is_cancel = o.get("isCancel", False)
    cancel_txt = ""
    if str(is_cancel).lower() in ("1", "true", "yes"):
        cancel_txt = " ❌ ОТМЕНА"

    return (
        f"🏬 Заказ товара со склада ({warehouse}) · {SHOP_NAME}{cancel_txt}\n"
        f"📦 Склад отгрузки: {address}\n"
        f"• {product_name} ({supplier_article or nm_id or '—'})\n"
        f"  — {qty} шт • цена - {rub(price)}\n"
        f"Итого позиций: 1\n"
        f"Сумма: {rub(price)}"
    ).strip()

async def poll_fbw_loop():
    while True:
        try:
            rows = stats_fetch_orders_since("stats_orders_cursor")

            if rows and isinstance(rows[0], dict) and rows[0].get("__error__"):
                # не спамим одинаковой ошибкой
                ek = f"err:stats_orders:{rows[0].get('status_code')}:{safe_str(rows[0].get('url'))}"
                if not was_sent(ek):
                    tg_send(f"⚠️ statistics orders error: {rows[0].get('status_code')} {safe_str(rows[0].get('response_text'))[:300]}")
                    mark_sent(ek)
            else:
                for o in rows:
                    if not isinstance(o, dict):
                        continue
                    srid = safe_str(o.get("srid"))
                    lcd = safe_str(o.get("lastChangeDate"))
                    if not srid:
                        continue
                    key = f"stats:order:{srid}:{lcd}"
                    if was_sent(key):
                        continue
                    res = tg_send(format_stats_order(o))
                    if res.get("ok"):
                        mark_sent(key)

        except Exception as e:
            ek = f"err:stats:{type(e).__name__}:{str(e)[:120]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка statistics polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_FBW_SECONDS)


# =========================
# FEEDBACKS (reviews)
# =========================
def feedbacks_fetch_latest() -> List[Dict[str, Any]]:
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
            out.append({"__error__": True, "__stage__": f"isAnswered={is_answered}", **data})
            continue

        if isinstance(data, dict) and isinstance(data.get("data"), dict):
            fb = data["data"].get("feedbacks", [])
            if isinstance(fb, list):
                for x in fb:
                    if isinstance(x, dict):
                        out.append(x)

    return out

def format_feedback(f: Dict[str, Any]) -> str:
    rating_raw = f.get("productValuation")
    try:
        rating = int(rating_raw) if rating_raw is not None else 0
    except Exception:
        rating = 0

    mood = "Хороший отзыв" if rating >= 4 else "Плохой отзыв"

    # магазин — всегда Bright Shop (или SHOP_NAME), чтобы не было "Ваш магазин"
    shop = SHOP_NAME

    # product_name: иногда в feedbacks бывает, но часто нормальный title лучше тянуть из Content по nmId
    nm_id = safe_str(f.get("nmId") or "")
    title = wb_get_product_title_by_nmid(nm_id) if nm_id else ""

    product_name = safe_str(f.get("productName") or f.get("nmName") or f.get("subjectName") or "")
    if title:
        product_name = title
    if not product_name:
        product_name = "Товар"

    article = safe_str(
        f.get("supplierArticle")
        or f.get("vendorCode")
        or f.get("article")
        or nm_id
        or ""
    )

    text = safe_str(f.get("text") or "")
    if not text:
        text_line = "Отзыв: (без текста, только оценка)"
    else:
        text_line = f"Отзыв: {text}"

    created = format_dt_ru(safe_str(f.get("createdDate") or ""))

    return (
        f"💬 Новый отзыв о товаре · ({shop})\n"
        f"Товар: {product_name} ({article or '—'})\n"
        f"Оценка: {stars_bar(rating)} {rating} {ru_star_word(rating)} ({mood})\n"
        f"{text_line}\n"
        f"Дата: {created}"
    ).strip()

def prime_feedbacks_silently() -> None:
    """
    На бесплатном Render база в /tmp теряется на рестарте.
    Чтобы после рестарта НЕ прилетала пачка старых отзывов:
    - читаем последние отзывы
    - помечаем их как уже отправленные (НЕ отправляя в TG)
    """
    try:
        items = feedbacks_fetch_latest()
        for it in items:
            if isinstance(it, dict) and it.get("__error__"):
                return
        for f in items:
            if not isinstance(f, dict):
                continue
            fid = safe_str(f.get("id"))
            if not fid:
                continue
            key = f"feedback:{fid}"
            if not was_sent(key):
                mark_sent(key)
        print("[prime_feedbacks_silently] done")
    except Exception as e:
        print(f"[prime_feedbacks_silently] error: {e}")

def prime_orders_silently() -> None:
    """
    Аналогично: чтобы после рестарта не переслались старые заказы пачкой,
    помечаем текущую "новую" выдачу marketplace и текущую статистику как уже отправленные.
    """
    try:
        # marketplace
        mp = mp_fetch_new_orders()
        for kind, o in mp:
            key = f"mp:{kind}:{safe_str(o.get('_id'))}"
            if not was_sent(key):
                mark_sent(key)

        # stats: только последние 2 часа курсор выставляем на "сейчас"
        # чтобы не шла история после рестарта
        set_cursor("stats_orders_cursor", iso_msk(msk_now()))

        print("[prime_orders_silently] done")
    except Exception as e:
        print(f"[prime_orders_silently] error: {e}")

async def poll_feedbacks_loop():
    while True:
        try:
            items = feedbacks_fetch_latest()

            # ошибки — не спамим постоянно
            for it in items:
                if isinstance(it, dict) and it.get("__error__"):
                    ek = f"err:feedbacks:{it.get('status_code')}:{safe_str(it.get('__stage__'))}"
                    if not was_sent(ek):
                        tg_send(f"⚠️ feedbacks error: {it.get('status_code')} {safe_str(it.get('response_text'))[:300]}")
                        mark_sent(ek)

            for f in items:
                if not isinstance(f, dict) or f.get("__error__"):
                    continue
                fid = safe_str(f.get("id"))
                if not fid:
                    continue
                key = f"feedback:{fid}"
                if was_sent(key):
                    continue
                res = tg_send(format_feedback(f))
                if res.get("ok"):
                    mark_sent(key)

        except Exception as e:
            ek = f"err:feedbacks:{type(e).__name__}:{str(e)[:120]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка feedbacks polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_FEEDBACKS_SECONDS)


# =========================
# DAILY SUMMARY (sales)
# =========================
def daily_summary_text(day_msk: datetime) -> str:
    if not WB_STATS_TOKEN:
        return f"⚠️ Суточная сводка: нет WB_STATS_TOKEN · {SHOP_NAME}"

    day_str = day_msk.strftime("%Y-%m-%d")
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/sales"
    data = wb_get(url, WB_STATS_TOKEN, params={"dateFrom": day_str, "flag": 1})

    if isinstance(data, dict) and data.get("__error__"):
        return f"⚠️ Суточная сводка: ошибка statistics sales {data.get('status_code')} · {SHOP_NAME}"

    if not isinstance(data, list):
        return f"⚠️ Суточная сводка: нет данных · {SHOP_NAME}"

    sold_cnt = 0
    sold_sum = 0.0
    returns_cnt = 0
    returns_sum = 0.0

    for row in data:
        if not isinstance(row, dict):
            continue
        price = row.get("forPay") or row.get("priceWithDisc") or row.get("finishedPrice") or 0
        try:
            price_f = float(price)
        except Exception:
            price_f = 0.0

        # простая эвристика
        if row.get("saleID") is not None and price_f >= 0:
            sold_cnt += 1
            sold_sum += price_f
        else:
            returns_cnt += 1
            returns_sum += abs(price_f)

    # отзывы за день можно считать по факту отправленных, но на free Render нет persistence.
    # Поэтому пишем честно: "см. уведомления".
    return (
        f"📊 Суточная сводка за {day_str} (МСК) · {SHOP_NAME}\n"
        f"Продано позиций: {sold_cnt}\n"
        f"Сумма продаж/выкупа: {sold_sum:.2f} ₽\n"
        f"Отказы/возвраты позиций: {returns_cnt}\n"
        f"Сумма отказов/возвратов: {returns_sum:.2f} ₽\n"
        f"Отзывы: см. уведомления (если были — ты их получил)"
    ).strip()

async def daily_summary_loop():
    while True:
        try:
            now = msk_now()
            target = now.replace(hour=DAILY_SUMMARY_HOUR_MSK, minute=DAILY_SUMMARY_MINUTE_MSK, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)

            await asyncio.sleep((target - now).total_seconds())

            # на free Render это тоже сбросится, но хотя бы в рамках аптайма не продублируем
            key = f"daily:{target.strftime('%Y-%m-%d')}"
            if not was_sent(key):
                tg_send(daily_summary_text(target))
                mark_sent(key)

        except Exception as e:
            ek = f"err:daily:{type(e).__name__}:{str(e)[:120]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка суточной сводки: {e}")
                mark_sent(ek)


# =========================
# WB WEBHOOK (optional)
# =========================
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


# =========================
# ENDPOINTS
# =========================
@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/test-telegram")
def test_telegram():
    return {"telegram_result": tg_send("✅ Тест: сообщение из облачного сервера Render")}

@app.get("/poll-once")
def poll_once():
    """
    Ручной запуск одного шага — удобно тестировать.
    """
    result: Dict[str, Any] = {}

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
            fb = feedbacks_fetch_latest()
            err = [x for x in fb if isinstance(x, dict) and x.get("__error__")]
            result["feedbacks_errors"] = err[:1] if err else []
            result["feedbacks_found"] = len([x for x in fb if isinstance(x, dict) and x.get("id")])
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

    # content token status
    result["content_token"] = "ok" if WB_CONTENT_TOKEN else "no WB_CONTENT_TOKEN"
    return result


# =========================
# STARTUP
# =========================
@app.on_event("startup")
async def startup():
    _ = db()

    # На бесплатном Render после деплоя/рестарта БД в /tmp пустая,
    # поэтому "проглатываем" историю, чтобы НЕ сыпалась пачка отзывов/заказов.
    prime_feedbacks_silently()
    prime_orders_silently()

    # Запускаем фоновые задачи
    asyncio.create_task(poll_marketplace_loop())
    asyncio.create_task(poll_feedbacks_loop())
    asyncio.create_task(poll_fbw_loop())
    asyncio.create_task(daily_summary_loop())

    # Приветствие НЕ шлём каждый раз (на free оно всё равно может повториться после рестарта),
    # поэтому выключаем по умолчанию. Если хочешь — раскомментируй.
    # hello_key = "hello:started"
    # if not was_sent(hello_key):
    #     tg_send(f"✅ WB→Telegram запущен · {SHOP_NAME}. Жду заказы (FBS/DBS/DBW), FBW (~30 мин) и отзывы.")
    #     mark_sent(hello_key)
