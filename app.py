"""WB → Telegram notifier (Render-safe)

Key goals for Render Web Service:
- startup MUST finish fast so uvicorn opens the port (Render port scan).
- absolutely no blocking network or sqlite work inside startup.

This file:
- runs all network/db warmups in background tasks (threads) after startup.
- exposes /health for quick checks.
- uses conservative polling intervals + per-host cooldown/backoff to avoid 429/502 storms.

Env vars (names preserved from earlier versions):
- TG_BOT_TOKEN, TG_CHAT_ID
- WB_STATS_TOKEN, WB_FEEDBACKS_TOKEN, WB_MP_TOKEN, WB_CONTENT_TOKEN
- DB_PATH (default: /tmp/wb_bot.sqlite)
- DISABLE_STARTUP_HELLO (1 to disable)
- POLL_*_SECONDS (optional):
  POLL_SALES_SECONDS, POLL_FBW_SECONDS, POLL_FEEDBACKS_SECONDS, POLL_QUESTIONS_SECONDS, POLL_FBS_SECONDS
- DAILY_SUMMARY_HOUR_MSK, DAILY_SUMMARY_MINUTE_MSK

Note: For FBW/FBO (stock on WB), stocks should come from statistics supplier/stocks.
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from fastapi import FastAPI
from fastapi.responses import JSONResponse

# ---------------------------
# Config
# ---------------------------

TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()
WB_FEEDBACKS_TOKEN = os.getenv("WB_FEEDBACKS_TOKEN", "").strip()
WB_MP_TOKEN = os.getenv("WB_MP_TOKEN", "").strip()
WB_CONTENT_TOKEN = os.getenv("WB_CONTENT_TOKEN", "").strip()

DB_PATH = os.getenv("DB_PATH", "/tmp/wb_bot.sqlite").strip() or "/tmp/wb_bot.sqlite"

DISABLE_STARTUP_HELLO = os.getenv("DISABLE_STARTUP_HELLO", "0").strip() in {"1", "true", "True", "yes", "YES"}

# Conservative defaults (to reduce 429/502). You can override in Render env.
POLL_SALES_SECONDS = int(os.getenv("POLL_SALES_SECONDS", "600"))
POLL_FBW_SECONDS = int(os.getenv("POLL_FBW_SECONDS", "3600"))
POLL_FEEDBACKS_SECONDS = int(os.getenv("POLL_FEEDBACKS_SECONDS", "300"))
POLL_QUESTIONS_SECONDS = int(os.getenv("POLL_QUESTIONS_SECONDS", "300"))
POLL_FBS_SECONDS = int(os.getenv("POLL_FBS_SECONDS", "30"))

DAILY_SUMMARY_HOUR_MSK = int(os.getenv("DAILY_SUMMARY_HOUR_MSK", "23"))
DAILY_SUMMARY_MINUTE_MSK = int(os.getenv("DAILY_SUMMARY_MINUTE_MSK", "50"))

# MSK is UTC+3
MSK = timezone(timedelta(hours=3))

# ---------------------------
# HTTP helpers
# ---------------------------

DEFAULT_TIMEOUT = (5, 30)  # connect, read

# per-host cooldown timestamp (epoch seconds)
_HOST_COOLDOWN: Dict[str, float] = {}


def _now() -> float:
    return time.time()


def _cooldown_left(host: str) -> float:
    until = _HOST_COOLDOWN.get(host, 0.0)
    left = until - _now()
    return left if left > 0 else 0.0


def _set_cooldown(host: str, seconds: float) -> None:
    _HOST_COOLDOWN[host] = max(_HOST_COOLDOWN.get(host, 0.0), _now() + seconds)


def _host_from_url(url: str) -> str:
    try:
        return url.split("//", 1)[1].split("/", 1)[0]
    except Exception:
        return url


def http_get_json(url: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Any:
    """Blocking GET with cooldown + backoff on transient errors.

    Returns parsed JSON on success, or raises requests.HTTPError for non-retriable errors.
    """
    host = _host_from_url(url)

    # Respect cooldown
    left = _cooldown_left(host)
    if left > 0:
        raise RuntimeError(f"cooldown {host} {left:.0f}s")

    # Exponential backoff attempts
    base_sleep = 1.0
    for attempt in range(1, 6):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=DEFAULT_TIMEOUT)
            if r.status_code == 429:
                # WB limiter; back off hard
                retry = 60
                try:
                    retry = int(r.headers.get("Retry-After") or retry)
                except Exception:
                    pass
                _set_cooldown(host, max(60, retry))
                raise RuntimeError(f"429 Too Many Requests ({host})")

            if r.status_code in (502, 503, 504):
                # Gateway issues
                sleep_s = min(300, base_sleep * (2 ** (attempt - 1)))
                _set_cooldown(host, sleep_s)
                raise RuntimeError(f"{r.status_code} gateway ({host})")

            if r.status_code >= 400:
                # Non-transient
                try:
                    detail = r.json()
                except Exception:
                    detail = r.text[:500]
                raise requests.HTTPError(f"{r.status_code} {detail}")

            return r.json()

        except (requests.ConnectionError, requests.Timeout) as e:
            # Network hiccup
            sleep_s = min(120, base_sleep * (2 ** (attempt - 1))) + random.uniform(0, 2)
            _set_cooldown(host, sleep_s)
            if attempt == 5:
                raise RuntimeError(f"network error {host}: {e}")
        except ValueError as e:
            # JSON parse
            raise RuntimeError(f"bad json {host}: {e}")

    raise RuntimeError(f"http_get_json failed {url}")


# ---------------------------
# Telegram
# ---------------------------


def tg_send(text: str) -> None:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("⚠️ TG creds missing; message skipped")
        return

    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TG_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=(5, 20))
        if r.status_code >= 400:
            print(f"⚠️ TG send error {r.status_code}: {r.text[:300]}")
    except Exception as e:
        print(f"⚠️ TG send exception: {e}")


# ---------------------------
# DB (minimal)
# ---------------------------

_DB_CONN: Optional[sqlite3.Connection] = None


def db() -> sqlite3.Connection:
    global _DB_CONN
    if _DB_CONN is None:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        _DB_CONN = sqlite3.connect(DB_PATH, check_same_thread=False)
        _DB_CONN.execute(
            "CREATE TABLE IF NOT EXISTS sent (k TEXT PRIMARY KEY, ts INTEGER)"
        )
        _DB_CONN.commit()
    return _DB_CONN


def dedup_seen(key: str, ttl_days: int = 30) -> bool:
    """Return True if key already seen, else store and return False."""
    conn = db()
    now_ts = int(time.time())
    try:
        row = conn.execute("SELECT 1 FROM sent WHERE k=?", (key,)).fetchone()
        if row:
            return True
        conn.execute("INSERT OR REPLACE INTO sent(k, ts) VALUES(?, ?)", (key, now_ts))
        # light cleanup
        cutoff = now_ts - ttl_days * 86400
        conn.execute("DELETE FROM sent WHERE ts < ?", (cutoff,))
        conn.commit()
        return False
    except Exception as e:
        print(f"⚠️ DB dedup error: {e}")
        return False


# ---------------------------
# WB API wrappers (Statistics + Feedbacks)
# ---------------------------


def stats_headers() -> Dict[str, str]:
    return {"Authorization": WB_STATS_TOKEN}


def feedbacks_headers() -> Dict[str, str]:
    return {"Authorization": WB_FEEDBACKS_TOKEN}


def rfc3339_msk_start_of_day(d: datetime) -> str:
    d0 = d.astimezone(MSK).replace(hour=0, minute=0, second=0, microsecond=0)
    # format like 2026-03-03T00:00:00+03:00
    return d0.isoformat(timespec="seconds")


def get_sales(date_from: datetime) -> List[Dict[str, Any]]:
    if not WB_STATS_TOKEN:
        raise RuntimeError("WB_STATS_TOKEN missing")
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/sales"
    params = {"dateFrom": rfc3339_msk_start_of_day(date_from), "flag": 1}
    return http_get_json(url, stats_headers(), params=params) or []


def get_orders(date_from: datetime) -> List[Dict[str, Any]]:
    if not WB_STATS_TOKEN:
        raise RuntimeError("WB_STATS_TOKEN missing")
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/orders"
    params = {"dateFrom": rfc3339_msk_start_of_day(date_from), "flag": 1}
    return http_get_json(url, stats_headers(), params=params) or []


def get_stocks_full(date_from: datetime) -> List[Dict[str, Any]]:
    if not WB_STATS_TOKEN:
        raise RuntimeError("WB_STATS_TOKEN missing")
    url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks"
    params = {"dateFrom": rfc3339_msk_start_of_day(date_from)}
    return http_get_json(url, stats_headers(), params=params) or []


def get_feedbacks_page(is_answered: bool, take: int = 100, skip: int = 0) -> List[Dict[str, Any]]:
    if not WB_FEEDBACKS_TOKEN:
        raise RuntimeError("WB_FEEDBACKS_TOKEN missing")
    url = "https://feedbacks-api.wildberries.ru/api/v1/feedbacks"
    params = {
        "isAnswered": str(is_answered).lower(),
        "take": take,
        "skip": skip,
        "order": "dateDesc",
    }
    return http_get_json(url, feedbacks_headers(), params=params) or []


def get_questions_page(is_answered: bool, take: int = 100, skip: int = 0) -> List[Dict[str, Any]]:
    if not WB_FEEDBACKS_TOKEN:
        raise RuntimeError("WB_FEEDBACKS_TOKEN missing")
    url = "https://feedbacks-api.wildberries.ru/api/v1/questions"
    params = {
        "isAnswered": str(is_answered).lower(),
        "take": take,
        "skip": skip,
        "order": "dateDesc",
    }
    return http_get_json(url, feedbacks_headers(), params=params) or []


# ---------------------------
# Business logic (minimal but correct direction)
# ---------------------------


def _money(v: Any) -> float:
    try:
        return float(v)
    except Exception:
        return 0.0


def classify_sale(s: Dict[str, Any]) -> str:
    # WB commonly uses saleID starting with 'R' for returns; also negative forPay.
    sale_id = str(s.get("saleID") or "")
    for_pay = _money(s.get("forPay"))
    if sale_id.startswith("R") or for_pay < 0:
        return "return"
    return "sale"


def sale_amount(s: Dict[str, Any]) -> float:
    # Prefer forPay (amount to pay to seller). Fallbacks.
    for k in ("forPay", "finishedPrice", "priceWithDisc", "totalPrice"):
        if k in s:
            v = _money(s.get(k))
            if v != 0:
                return v
    return 0.0


def order_amount(o: Dict[str, Any]) -> float:
    for k in ("finishedPrice", "priceWithDisc", "totalPrice"):
        if k in o:
            v = _money(o.get(k))
            if v != 0:
                return v
    return 0.0


def stock_qty(row: Dict[str, Any]) -> int:
    for k in ("quantity", "qty", "amount", "quantityFull"):
        if k in row and row.get(k) is not None:
            try:
                return int(float(row.get(k)))
            except Exception:
                pass
    return 0


def sum_stocks_for_item(
    stocks: List[Dict[str, Any]],
    nm_id: Optional[int],
    barcode: Optional[str],
    supplier_article: Optional[str],
) -> int:
    # Best match by barcode, then nmId, then supplierArticle
    total = 0
    if barcode:
        for r in stocks:
            if str(r.get("barcode") or "") == str(barcode):
                total += stock_qty(r)
        if total:
            return total

    if nm_id is not None:
        for r in stocks:
            try:
                if int(r.get("nmId") or 0) == int(nm_id):
                    total += stock_qty(r)
            except Exception:
                continue
        if total:
            return total

    if supplier_article:
        for r in stocks:
            if str(r.get("supplierArticle") or "") == str(supplier_article):
                total += stock_qty(r)
    return total


# ---------------------------
# Poll loops
# ---------------------------


def _sleep_with_jitter(seconds: int) -> float:
    return max(1.0, seconds + random.uniform(0, min(3.0, seconds * 0.1)))


async def poll_sales_loop() -> None:
    # poll today's sales frequently; summary uses same.
    last_day = datetime.now(MSK).date()
    while True:
        try:
            today = datetime.now(MSK)
            # If day changed, reset dedup window naturally via keys
            if today.date() != last_day:
                last_day = today.date()

            rows = await asyncio.to_thread(get_sales, today)
            for s in rows:
                # Dedup key
                key = f"sale:{s.get('srid') or s.get('saleID') or json.dumps(s, sort_keys=True)[:120]}"
                if dedup_seen(key):
                    continue

                kind = classify_sale(s)
                amt = sale_amount(s)
                nm = s.get("subject") or s.get("supplierArticle") or "Товар"
                dt = s.get("date") or s.get("lastChangeDate") or ""

                if kind == "sale":
                    msg = f"✅ Выкуп\nТовар: {nm}\nСумма: {amt:.2f} ₽\nДата: {dt}"
                else:
                    msg = f"↩️ Возврат/отказ\nТовар: {nm}\nСумма: {amt:.2f} ₽\nДата: {dt}"
                await asyncio.to_thread(tg_send, msg)

        except Exception as e:
            print(f"⚠️ sales polling error: {e}")

        await asyncio.sleep(_sleep_with_jitter(POLL_SALES_SECONDS))


async def poll_fbw_loop() -> None:
    # FBW orders + stocks summary. We only use orders here for events; stocks are used for 'Остаток'.
    # To keep load low, fetch stocks as a cached snapshot periodically.
    stocks_cache: List[Dict[str, Any]] = []
    stocks_cache_ts = 0.0

    while True:
        try:
            now = datetime.now(MSK)

            # refresh stocks snapshot every 30 minutes; use old dateFrom to approximate "full" list
            if _now() - stocks_cache_ts > 1800:
                # Using 2020-01-01 reduces risk of empty stocks due to incremental semantics
                stocks_cache = await asyncio.to_thread(get_stocks_full, datetime(2020, 1, 1, tzinfo=MSK))
                stocks_cache_ts = _now()

            # orders for today
            orders = await asyncio.to_thread(get_orders, now)
            for o in orders:
                key = f"order:{o.get('srid') or o.get('gNumber') or o.get('odid') or json.dumps(o, sort_keys=True)[:120]}"
                if dedup_seen(key):
                    continue

                is_cancel = bool(o.get("isCancel"))
                cancel_date = o.get("cancelDate") or ""

                nm_id = None
                try:
                    nm_id = int(o.get("nmId") or 0) or None
                except Exception:
                    nm_id = None
                barcode = (o.get("barcode") or "")
                supplier_article = (o.get("supplierArticle") or "")

                qty = int(float(o.get("quantity") or 1)) if o.get("quantity") is not None else 1
                amount = order_amount(o)

                stock_sum = sum_stocks_for_item(stocks_cache, nm_id, barcode, supplier_article)

                title = "❌ Отмена заказа" if is_cancel else "🏬 Заказ (FBW)"
                lines = [
                    f"{title}",
                    f"Товар: {o.get('subject') or supplier_article or '—'}",
                ]
                if nm_id:
                    lines.append(f"Артикул WB: {nm_id}")
                if amount:
                    lines.append(f"Сумма: {amount:.2f} ₽")
                lines.append(f"Кол-во: {qty}")
                lines.append(f"Остаток (сумма по складам WB): {stock_sum}")
                if is_cancel and cancel_date:
                    lines.append(f"Дата отмены: {cancel_date}")
                dt = o.get("date") or o.get("lastChangeDate") or ""
                if dt:
                    lines.append(f"Дата: {dt}")

                await asyncio.to_thread(tg_send, "\n".join(lines))

        except Exception as e:
            print(f"⚠️ FBW polling error: {e}")

        await asyncio.sleep(_sleep_with_jitter(POLL_FBW_SECONDS))


async def poll_feedbacks_loop() -> None:
    while True:
        try:
            rows = await asyncio.to_thread(get_feedbacks_page, True, 100, 0)
            for f in rows:
                key = f"fb:{f.get('id') or f.get('uuid') or json.dumps(f, sort_keys=True)[:120]}"
                if dedup_seen(key):
                    continue
                rating = f.get("productValuation") or f.get("valuation") or ""
                text = (f.get("text") or "").strip() or "(без текста, только оценка)"
                nm = f.get("productName") or f.get("subject") or "Товар"
                dt = f.get("createdDate") or f.get("date") or ""
                msg = f"💬 Новый отзыв\nТовар: {nm}\nОценка: {rating}\nОтзыв: {text}\nДата: {dt}"
                await asyncio.to_thread(tg_send, msg)

        except Exception as e:
            print(f"⚠️ feedbacks error: {e}")

        await asyncio.sleep(_sleep_with_jitter(POLL_FEEDBACKS_SECONDS))


async def poll_questions_loop() -> None:
    while True:
        try:
            rows = await asyncio.to_thread(get_questions_page, True, 100, 0)
            for q in rows:
                key = f"q:{q.get('id') or q.get('uuid') or json.dumps(q, sort_keys=True)[:120]}"
                if dedup_seen(key):
                    continue
                text = (q.get("text") or "").strip() or "(без текста)"
                nm = q.get("productName") or q.get("subject") or "Товар"
                dt = q.get("createdDate") or q.get("date") or ""
                msg = f"❓ Новый вопрос\nТовар: {nm}\nВопрос: {text}\nДата: {dt}"
                await asyncio.to_thread(tg_send, msg)

        except Exception as e:
            print(f"⚠️ questions error: {e}")

        await asyncio.sleep(_sleep_with_jitter(POLL_QUESTIONS_SECONDS))


# Placeholder FBS polling (kept minimal; safe even if you mostly FBW)
async def poll_marketplace_loop() -> None:
    while True:
        try:
            # Not implemented fully here to keep load low; your main focus is FBW.
            # If needed, we can add marketplace orders/new later.
            pass
        except Exception as e:
            print(f"⚠️ marketplace polling error: {e}")
        await asyncio.sleep(_sleep_with_jitter(POLL_FBS_SECONDS))


async def daily_summary_loop() -> None:
    while True:
        try:
            now = datetime.now(MSK)
            target = now.replace(hour=DAILY_SUMMARY_HOUR_MSK, minute=DAILY_SUMMARY_MINUTE_MSK, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)
            await asyncio.sleep((target - now).total_seconds())

            day = datetime.now(MSK)
            sales = await asyncio.to_thread(get_sales, day)
            orders = await asyncio.to_thread(get_orders, day)

            sold_cnt = 0
            sold_sum = 0.0
            cancel_cnt = 0
            cancel_sum = 0.0

            for o in orders:
                if bool(o.get("isCancel")):
                    cancel_cnt += 1
                    cancel_sum += order_amount(o)
                else:
                    sold_cnt += int(float(o.get("quantity") or 1))
                    sold_sum += order_amount(o)

            buy_cnt = 0
            buy_sum = 0.0
            ret_cnt = 0
            ret_sum = 0.0

            for s in sales:
                amt = sale_amount(s)
                if classify_sale(s) == "sale":
                    buy_cnt += 1
                    buy_sum += amt
                else:
                    ret_cnt += 1
                    ret_sum += amt

            msg = (
                f"📊 Итог дня ({day.strftime('%d.%m.%Y')})\n"
                f"🛒 Товаров продано: {sold_cnt} на сумму {sold_sum:.2f} ₽\n"
                f"✅ Выкуп товаров произведён: {buy_cnt} на сумму {buy_sum:.2f} ₽\n"
                f"❌ Отмен: {cancel_cnt} на сумму {cancel_sum:.2f} ₽\n"
                f"↩️ Возвратов/отказов: {ret_cnt} на сумму {ret_sum:.2f} ₽"
            )
            await asyncio.to_thread(tg_send, msg)

        except Exception as e:
            print(f"⚠️ daily summary error: {e}")
            await asyncio.sleep(60)


# ---------------------------
# FastAPI app (Render)
# ---------------------------

app = FastAPI(default_response_class=JSONResponse)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "time": datetime.utcnow().isoformat(timespec="seconds") + "Z",
    }


@app.on_event("startup")
async def startup() -> None:
    # IMPORTANT: do not block startup.
    # Any DB/network warmups go to background threads.
    asyncio.create_task(asyncio.to_thread(db))

    # Gentle hello in background (never blocks port opening)
    if not DISABLE_STARTUP_HELLO:
        asyncio.create_task(
            asyncio.to_thread(
                tg_send,
                "✅ WB→Telegram запущен (Render-safe). /health доступен."
            )
        )

    # Start loops
    asyncio.create_task(poll_marketplace_loop())
    asyncio.create_task(poll_sales_loop())
    asyncio.create_task(poll_feedbacks_loop())
    asyncio.create_task(poll_fbw_loop())
    asyncio.create_task(poll_questions_loop())
    asyncio.create_task(daily_summary_loop())


def _looks_like_mojibake(s: str) -> bool:
    # типичный мусор "Р°", "СЃ" и т.п.
    if not s:
        return False
    if ("Р" not in s) and ("С" not in s):
        return False
    # если есть много последовательностей вида "Р°" / "СЃ" и т.п. — почти наверняка кракозябра
    return bool(re.search(r"(Р[а-яА-ЯёЁ]|С[а-яА-ЯёЁ]|Р[^\s]|С[^\s])", s))

def fix_mojibake(s: str) -> str:
    """
    Лечит типичные кракозябры WB вида "Р°С..." (когда UTF-8 текст был неверно интерпретирован).
    ВАЖНО: не ломает нормальный русский текст (пробелы не вырезаем без необходимости).
    """
    s = _safe_str(s)
    if not s:
        return ""

    if not _looks_like_mojibake(s):
        return s

    candidates: List[str] = []

    # вариант А: UTF-8 байты прочитали как latin1
    try:
        candidates.append(s.encode("latin1", errors="strict").decode("utf-8", errors="strict"))
    except Exception:
        pass

    # вариант Б: UTF-8 байты прочитали как cp1251
    try:
        candidates.append(s.encode("cp1251", errors="strict").decode("utf-8", errors="strict"))
    except Exception:
        pass

    # вариант В: иногда прилетают строки "Р Р°С ..." (с мусорными пробелами/nbsp).
    # Убираем ТОЛЬКО если видно что пробелов слишком много и это именно кракозябра.
    if s.count(" ") > max(2, len(s) // 6) or "\u00A0" in s:
        compact = s.replace("\u00A0", "").replace(" ", "")
        if compact != s and _looks_like_mojibake(compact):
            try:
                candidates.append(compact.encode("latin1", errors="strict").decode("utf-8", errors="strict"))
            except Exception:
                pass
            try:
                candidates.append(compact.encode("cp1251", errors="strict").decode("utf-8", errors="strict"))
            except Exception:
                pass

    # выбираем “лучшего”: меньше 'Р'/'С' и нет �
    def score(t: str) -> Tuple[int, int, int]:
        return (t.count("�"), t.count("Р") + t.count("С"), -sum(1 for ch in t if "А" <= ch <= "я" or ch in "Ёё"))

    best = None
    for t in candidates:
        t = _safe_str(t)
        if not t:
            continue
        if best is None or score(t) < score(best):
            best = t

    return best if best is not None else s

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
    rating = max(0, min(5, rating))
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
# Helpers: DB (dedup + cursors)
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
# WB HTTP (ЕДИНСТВЕННЫЕ wb_get/wb_post — без дублей)
# -------------------------
def _decode_json_from_response(r: requests.Response) -> Any:
    raw = r.content or b""
    for enc in ("utf-8", "cp1251"):
        try:
            return json.loads(raw.decode(enc))
        except Exception:
            pass
    try:
        return r.json()
    except Exception:
        return (raw.decode("utf-8", errors="replace") or r.text)

_WB_COOLDOWN_UNTIL: dict[str, float] = {}

def _wb_request_with_429_retry(method: str, url: str, headers: dict, *, params=None, json_payload=None, timeout: int = 25) -> requests.Response:
    """
    Устойчивые запросы к WB:
    - уважает 429 (X-Ratelimit-Retry / Retry-After) и ставит cooldown по хосту
    - ретраи на сетевые обрывы (RemoteDisconnected/SSLError/etc.)
    - экспоненциальный backoff с потолком
    """
    try:
        host = requests.utils.urlparse(url).hostname or ""
    except Exception:
        host = ""

    # если по хосту включён cooldown — подождём чуть-чуть
    now = time.time()
    until = _WB_COOLDOWN_UNTIL.get(host, 0.0)
    if until > now:
        time.sleep(min(60, max(1, until - now)))

    backoff = 1.0
    last_resp: Optional[requests.Response] = None

    for attempt in range(1, 6):  # до 5 попыток
        try:
            r = requests.request(method, url, headers=headers, params=params, json=json_payload, timeout=timeout)
            last_resp = r

            if r.status_code != 429:
                return r

            retry = r.headers.get("X-Ratelimit-Retry") or r.headers.get("Retry-After")
            try:
                wait_s = int(float(retry)) if retry is not None else int(backoff)
            except Exception:
                wait_s = int(backoff)

            wait_s = max(2, min(wait_s, 300))
            _WB_COOLDOWN_UNTIL[host] = time.time() + wait_s
            time.sleep(wait_s)

        except requests.RequestException:
            time.sleep(min(30, backoff))

        backoff = min(30.0, backoff * 2.0)

    if last_resp is not None:
        return last_resp
    raise

def wb_get(url: str, token: str, params: Optional[dict] = None, timeout: int = 25) -> Any:
    headers = {"Authorization": token}
    r = _wb_request_with_429_retry("GET", url, headers, params=params, timeout=timeout)

    if r.status_code >= 400:
        return {
            "__error__": True,
            "status_code": r.status_code,
            "url": r.url,
            "response_text": r.text,
            "ratelimit_retry": r.headers.get("X-Ratelimit-Retry"),
            "ratelimit_reset": r.headers.get("X-Ratelimit-Reset"),
        }

    return _decode_json_from_response(r)

def wb_post(url: str, token: str, payload: dict, timeout: int = 25) -> Any:
    headers = {"Authorization": token}
    r = _wb_request_with_429_retry("POST", url, headers, json_payload=payload, timeout=timeout)

    if r.status_code >= 400:
        return {
            "__error__": True,
            "status_code": r.status_code,
            "url": r.url,
            "response_text": r.text,
            "ratelimit_retry": r.headers.get("X-Ratelimit-Retry"),
            "ratelimit_reset": r.headers.get("X-Ratelimit-Reset"),
        }

    return _decode_json_from_response(r)


# -------------------------
# Content API: title + sizes (cache)
# -------------------------
_TITLE_CACHE: Dict[str, Tuple[float, str]] = {}
_TITLE_CACHE_TTL = 24 * 3600  # 24 часа

def content_get_title(nm_id: Optional[int] = None, vendor_code: str = "") -> str:
    if not WB_CONTENT_TOKEN:
        return ""

    key = f"nm:{nm_id}" if nm_id else f"vc:{vendor_code}"
    now = time.time()
    if key in _TITLE_CACHE:
        ts, title = _TITLE_CACHE[key]
        if now - ts <= _TITLE_CACHE_TTL:
            return title

    text_search = str(nm_id) if nm_id else _safe_str(vendor_code)
    if not text_search:
        return ""

    url = f"{WB_CONTENT_BASE}/content/v2/get/cards/list"
    payload = {
        "settings": {
            "sort": {"ascending": False},
            "filter": {"textSearch": text_search, "withPhoto": -1},
            "cursor": {"limit": 10}
        }
    }

    data = wb_post(url, WB_CONTENT_TOKEN, payload=payload)
    if isinstance(data, dict) and data.get("__error__"):
        return ""

    cards = data.get("cards") if isinstance(data, dict) else None
    if not isinstance(cards, list) or not cards:
        return ""

    if nm_id:
        for c in cards:
            if isinstance(c, dict) and str(c.get("nmID")) == str(nm_id):
                title = fix_mojibake(_safe_str(c.get("title")))
                if title:
                    _TITLE_CACHE[key] = (now, title)
                    return title

    title = fix_mojibake(_safe_str(cards[0].get("title"))) if isinstance(cards[0], dict) else ""
    if title:
        _TITLE_CACHE[key] = (now, title)
    return title

_SIZES_CACHE: Dict[int, Tuple[float, List[Dict[str, Any]]]] = {}
_SIZES_CACHE_TTL = 24 * 3600

def content_get_sizes(nm_id: int) -> List[Dict[str, Any]]:
    if not WB_CONTENT_TOKEN or not nm_id:
        return []

    now = time.time()
    if nm_id in _SIZES_CACHE:
        ts, sizes = _SIZES_CACHE[nm_id]
        if now - ts <= _SIZES_CACHE_TTL:
            return sizes

    url = f"{WB_CONTENT_BASE}/content/v2/get/cards/list"
    payload = {
        "settings": {
            "sort": {"ascending": False},
            "filter": {"textSearch": str(nm_id), "withPhoto": -1},
            "cursor": {"limit": 10}
        }
    }

    data = wb_post(url, WB_CONTENT_TOKEN, payload=payload)
    if not isinstance(data, dict) or data.get("__error__"):
        return []

    cards = data.get("cards")
    if not isinstance(cards, list) or not cards:
        return []

    card = None
    for c in cards:
        if isinstance(c, dict) and str(c.get("nmID")) == str(nm_id):
            card = c
            break
    if not isinstance(card, dict):
        card = cards[0] if isinstance(cards[0], dict) else None
    if not isinstance(card, dict):
        return []

    sizes = card.get("sizes")
    if not isinstance(sizes, list):
        return []

    out: List[Dict[str, Any]] = []
    for s in sizes:
        if not isinstance(s, dict):
            continue
        try:
            chrt = int(s.get("chrtID") or s.get("chrtId") or 0)
        except Exception:
            chrt = 0
        skus = s.get("skus")
        if not isinstance(skus, list):
            skus = []
        tech = fix_mojibake(_safe_str(s.get("techSize") or s.get("size") or ""))
        if chrt > 0:
            out.append({"chrtId": chrt, "skus": [str(x) for x in skus if x], "techSize": tech})

    _SIZES_CACHE[nm_id] = (now, out)
    return out


# -------------------------
# Marketplace Inventory (seller warehouse stocks) cache
# -------------------------
_STOCKS_CACHE: Dict[str, Tuple[float, Dict[int, int]]] = {}
_STOCKS_CACHE_TTL = 30

def mp_get_inventory_map(warehouse_id: str, chrt_ids: List[int]) -> Dict[int, int]:
    if not WB_MP_TOKEN or not warehouse_id:
        return {}

    chrt_ids2: List[int] = []
    for x in chrt_ids:
        try:
            xi = int(x)
            if xi > 0:
                chrt_ids2.append(xi)
        except Exception:
            continue
    chrt_ids2 = list({x for x in chrt_ids2})
    if not chrt_ids2:
        return {}

    cache_key = f"{warehouse_id}:{','.join(map(str, sorted(chrt_ids2)))}"
    now = time.time()
    if cache_key in _STOCKS_CACHE:
        ts, data = _STOCKS_CACHE[cache_key]
        if now - ts <= _STOCKS_CACHE_TTL:
            return data

    url = f"{WB_MARKETPLACE_BASE}/api/v3/stocks/{warehouse_id}"
    data = wb_post(url, WB_MP_TOKEN, payload={"chrtIds": chrt_ids2})
    if isinstance(data, dict) and data.get("__error__"):
        return {}

    out: Dict[int, int] = {}
    if isinstance(data, dict) and isinstance(data.get("stocks"), list):
        for row in data["stocks"]:
            if not isinstance(row, dict):
                continue
            try:
                cid = int(row.get("chrtId"))
                amt = int(row.get("amount"))
                out[cid] = amt
            except Exception:
                continue

    _STOCKS_CACHE[cache_key] = (now, out)
    return out


# -------------------------
# Seller warehouses (marketplace /api/v3/warehouses) cache
# -------------------------
_WAREHOUSES_CACHE: Tuple[float, List[Dict[str, Any]]] = (0.0, [])
_WAREHOUSES_TTL = 300

def mp_list_warehouses() -> List[Dict[str, Any]]:
    global _WAREHOUSES_CACHE
    if not WB_MP_TOKEN:
        return []
    now = time.time()
    ts, cached = _WAREHOUSES_CACHE
    if cached and (now - ts) <= _WAREHOUSES_TTL:
        return cached

    url = f"{WB_MARKETPLACE_BASE}/api/v3/warehouses"
    data = wb_get(url, WB_MP_TOKEN, params=None)
    if not isinstance(data, list):
        return []
    out = [w for w in data if isinstance(w, dict)]
    _WAREHOUSES_CACHE = (now, out)
    return out

def _warehouse_id_by_name(warehouse_name: str) -> str:
    # 1) явный ENV
    if SELLER_WAREHOUSE_ID:
        return SELLER_WAREHOUSE_ID

    wn = _norm_ws(warehouse_name)
    if not wn:
        return ""

    best_id = ""
    best_score = 0
    for w in mp_list_warehouses():
        wid = _safe_str(w.get("id") or w.get("warehouseId") or w.get("warehouseID") or "")
        wname = _norm_ws(w.get("name") or w.get("warehouseName") or w.get("officeName") or "")
        if not wid or not wname:
            continue
        if wn == wname:
            return wid
        if wn in wname or wname in wn:
            score = min(len(wn), len(wname))
            if score > best_score:
                best_score = score
                best_id = wid

    return best_id

def seller_stock_quantity(warehouse_name: str, barcode: str, nm_id: Optional[int] = None, supplier_article: str = "") -> Optional[int]:
    if not WB_MP_TOKEN:
        return None
    if not nm_id:
        return None

    bc = _safe_str(barcode)
    sizes = content_get_sizes(int(nm_id))
    if not sizes:
        return None

    chrt_id = 0
    if bc:
        for s in sizes:
            if bc in (s.get("skus") or []):
                try:
                    chrt_id = int(s.get("chrtId") or 0)
                except Exception:
                    chrt_id = 0
                break
    if chrt_id <= 0 and len(sizes) == 1:
        try:
            chrt_id = int(sizes[0].get("chrtId") or 0)
        except Exception:
            chrt_id = 0

    if chrt_id <= 0:
        return None

    wid = _warehouse_id_by_name(warehouse_name)
    if not wid:
        return None

    inv = mp_get_inventory_map(wid, [chrt_id])
    if chrt_id in inv:
        return inv[chrt_id]
    return None

# -------------------------
# FBW stocks (Statistics supplier/stocks) cache
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

    for r in data:
        if isinstance(r, dict):
            for k in ("warehouseName", "supplierArticle", "category", "subject", "nmName"):
                if k in r:
                    r[k] = fix_mojibake(_safe_str(r.get(k)))

    _FBW_STOCKS_CACHE = (now, data)
    return data

def _norm_ws(s: str) -> str:
    s = fix_mojibake(_safe_str(s)).lower()
    return " ".join(s.replace("–", "-").replace("—", "-").split())

def fbw_stock_quantity(warehouse: str, barcode: str, nm_id: Optional[int] = None, supplier_article: str = "") -> Optional[int]:
    w = _norm_ws(warehouse)
    bc = _safe_str(barcode)
    sa = _norm_ws(supplier_article)

    rows = stats_fetch_fbw_stocks()
    if not rows:
        return None

    def pick_qty(r: Dict[str, Any]) -> Optional[int]:
        for k in ("quantityFull", "quantity", "QuantityFull", "Quantity"):
            if k in r:
                try:
                    return int(r.get(k) or 0)
                except Exception:
                    pass
        return None

    # 1) точное: warehouse + barcode
    if w and bc:
        for r in rows:
            if not isinstance(r, dict):
                continue
            rw = _norm_ws(r.get("warehouseName"))
            if rw == w and _safe_str(r.get("barcode")) == bc:
                q = pick_qty(r)
                if isinstance(q, int):
                    return q

    # 2) мягкое: warehouse содержит/входит + barcode
    if w and bc:
        for r in rows:
            if not isinstance(r, dict):
                continue
            rw = _norm_ws(r.get("warehouseName"))
            if (w in rw or rw in w) and _safe_str(r.get("barcode")) == bc:
                q = pick_qty(r)
                if isinstance(q, int):
                    return q

    # 3) fallback: warehouse + nmId
    if w and nm_id:
        for r in rows:
            if not isinstance(r, dict):
                continue
            rw = _norm_ws(r.get("warehouseName"))
            try:
                r_nm = int(r.get("nmId") or r.get("nmID") or 0)
            except Exception:
                r_nm = 0
            if (rw == w or w in rw or rw in w) and r_nm == int(nm_id):
                q = pick_qty(r)
                if isinstance(q, int):
                    return q

    # 4) fallback: warehouse + supplierArticle
    if w and sa:
        for r in rows:
            if not isinstance(r, dict):
                continue
            rw = _norm_ws(r.get("warehouseName"))
            r_sa = _norm_ws(r.get("supplierArticle"))
            if (rw == w or w in rw or rw in w) and r_sa and r_sa == sa:
                q = pick_qty(r)
                if isinstance(q, int):
                    return q

    return None


# -------------------------
# Marketplace: new orders (FBS/DBS/DBW)
# -------------------------

def fbw_stock_total(barcode: str, nm_id: Optional[int] = None, supplier_article: str = "") -> Optional[int]:
    """Суммарный остаток FBW (склады WB) по всем складам.
    1) Суммируем по точному barcode (самый точный вариант для размера/варианта).
    2) Если barcode нет/не найден — суммируем по nmId.
    3) Если nmId нет/не найден — пробуем по supplierArticle.
    """
    bc = _safe_str(barcode)
    sa = _norm_ws(supplier_article)
    rows = stats_fetch_fbw_stocks()
    if not rows:
        return None

    def pick_qty(r: Dict[str, Any]) -> int:
        for k in ("quantityFull", "quantity", "QuantityFull", "Quantity"):
            if k in r:
                try:
                    return int(r.get(k) or 0)
                except Exception:
                    return 0
        return 0

    total = 0
    matched = 0

    if bc:
        for r in rows:
            if isinstance(r, dict) and _safe_str(r.get("barcode")) == bc:
                total += pick_qty(r)
                matched += 1
        if matched:
            return total

    if nm_id is not None:
        for r in rows:
            if not isinstance(r, dict):
                continue
            rid = r.get("nmId") or r.get("nmID") or r.get("nm_id")
            if rid is None:
                continue
            try:
                if int(float(rid)) == int(nm_id):
                    total += pick_qty(r)
                    matched += 1
            except Exception:
                continue
        if matched:
            return total

    if sa:
        for r in rows:
            if not isinstance(r, dict):
                continue
            rsa = _norm_ws(r.get("supplierArticle") or r.get("vendorCode") or r.get("article") or "")
            if rsa == sa:
                total += pick_qty(r)
                matched += 1
        if matched:
            return total

    return None


def _extract_items_from_mp_order(o: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = o.get("items")
    if isinstance(items, list) and items:
        return [it for it in items if isinstance(it, dict)]
    return [o]

def pick_best_name_from_order(it: Dict[str, Any]) -> str:
    candidates = [
        it.get("productName"),
        it.get("nmName"),
        it.get("goodsName"),
        it.get("name"),
        it.get("imtName"),
        it.get("title"),
    ]
    for c in candidates:
        s = fix_mojibake(_safe_str(c))
        if s:
            return s
    subject = fix_mojibake(_safe_str(it.get("subject") or it.get("subjectName")))
    return subject or "Товар"

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
            oid = _safe_str(o.get("id") or o.get("orderId") or o.get("rid") or o.get("srid"))
            if not oid:
                oid = str(abs(hash(json.dumps(o, ensure_ascii=False, sort_keys=True))))
            found.append((kind, {"_id": oid, **o}))

    return found

def format_mp_order(kind: str, o: Dict[str, Any]) -> str:
    oid = _safe_str(o.get("_id"))
    warehouse = fix_mojibake(_safe_str(o.get("warehouseName") or o.get("warehouse") or o.get("officeName") or ""))
    header = f"🏬 Новый заказ ({kind}) · {SHOP_NAME}"

    items = _extract_items_from_mp_order(o)

    chrt_ids: List[int] = []
    for it in items:
        cid = it.get("chrtId") or it.get("chrtID")
        try:
            ci = int(cid) if cid is not None else 0
        except Exception:
            ci = 0
        if ci > 0:
            chrt_ids.append(ci)

    stocks_map: Dict[int, int] = {}
    if SELLER_WAREHOUSE_ID and chrt_ids:
        stocks_map = mp_get_inventory_map(SELLER_WAREHOUSE_ID, chrt_ids)

    lines: List[str] = []
    total_qty = 0
    total_sum = 0.0

    for it in items:
        vendor_code = fix_mojibake(_safe_str(it.get("supplierArticle") or it.get("vendorCode") or it.get("article") or ""))
        product_name = pick_best_name_from_order(it)

        nm_id_raw = it.get("nmId") or it.get("nmID")
        nm_id: Optional[int] = None
        if nm_id_raw is not None:
            try:
                nm_id = int(float(nm_id_raw))
            except Exception:
                nm_id = None

        subject = fix_mojibake(_safe_str(it.get("subject") or it.get("subjectName")))
        if nm_id:
            full_title = content_get_title(nm_id=nm_id, vendor_code=vendor_code)
            if full_title:
                product_name = full_title
        elif subject and product_name == subject:
            full_title = content_get_title(nm_id=None, vendor_code=vendor_code)
            if full_title:
                product_name = full_title

        qty = it.get("quantity") or it.get("qty") or 1
        try:
            qty_int = int(qty)
        except Exception:
            qty_int = 1
        if qty_int <= 0:
            qty_int = 1

        price = (
            it.get("priceWithDisc")
            or it.get("finishedPrice")
            or it.get("forPay")
            or it.get("totalPrice")
            or it.get("price")
            or 0
        )
        try:
            price_f = float(price)
        except Exception:
            price_f = 0.0

        cid = it.get("chrtId") or it.get("chrtID")
        try:
            cid_int = int(cid) if cid is not None else 0
        except Exception:
            cid_int = 0

        ost_line = "Остаток: -"
        if cid_int and cid_int in stocks_map:
            ost_line = f"Остаток: {stocks_map[cid_int]} шт"

        lines.append(
            f"• {product_name}\n"
            f"  Артикул: {vendor_code or '-'}\n"
            f"  — {qty_int} шт • Покупка на сумму - {_rub(price_f)}\n"
            f"  {ost_line}"
        )

        total_qty += qty_int
        if price_f > 0:
            total_sum += price_f * qty_int

    body = (
        f"📦 Склад отгрузки: {warehouse or '-'}\n"
        + "\n".join(lines)
        + f"\nИтого позиций: {total_qty}\n"
        + f"Сумма: {_rub(total_sum)}\n"
        + f"ID: {oid}"
    )
    return f"{header}\n{body}".strip()


# -------------------------
# FBW: Statistics orders
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

    for r in data:
        if isinstance(r, dict):
            for k in ("warehouseName", "supplierArticle", "subject", "nmName", "category"):
                if k in r:
                    r[k] = fix_mojibake(_safe_str(r.get(k)))

    return data

def format_stats_order(o: Dict[str, Any]) -> str:
    warehouse = fix_mojibake(_safe_str(o.get("warehouseName") or o.get("warehouse") or o.get("officeName") or "WB"))

    nm_id_raw = o.get("nmId") or o.get("nmID") or o.get("nm_id")
    nm_id: Optional[int] = None
    if nm_id_raw is not None:
        try:
            nm_id = int(float(nm_id_raw))
        except Exception:
            nm_id = None

    barcode = _safe_str(o.get("barcode") or o.get("barCode") or "")
    supplier_article = fix_mojibake(_safe_str(o.get("supplierArticle") or o.get("vendorCode") or o.get("article") or ""))

    product_name = fix_mojibake(_safe_str(
        o.get("nmName")
        or o.get("productName")
        or o.get("subjectName")
        or o.get("subject")
        or "Товар"
    ))

    if nm_id:
        full_title = content_get_title(nm_id=nm_id, vendor_code=supplier_article)
        if full_title:
            product_name = full_title

    qty_raw = o.get("quantity") or o.get("qty") or 1
    try:
        qty = int(qty_raw)
    except Exception:
        qty = 1
    if qty <= 0:
        qty = 1

    price = (
        o.get("priceWithDisc")
        or o.get("finishedPrice")
        or o.get("forPay")
        or o.get("totalPrice")
        or o.get("price")
        or 0
    )

    # Отмена (statistics /supplier/orders)
    is_cancel = bool(o.get("isCancel") or o.get("is_cancel") or False)
    cancel_date = _format_dt_ru(_safe_str(o.get("cancelDate") or o.get("cancel_date") or ""))

        # Остатки (FBW/склады WB): суммарно по всем складам по barcode/nmId
    ostatok_line = "Остаток: -"
    q = fbw_stock_total(barcode, nm_id=nm_id, supplier_article=supplier_article)
    if isinstance(q, int):
        ostatok_line = f"Остаток: {q} шт"

    if is_cancel:
        header = f"❌ Отмена заказа со склада ({warehouse}) · {SHOP_NAME}"
    else:
        header = f"🏬 Заказ товара со склада ({warehouse}) · {SHOP_NAME}"

    cancel_line = f"Дата отмены: {cancel_date}\n" if (is_cancel and cancel_date) else ""

    body = (
        f"📦 Склад отгрузки: {warehouse}\n"
        f"• {product_name}\n"
        f"  Артикул WB: {nm_id or '-'}\n"
        f"  — {qty} шт • Покупка на сумму - {_rub(price)}\n"
        f"{cancel_line}"
        f"{ostatok_line}\n"
        f"Итого позиций: {qty}\n"
        f"Сумма: {_rub(price)}"
    )

    return f"{header}\n{body}".strip()


# -------------------------
# Questions (Q&A)
# -------------------------
def questions_fetch(is_answered: bool) -> List[Dict[str, Any]]:
    if not WB_FEEDBACKS_TOKEN:
        return []

    url = f"{WB_FEEDBACKS_BASE}/api/v1/questions"
    data = wb_get(
        url,
        WB_FEEDBACKS_TOKEN,
        params={
            "isAnswered": "true" if is_answered else "false",
            "take": 100,
            "skip": 0
        },
    )

    if isinstance(data, dict) and data.get("__error__"):
        return [{"__error__": True, **data, "__stage__": f"questions isAnswered={is_answered}"}]

    if isinstance(data, dict) and isinstance(data.get("data"), dict):
        qs = data["data"].get("questions", [])
        if isinstance(qs, list):
            return [q for q in qs if isinstance(q, dict)]

    return []

def format_question(q: Dict[str, Any]) -> str:
    qid = _safe_str(q.get("id"))
    text = fix_mojibake(_safe_str(q.get("text") or ""))
    created = _format_dt_ru(_safe_str(q.get("createdDate") or ""))

    pd = q.get("productDetails") if isinstance(q.get("productDetails"), dict) else {}
    nm_id_raw = pd.get("nmId")
    nm_id: Optional[int] = None
    try:
        nm_id = int(float(nm_id_raw)) if nm_id_raw is not None else None
    except Exception:
        nm_id = None

    product_name = fix_mojibake(_safe_str(pd.get("productName") or "Товар"))
    supplier_article = fix_mojibake(_safe_str(pd.get("supplierArticle") or ""))

    if nm_id:
        full_title = content_get_title(nm_id=nm_id, vendor_code=supplier_article)
        if full_title:
            product_name = full_title

    header = f"❓ Вопрос покупателя · {SHOP_NAME}"
    body = (
        f"Товар: {product_name}\n"
        f"Артикул WB: {nm_id or '-'}\n"
        f"Вопрос: {text}\n"
        f"Дата: {created}\n"
        f"ID: {qid}"
    )
    return f"{header}\n{body}".strip()

async def poll_questions_loop():
    while True:
        try:
            items = questions_fetch(is_answered=False)

            if items and isinstance(items[0], dict) and items[0].get("__error__"):
                it = items[0]
                ek = f"err:questions:{it.get('status_code')}:{it.get('__stage__','')}"
                if not was_sent(ek):
                    tg_send(f"⚠️ questions error: {it.get('status_code')} {it.get('response_text','')[:300]}")
                    mark_sent(ek)
            else:
                for q in items:
                    qid = _safe_str(q.get("id"))
                    if not qid:
                        continue
                    key = f"question:{qid}"
                    if was_sent(key):
                        continue
                    res = tg_send(format_question(q))
                    if res.get("ok"):
                        mark_sent(key)

        except Exception as e:
            ek = f"err:questions:{type(e).__name__}:{str(e)[:160]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка questions polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_QUESTIONS_SECONDS)


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
    product_name = fix_mojibake(_safe_str(f.get("productName") or f.get("nmName") or f.get("subjectName") or "Без названия"))
    article = fix_mojibake(_safe_str(f.get("supplierArticle") or f.get("vendorCode") or f.get("article") or f.get("nmId") or ""))
    text = fix_mojibake(_safe_str(f.get("text") or ""))

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


# -------------------------
# Statistics: sales (выкупы/возвраты) polling
# -------------------------
def stats_fetch_sales_since(cursor_name: str) -> List[Dict[str, Any]]:
    if not WB_STATS_TOKEN:
        return []

    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/sales"
    default_dt = msk_now() - timedelta(hours=4)
    cursor = get_cursor(cursor_name, iso_msk(default_dt))

    data = wb_get(url, WB_STATS_TOKEN, params={"dateFrom": cursor, "flag": 1})
    if isinstance(data, dict) and data.get("__error__"):
        return [{"__error__": True, **data}]

    if not isinstance(data, list) or not data:
        return []

    last = data[-1]
    if isinstance(last, dict) and last.get("lastChangeDate"):
        set_cursor(cursor_name, last["lastChangeDate"])

    for r in data:
        if isinstance(r, dict):
            for k in ("warehouseName", "supplierArticle", "subject", "nmName", "category"):
                if k in r:
                    r[k] = fix_mojibake(_safe_str(r.get(k)))

    return data

def format_sale_event(s: Dict[str, Any]) -> str:
    warehouse = fix_mojibake(_safe_str(s.get("warehouseName") or "WB"))

    nm_id = None
    for k in ("nmId", "nmID", "nm_id"):
        if s.get(k) is not None:
            try:
                nm_id = int(float(s.get(k)))
                break
            except Exception:
                pass

    supplier_article = fix_mojibake(_safe_str(s.get("supplierArticle") or ""))
    name = fix_mojibake(_safe_str(s.get("nmName") or s.get("subject") or "Товар"))

    if nm_id:
        t = content_get_title(nm_id=nm_id, vendor_code=supplier_article)
        if t:
            name = t

    price = s.get("forPay") or s.get("priceWithDisc") or s.get("finishedPrice") or 0
    try:
        price_f = float(price)
    except Exception:
        price_f = 0.0

    created = _format_dt_ru(_safe_str(s.get("date") or s.get("lastChangeDate") or ""))

    sale_id = _safe_str(s.get("saleID") or s.get("saleId") or "")
    is_return = (price_f < 0) or (sale_id.upper().startswith("R"))

    kind = "✅ Выкуп" if not is_return else "↩️ Возврат/отказ"
    return (
        f"{kind} · {SHOP_NAME}\n"
        f"Склад: {warehouse}\n"
        f"Товар: {name}\n"
        f"Артикул WB: {nm_id or '-'}\n"
        f"Сумма: {_rub(abs(price_f))}\n"
        f"Дата: {created}"
    ).strip()

async def poll_sales_loop():
    while True:
        try:
            rows = stats_fetch_sales_since("stats_sales_cursor")
            if rows and isinstance(rows[0], dict) and rows[0].get("__error__"):
                # 429 — это просто лимит. Не спамим в TG, просто подождём до следующего цикла.
                if int(rows[0].get("status_code") or 0) == 429:
                    pass
                else:
                    status = rows[0].get('status_code')
                    # 429/502/503/504 — это временные сбои/лимиты, не спамим в TG
                    if status not in (429, 502, 503, 504):
                        ek = f"err:stats_sales:{status}:{rows[0].get('url','')}"
                        if not was_sent(ek):
                            tg_send(f"⚠️ statistics sales error: {status} {rows[0].get('response_text','')[:300]}")
                            mark_sent(ek)
            else:
                for s in rows:
                    if not isinstance(s, dict):
                        continue
                    sid = _safe_str(s.get("saleID") or s.get("saleId") or "")
                    key = f"sale:{sid}:{_safe_str(s.get('lastChangeDate'))}:{_safe_str(s.get('srid'))}:{_safe_str(s.get('barcode'))}"
                    if was_sent(key):
                        continue
                    res = tg_send(format_sale_event(s))
                    if res.get("ok"):
                        mark_sent(key)
        except Exception as e:
            ek = f"err:sales:{type(e).__name__}:{str(e)[:160]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка sales polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_SALES_SECONDS)



def _msk_day_start_iso(day: datetime) -> str:
    """Старт суток в МСК в формате RFC3339 для Statistics API."""
    # МСК = UTC+3 без переходов
    return day.strftime("%Y-%m-%dT00:00:00+03:00")

# -------------------------
# Daily summary (orders + buyouts + returns)
# -------------------------
def _price_from_row(row: Dict[str, Any]) -> float:
    for k in ("forPay", "priceWithDisc", "finishedPrice", "totalPrice", "price"):
        if row.get(k) is not None:
            try:
                return float(row.get(k))
            except Exception:
                continue
    return 0.0

def daily_summary_text(today: datetime) -> str:
    if not WB_STATS_TOKEN:
        return f"⚠️ Суточная сводка: нет WB_STATS_TOKEN · {SHOP_NAME}"

    day_str = today.strftime("%Y-%m-%d")
    date_from = _msk_day_start_iso(today)

    # 1) Оформленные заказы — supplier/orders
    orders_url = f"{WB_STATISTICS_BASE}/api/v1/supplier/orders"
    orders = wb_get(orders_url, WB_STATS_TOKEN, params={"dateFrom": date_from, "flag": 1})

    sold_qty = 0
    sold_sum = 0.0
    cancel_qty = 0
    cancel_sum = 0.0

    if isinstance(orders, list):
        for o in orders:
            if not isinstance(o, dict):
                continue
            qty_raw = o.get("quantity") or o.get("qty") or 1
            try:
                qty = int(qty_raw)
            except Exception:
                qty = 1
            if qty <= 0:
                qty = 1

            p = max(0.0, _price_from_row(o))
            is_cancel = bool(o.get("isCancel") or o.get("is_cancel") or False)
            if is_cancel:
                cancel_qty += qty
                cancel_sum += p
            else:
                sold_qty += qty
                sold_sum += p
    elif isinstance(orders, dict) and orders.get("__error__"):
        return f"⚠️ Суточная сводка: не удалось получить orders (status {orders.get('status_code')}) · {SHOP_NAME}"

    # 2) Выкупы + возвраты — supplier/sales?flag=1
    sales_url = f"{WB_STATISTICS_BASE}/api/v1/supplier/sales"
    sales = wb_get(sales_url, WB_STATS_TOKEN, params={"dateFrom": date_from, "flag": 1})

    buyouts_qty = 0
    buyouts_sum = 0.0
    returns_qty = 0
    returns_sum = 0.0

    if isinstance(sales, list):
        for s in sales:
            if not isinstance(s, dict):
                continue
            qty_raw = s.get("quantity") or s.get("qty") or 1
            try:
                qty = int(qty_raw)
            except Exception:
                qty = 1
            if qty <= 0:
                qty = 1

            p = _price_from_row(s)
            sale_id = _safe_str(s.get("saleID") or s.get("saleId") or "")
            is_return = (p < 0) or (sale_id.upper().startswith("R"))
            if is_return:
                returns_qty += qty
                returns_sum += abs(p)
            else:
                buyouts_qty += qty
                buyouts_sum += max(0.0, p)
    elif isinstance(sales, dict) and sales.get("__error__"):
        return f"⚠️ Суточная сводка: не удалось получить sales (status {sales.get('status_code')}) · {SHOP_NAME}"

    msg = (
        f"📊 Итоги дня за {day_str} (МСК) · {SHOP_NAME}\n"
        f"🛒 Товаров продано на сумму: {_rub(sold_sum)}\n"
        f"   Кол-во товаров: {sold_qty} шт\n"
        f"✅ Выкуп товаров произведен на сумму: {_rub(buyouts_sum)}\n"
        f"   Кол-во выкупленных: {buyouts_qty} шт\n"
        f"❌ Отменено/аннулировано на сумму: {_rub(cancel_sum)}\n"
        f"   Кол-во отменённых: {cancel_qty} шт\n"
        f"↩️ Отказы/возвраты на сумму: {_rub(returns_sum)}\n"
        f"   Кол-во отказов/возвратов: {returns_qty} шт"
    )

    return msg.strip()


# -------------------------
# Poll loops
# -------------------------
async def poll_marketplace_loop():
    while True:
        try:
            orders = mp_fetch_new_orders()
            for kind, o in orders:
                if DEBUG_RAW_ORDERS:
                    debug_key = f"debug:raw:{kind}:{o.get('_id','')}"
                    if not was_sent(debug_key):
                        tg_send("DEBUG RAW ORDER:\n" + json.dumps(o, ensure_ascii=False, indent=2)[:3500])
                        mark_sent(debug_key)

                key = f"mp:{kind}:{o.get('_id','')}"
                if was_sent(key):
                    continue

                res = tg_send(format_mp_order(kind, o))
                if res.get("ok"):
                    mark_sent(key)
        except Exception as e:
            ek = f"err:mp:{type(e).__name__}:{str(e)[:160]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка marketplace polling: {e}")
                mark_sent(ek)

        await asyncio.sleep(POLL_FBS_SECONDS)

async def poll_fbw_loop():
    while True:
        try:
            rows = stats_fetch_orders_since("stats_orders_cursor")
            if rows and isinstance(rows[0], dict) and rows[0].get("__error__"):
                if int(rows[0].get("status_code") or 0) == 429:
                    pass
                else:
                    ek = f"err:stats_orders:{rows[0].get('status_code')}:{rows[0].get('url','')}"
                    if not was_sent(ek):
                        tg_send(f"⚠️ statistics orders error: {rows[0].get('status_code')} {rows[0].get('response_text','')[:300]}")
                        mark_sent(ek)
            else:
                for o in rows:
                    if not isinstance(o, dict) or not o.get("srid"):
                        continue
                    key = f"stats:order:{o.get('srid','')}:{o.get('lastChangeDate','')}"
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

async def daily_summary_loop():
    while True:
        try:
            now = msk_now()
            target = now.replace(hour=DAILY_SUMMARY_HOUR_MSK, minute=DAILY_SUMMARY_MINUTE_MSK, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)

            await asyncio.sleep((target - now).total_seconds())

            day_key = f"daily:{target.strftime('%Y-%m-%d')}"
            if not was_sent(day_key):
                tg_send(daily_summary_text(target))
                mark_sent(day_key)
        except Exception as e:
            ek = f"err:daily:{type(e).__name__}:{str(e)[:160]}"
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка суточной сводки: {e}")
                mark_sent(ek)


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

@app.get("/poll-once")
def poll_once():
    result: Dict[str, Any] = {}
    if WB_MP_TOKEN:
        try:
            orders = mp_fetch_new_orders()
            result["marketplace_found"] = len(orders)
        except Exception as e:
            result["marketplace_error"] = str(e)
    else:
        result["marketplace"] = "no WB_MP_TOKEN"
    return result

@app.get("/ping-content")
def ping_content():
    if not WB_CONTENT_TOKEN:
        return {"ok": False, "error": "WB_CONTENT_TOKEN is not set"}
    return wb_get("https://content-api.wildberries.ru/ping", WB_CONTENT_TOKEN)

@app.get("/test-title/{nm_id}")
def test_title(nm_id: int):
    return {"nm_id": nm_id, "title": content_get_title(nm_id=nm_id, vendor_code="")}

@app.get("/mp-warehouses")
def mp_warehouses():
    if not WB_MP_TOKEN:
        return {"ok": False, "error": "no WB_MP_TOKEN"}
    return wb_get(f"{WB_MARKETPLACE_BASE}/api/v3/warehouses", WB_MP_TOKEN)

@app.get("/test-fbw-stocks")
def test_fbw_stocks():
    if not WB_STATS_TOKEN:
        return {"ok": False, "error": "no WB_STATS_TOKEN"}
    date_from = datetime.utcnow().strftime("%Y-%m-%d")
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/stocks"
    return wb_get(url, WB_STATS_TOKEN, params={"dateFrom": date_from})

@app.get("/clear-cache")
def clear_cache():
    _TITLE_CACHE.clear()
    _SIZES_CACHE.clear()
    global _FBW_STOCKS_CACHE
    _FBW_STOCKS_CACHE = (0.0, [])
    return {"ok": True, "cache": "cleared"}

@app.get("/test-questions")
def test_questions():
    if not WB_FEEDBACKS_TOKEN:
        return {"ok": False, "error": "no WB_FEEDBACKS_TOKEN"}

    url = f"{WB_FEEDBACKS_BASE}/api/v1/questions"
    out = {
        "isAnswered=false": wb_get(url, WB_FEEDBACKS_TOKEN, params={"isAnswered": "false", "take": 20, "skip": 0}),
        "isAnswered=true": wb_get(url, WB_FEEDBACKS_TOKEN, params={"isAnswered": "true", "take": 20, "skip": 0}),
    }
    return out

@app.get("/debug-title/{nm_id}")
def debug_title(nm_id: int):
    raw = content_get_title(nm_id=nm_id, vendor_code="")
    return {
        "nm_id": nm_id,
        "raw": raw,
        "fixed": fix_mojibake(raw),
        "raw_repr": repr(raw),
    }


# -------------------------
# Startup
# -------------------------
@app.on_event("startup")
async def startup():
    _ = db()
    # ⚠️ В startup нельзя делать блокирующие HTTP-запросы — Render ждёт открытый порт только после завершения startup().
    # Все сетевые "прогревы" и уведомления запускаем в фоне.
    asyncio.create_task(asyncio.to_thread(prime_feedbacks_silently))

    if not DISABLE_STARTUP_HELLO:
        asyncio.create_task(asyncio.to_thread(
            tg_send,
            "✅ WB→Telegram запущен. Жду заказы (FBS/DBS/DBW), FBW (с задержкой), выкупы и отзывы/вопросы."
        ))

    asyncio.create_task(poll_marketplace_loop())
    asyncio.create_task(poll_sales_loop())
    asyncio.create_task(poll_feedbacks_loop())
    asyncio.create_task(poll_fbw_loop())
    asyncio.create_task(daily_summary_loop())
    asyncio.create_task(poll_questions_loop())
