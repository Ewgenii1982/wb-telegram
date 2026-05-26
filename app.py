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

# Base URLs (override via Render env if needed)
WB_MARKETPLACE_BASE = os.getenv("WB_MARKETPLACE_BASE", "https://marketplace-api.wildberries.ru").rstrip("/")
WB_STATISTICS_BASE  = os.getenv("WB_STATISTICS_BASE",  "https://statistics-api.wildberries.ru").rstrip("/")
WB_FEEDBACKS_BASE   = os.getenv("WB_FEEDBACKS_BASE",   "https://feedbacks-api.wildberries.ru").rstrip("/")


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
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/sales"
    params = {"dateFrom": rfc3339_msk_start_of_day(date_from), "flag": 1}
    return http_get_json(url, stats_headers(), params=params) or []


def get_orders(date_from: datetime) -> List[Dict[str, Any]]:
    if not WB_STATS_TOKEN:
        raise RuntimeError("WB_STATS_TOKEN missing")
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/orders"
    params = {"dateFrom": rfc3339_msk_start_of_day(date_from), "flag": 1}
    return http_get_json(url, stats_headers(), params=params) or []


def get_stocks_full(date_from: datetime) -> List[Dict[str, Any]]:
    if not WB_STATS_TOKEN:
        raise RuntimeError("WB_STATS_TOKEN missing")
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/stocks"
    params = {"dateFrom": rfc3339_msk_start_of_day(date_from)}
    return http_get_json(url, stats_headers(), params=params) or []


def get_feedbacks_page(is_answered: bool, take: int = 100, skip: int = 0) -> List[Dict[str, Any]]:
    if not WB_FEEDBACKS_TOKEN:
        raise RuntimeError("WB_FEEDBACKS_TOKEN missing")
    url = f"{WB_FEEDBACKS_BASE}/api/v1/feedbacks"
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
    url = f"{WB_FEEDBACKS_BASE}/api/v1/questions"
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



# ---------------------------
# Background Worker entrypoint (Render Background Worker)
# ---------------------------

async def run_worker() -> None:
    # Initialize DB and prime feedbacks in background (never block)
    asyncio.create_task(asyncio.to_thread(db))
    asyncio.create_task(asyncio.to_thread(prime_feedbacks_silently))

    # Start loops
    asyncio.create_task(poll_marketplace_loop())
    asyncio.create_task(poll_sales_loop())
    asyncio.create_task(poll_feedbacks_loop())
    asyncio.create_task(poll_fbw_loop())
    asyncio.create_task(daily_summary_loop())
    asyncio.create_task(poll_questions_loop())

    if not DISABLE_STARTUP_HELLO:
        asyncio.create_task(asyncio.to_thread(
            tg_send,
            "✅ WB→Telegram запущен (Worker)."
        ))

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(run_worker())
