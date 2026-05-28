# app.py — WB → Telegram
# Notifications: ONLY FBS orders (Marketplace API)
# Daily report: ALL sales/orders of the shop (Statistics API) — like before
# Deploy as Render Background Worker. Start Command: python app.py

from __future__ import annotations

import asyncio
import json
import os
import random
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from zoneinfo import ZoneInfo

# --- Base URLs (can be overridden via ENV, but defaults are correct) ---
WB_MARKETPLACE_BASE = os.getenv("WB_MARKETPLACE_BASE", "https://marketplace-api.wildberries.ru").rstrip("/")
WB_STATISTICS_BASE = os.getenv("WB_STATISTICS_BASE", "https://statistics-api.wildberries.ru").rstrip("/")
TG_API_BASE = "https://api.telegram.org"

# --- Required ENV ---
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

WB_MP_TOKEN = os.getenv("WB_MP_TOKEN", "").strip()        # Marketplace API token (FBS)
WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()  # Statistics API token (daily report)

# --- Optional ENV ---
SHOP_NAME = os.getenv("SHOP_NAME", "Магазин").strip()

DB_PATH = (os.getenv("DB_PATH", "/tmp/wb_bot.sqlite").strip() or "/tmp/wb_bot.sqlite")

POLL_FBS_SECONDS = int(os.getenv("POLL_FBS_SECONDS", "30"))

DAILY_SUMMARY_HOUR_MSK = int(os.getenv("DAILY_SUMMARY_HOUR_MSK", "23"))
DAILY_SUMMARY_MINUTE_MSK = int(os.getenv("DAILY_SUMMARY_MINUTE_MSK", "50"))

DISABLE_STARTUP_HELLO = os.getenv("DISABLE_STARTUP_HELLO", "0").strip().lower() in ("1", "true", "yes")

MSK = ZoneInfo("Europe/Moscow")


# ---------------- SQLite (dedup + kv) ----------------
def _ensure_parent_dir(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def db() -> sqlite3.Connection:
    _ensure_parent_dir(DB_PATH)
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("CREATE TABLE IF NOT EXISTS seen_fbs_orders (order_id TEXT PRIMARY KEY, seen_at TEXT NOT NULL)")
    conn.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL)")
    conn.commit()
    return conn


def kv_get(conn: sqlite3.Connection, k: str, default: str = "") -> str:
    row = conn.execute("SELECT v FROM kv WHERE k=? LIMIT 1", (k,)).fetchone()
    return row[0] if row else default


def kv_set(conn: sqlite3.Connection, k: str, v: str) -> None:
    conn.execute("INSERT OR REPLACE INTO kv(k,v) VALUES(?,?)", (k, v))
    conn.commit()


def seen_order(conn: sqlite3.Connection, order_id: str) -> bool:
    return conn.execute("SELECT 1 FROM seen_fbs_orders WHERE order_id=? LIMIT 1", (order_id,)).fetchone() is not None


def mark_seen(conn: sqlite3.Connection, order_id: str) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO seen_fbs_orders(order_id, seen_at) VALUES(?,?)",
        (order_id, datetime.now(tz=MSK).isoformat()),
    )
    conn.commit()


# ---------------- Telegram ----------------
def tg_send(text: str) -> None:
    """Never raises (so it can't crash the worker)."""
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    url = f"{TG_API_BASE}/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        requests.post(url, json=payload, timeout=(5, 30))
    except Exception:
        pass


# ---------------- HTTP helpers (backoff + cooldown) ----------------
@dataclass
class Cooldown:
    until_ts: float = 0.0


_HOST_COOLDOWN: Dict[str, Cooldown] = {}


def _sleep_with_jitter(base: float) -> None:
    time.sleep(base + random.random() * min(1.5, max(0.2, base * 0.2)))


def _set_cooldown(url: str, seconds: float) -> None:
    host = requests.utils.urlparse(url).netloc
    _HOST_COOLDOWN[host] = Cooldown(until_ts=time.time() + seconds)


def _respect_cooldown(url: str) -> None:
    host = requests.utils.urlparse(url).netloc
    cd = _HOST_COOLDOWN.get(host)
    if cd and time.time() < cd.until_ts:
        _sleep_with_jitter(max(0.0, cd.until_ts - time.time()))


def http_json(
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    timeout: Tuple[float, float] = (8, 40),
    max_tries: int = 6,
) -> Any:
    last_err: Optional[Exception] = None
    for attempt in range(1, max_tries + 1):
        _respect_cooldown(url)
        try:
            resp = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=timeout)

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else min(120.0, 5.0 * attempt)
                _set_cooldown(url, wait)
                _sleep_with_jitter(wait)
                continue

            if resp.status_code in (502, 503, 504):
                wait = min(180.0, 8.0 * attempt)
                _set_cooldown(url, wait)
                _sleep_with_jitter(wait)
                continue

            if resp.status_code >= 400:
                return {"_http_status": resp.status_code, "_text": resp.text}

            if resp.text.strip().startswith("<"):
                wait = min(180.0, 8.0 * attempt)
                _set_cooldown(url, wait)
                _sleep_with_jitter(wait)
                continue

            return resp.json()

        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            last_err = e
            _sleep_with_jitter(min(120.0, 2.0 ** attempt))
        except Exception as e:
            last_err = e
            _sleep_with_jitter(min(30.0, 2.0 ** attempt))

    raise last_err or RuntimeError("http_json failed")


# ---------------- WB API ----------------
def mp_headers() -> Dict[str, str]:
    return {"Authorization": WB_MP_TOKEN}


def stats_headers() -> Dict[str, str]:
    return {"Authorization": WB_STATS_TOKEN}


def mp_get_new_fbs_orders() -> List[Dict[str, Any]]:
    url = f"{WB_MARKETPLACE_BASE}/api/v3/orders/new"
    data = http_json("GET", url, headers=mp_headers())
    if isinstance(data, dict) and "_http_status" in data:
        raise RuntimeError(f"marketplace http {data.get('_http_status')}: {str(data.get('_text'))[:200]}")
    if isinstance(data, dict) and isinstance(data.get("orders"), list):
        return data["orders"]
    return data if isinstance(data, list) else []


def _datefrom_for_day(day_msk: datetime) -> str:
    return day_msk.astimezone(MSK).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def stats_orders_for_day(day_msk: datetime) -> List[Dict[str, Any]]:
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/orders"
    params = {"dateFrom": _datefrom_for_day(day_msk), "flag": 1}
    data = http_json("GET", url, headers=stats_headers(), params=params)
    if isinstance(data, dict) and "_http_status" in data:
        raise RuntimeError(f"statistics orders http {data.get('_http_status')}: {str(data.get('_text'))[:200]}")
    return data if isinstance(data, list) else []


def stats_sales_for_day(day_msk: datetime) -> List[Dict[str, Any]]:
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/sales"
    params = {"dateFrom": _datefrom_for_day(day_msk), "flag": 1}
    data = http_json("GET", url, headers=stats_headers(), params=params)
    if isinstance(data, dict) and "_http_status" in data:
        raise RuntimeError(f"statistics sales http {data.get('_http_status')}: {str(data.get('_text'))[:200]}")
    return data if isinstance(data, list) else []


# ---------------- Formatting helpers ----------------
def _money(v: Any) -> float:
    try:
        if v is None:
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        return float(str(v).replace(",", ".").strip())
    except Exception:
        return 0.0


def _best_sum(o: Dict[str, Any], keys: Iterable[str]) -> float:
    for k in keys:
        if k in o and o[k] not in (None, ""):
            val = _money(o.get(k))
            if val != 0:
                return val
    for k in keys:
        if k in o:
            return _money(o.get(k))
    return 0.0


def fmt_fbs_order(order: Dict[str, Any]) -> str:
    oid = str(order.get("id") or order.get("orderId") or order.get("wbOrderId") or "").strip()
    created = str(order.get("createdAt") or order.get("dateCreated") or "").strip()
    wh = str(order.get("warehouseName") or order.get("warehouse") or "").strip()

    items = order.get("items") if isinstance(order.get("items"), list) else []

    lines: List[str] = [f"🧾 Новый заказ FBS · {SHOP_NAME}"]
    if oid:
        lines.append(f"Заказ: {oid}")
    if wh:
        lines.append(f"Склад: {wh}")
    if created:
        lines.append(f"Дата: {created}")

    if items:
        lines.append("")
        lines.append("Товары:")
        for it in items[:50]:
            name = (it.get("name") or it.get("subject") or it.get("supplierArticle") or "").strip()
            nm = it.get("nmId") or it.get("nmID") or it.get("article") or ""
            qty = int(_money(it.get("quantity") or it.get("count") or 1) or 1)
            price = _best_sum(it, ["priceWithDisc", "price", "totalPrice", "forPay"])
            s = f"• {name}" if name else "• Товар"
            if nm:
                s += f" (nmId: {nm})"
            s += f" — {qty} шт"
            if price:
                s += f" • {price:.2f} ₽"
            lines.append(s)

    return "\n".join(lines)


def fmt_daily_report(day_msk: datetime, orders: List[Dict[str, Any]], sales: List[Dict[str, Any]]) -> str:
    sold_count = sold_sum = 0.0
    cancel_count = cancel_sum = 0.0

    for o in orders:
        is_cancel = bool(o.get("isCancel"))
        qty = _money(o.get("quantity") or 1) or 1
        price = _best_sum(o, ["totalPrice", "priceWithDisc", "forPay", "price"])
        if is_cancel:
            cancel_count += qty
            cancel_sum += price
        else:
            sold_count += qty
            sold_sum += price

    buy_count = buy_sum = 0.0
    ret_count = ret_sum = 0.0

    for s in sales:
        sale_id = str(s.get("saleID") or s.get("saleId") or s.get("srid") or "")
        qty = _money(s.get("quantity") or 1) or 1
        amount = _best_sum(s, ["forPay", "priceWithDisc", "totalPrice", "price"])
        is_return = sale_id.startswith("R") or amount < 0
        if is_return:
            ret_count += qty
            ret_sum += abs(amount)
        else:
            buy_count += qty
            buy_sum += amount

    d_str = day_msk.astimezone(MSK).strftime("%d.%m.%Y")
    return "\n".join(
        [
            f"📊 Итоги дня · {SHOP_NAME}",
            f"Дата: {d_str}",
            "",
            f"🛒 Продано (заказы): {int(sold_count)} шт • {sold_sum:.2f} ₽",
            f"✅ Выкуп: {int(buy_count)} шт • {buy_sum:.2f} ₽",
            f"❌ Отмены: {int(cancel_count)} шт • {cancel_sum:.2f} ₽",
            f"↩️ Возвраты: {int(ret_count)} шт • {ret_sum:.2f} ₽",
        ]
    )


# ---------------- Loops ----------------
async def poll_fbs_loop(conn: sqlite3.Connection) -> None:
    if not WB_MP_TOKEN:
        await asyncio.to_thread(tg_send, "⚠️ WB_MP_TOKEN не задан — уведомления по заказам FBS отключены.")
        return

    while True:
        try:
            orders = await asyncio.to_thread(mp_get_new_fbs_orders)
            for o in orders:
                oid = str(o.get("id") or o.get("orderId") or o.get("wbOrderId") or "").strip()
                if not oid:
                    oid = str(hash(json.dumps(o, ensure_ascii=False, sort_keys=True)))
                if seen_order(conn, oid):
                    continue
                await asyncio.to_thread(tg_send, fmt_fbs_order(o))
                mark_seen(conn, oid)
        except Exception as e:
            await asyncio.to_thread(tg_send, f"⚠️ FBS polling error: {e}")

        await asyncio.sleep(POLL_FBS_SECONDS + random.random() * 2.0)


def _next_summary_dt(now_msk: datetime) -> datetime:
    target = now_msk.astimezone(MSK).replace(
        hour=DAILY_SUMMARY_HOUR_MSK,
        minute=DAILY_SUMMARY_MINUTE_MSK,
        second=0,
        microsecond=0,
    )
    if target <= now_msk:
        target += timedelta(days=1)
    return target


async def daily_summary_loop(conn: sqlite3.Connection) -> None:
    if not WB_STATS_TOKEN:
        await asyncio.to_thread(tg_send, "⚠️ WB_STATS_TOKEN не задан — суточный отчёт отключён.")
        return

    while True:
        now = datetime.now(tz=MSK)
        nxt = _next_summary_dt(now)
        await asyncio.sleep(max(1.0, (nxt - now).total_seconds()))

        day = datetime.now(tz=MSK)
        day_key = day.strftime("%Y-%m-%d")
        if kv_get(conn, "daily_sent", "") == day_key:
            continue

        try:
            orders = await asyncio.to_thread(stats_orders_for_day, day)
            sales = await asyncio.to_thread(stats_sales_for_day, day)
            await asyncio.to_thread(tg_send, fmt_daily_report(day, orders, sales))
            kv_set(conn, "daily_sent", day_key)
        except Exception as e:
            await asyncio.to_thread(tg_send, f"⚠️ Daily report error: {e}")


# ---------------- Worker entrypoint ----------------
async def run_worker() -> None:
    conn = await asyncio.to_thread(db)

    if not DISABLE_STARTUP_HELLO:
        asyncio.create_task(
            asyncio.to_thread(
                tg_send,
                f"✅ WB→Telegram запущен (уведомления: FBS; итоги: все продажи/выкупы). {SHOP_NAME}",
            )
        )

    asyncio.create_task(poll_fbs_loop(conn))
    asyncio.create_task(daily_summary_loop(conn))

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(run_worker())
