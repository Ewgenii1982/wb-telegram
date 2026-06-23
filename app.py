# app.py — WB -> Telegram (ONLY FBS orders + Daily report + FBS stock in order notification)
# Render: use Background Worker
# Start Command: python app.py

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

# ---------- Base URLs ----------
WB_MARKETPLACE_BASE = os.getenv("WB_MARKETPLACE_BASE", "https://marketplace-api.wildberries.ru").rstrip("/")
WB_STATISTICS_BASE = os.getenv("WB_STATISTICS_BASE", "https://statistics-api.wildberries.ru").rstrip("/")
WB_CONTENT_BASE = os.getenv("WB_CONTENT_BASE", "https://content-api.wildberries.ru").rstrip("/")
TG_API_BASE = "https://api.telegram.org"

# ---------- Required ENV ----------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
WB_MP_TOKEN = os.getenv("WB_MP_TOKEN", "").strip()              # Marketplace API token: FBS orders + seller stocks
WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()        # Statistics API token: daily report

# ---------- Optional ENV ----------
WB_CONTENT_TOKEN = os.getenv("WB_CONTENT_TOKEN", "").strip()    # Optional: product names / barcode->chrtId fallback
SELLER_WAREHOUSE_ID = os.getenv("SELLER_WAREHOUSE_ID", "").strip()
SHOP_NAME = os.getenv("SHOP_NAME", "Магазин").strip()
DB_PATH = (os.getenv("DB_PATH", "/tmp/wb_bot.sqlite").strip() or "/tmp/wb_bot.sqlite")
POLL_FBS_SECONDS = int(os.getenv("POLL_FBS_SECONDS", "30"))
DAILY_SUMMARY_HOUR_MSK = int(os.getenv("DAILY_SUMMARY_HOUR_MSK", "23"))
DAILY_SUMMARY_MINUTE_MSK = int(os.getenv("DAILY_SUMMARY_MINUTE_MSK", "50"))
DISABLE_STARTUP_HELLO = os.getenv("DISABLE_STARTUP_HELLO", "0").strip().lower() in ("1", "true", "yes")

MSK = ZoneInfo("Europe/Moscow")

# ---------- small in-memory caches ----------
_STOCK_CACHE: Dict[Tuple[int, int], Tuple[float, int]] = {}  # (warehouse_id, chrt_id) -> (expires_ts, amount)
_CONTENT_SIZES_CACHE: Dict[int, Tuple[float, List[Dict[str, Any]]]] = {}  # nmId -> (expires_ts, sizes)
_PRODUCT_NAME_CACHE: Dict[int, Tuple[float, str]] = {}
STOCK_CACHE_TTL_SECONDS = int(os.getenv("STOCK_CACHE_TTL_SECONDS", "20"))
CONTENT_CACHE_TTL_SECONDS = int(os.getenv("CONTENT_CACHE_TTL_SECONDS", "21600"))  # 6 hours


# ---------------- SQLite ----------------
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
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        return
    url = f"{TG_API_BASE}/bot{TG_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TG_CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        requests.post(url, json=payload, timeout=(5, 30))
    except Exception:
        pass


# ---------------- HTTP helpers ----------------
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


# ---------------- WB headers ----------------
def mp_headers() -> Dict[str, str]:
    return {"Authorization": WB_MP_TOKEN}


def stats_headers() -> Dict[str, str]:
    return {"Authorization": WB_STATS_TOKEN}


def content_headers() -> Dict[str, str]:
    token = WB_CONTENT_TOKEN or WB_MP_TOKEN
    return {"Authorization": token}


# ---------------- WB API calls ----------------
def mp_get_new_fbs_orders() -> List[Dict[str, Any]]:
    url = f"{WB_MARKETPLACE_BASE}/api/v3/orders/new"
    data = http_json("GET", url, headers=mp_headers())
    if isinstance(data, dict) and "_http_status" in data:
        raise RuntimeError(f"marketplace http {data.get('_http_status')}: {str(data.get('_text'))[:300]}")
    if isinstance(data, dict):
        orders = data.get("orders")
        if isinstance(orders, list):
            return orders
    return data if isinstance(data, list) else []


def seller_stock_amount(warehouse_id: int, chrt_id: int) -> Optional[int]:
    if not warehouse_id or not chrt_id or not WB_MP_TOKEN:
        return None

    cache_key = (warehouse_id, chrt_id)
    cached = _STOCK_CACHE.get(cache_key)
    if cached and time.time() < cached[0]:
        return cached[1]

    url = f"{WB_MARKETPLACE_BASE}/api/v3/stocks/{warehouse_id}"
    data = http_json("POST", url, headers=mp_headers(), json_body={"chrtIds": [chrt_id]}, max_tries=4)
    if isinstance(data, dict) and "_http_status" in data:
        return None

    stocks = data.get("stocks") if isinstance(data, dict) else data
    if isinstance(stocks, list):
        for s in stocks:
            if int_safe(s.get("chrtId") or s.get("chrtID")) == chrt_id:
                amount = int_safe(s.get("amount") or s.get("quantity") or 0)
                _STOCK_CACHE[cache_key] = (time.time() + STOCK_CACHE_TTL_SECONDS, amount)
                return amount
    return None


def content_get_cards_by_nm(nm_ids: List[int]) -> List[Dict[str, Any]]:
    """Best-effort Content API call. Used only for names and barcode->chrtId fallback."""
    if not nm_ids or not (WB_CONTENT_TOKEN or WB_MP_TOKEN):
        return []
    url = f"{WB_CONTENT_BASE}/content/v2/get/cards/list"
    body = {
        "settings": {
            "cursor": {"limit": 100},
            "filter": {"withPhoto": -1, "nmID": nm_ids},
        }
    }
    data = http_json("POST", url, headers=content_headers(), json_body=body, max_tries=3)
    if isinstance(data, dict) and "_http_status" in data:
        return []
    cards = data.get("cards") if isinstance(data, dict) else []
    return cards if isinstance(cards, list) else []


def content_get_sizes(nm_id: int) -> List[Dict[str, Any]]:
    if not nm_id:
        return []
    cached = _CONTENT_SIZES_CACHE.get(nm_id)
    if cached and time.time() < cached[0]:
        return cached[1]

    cards = content_get_cards_by_nm([nm_id])
    sizes: List[Dict[str, Any]] = []
    name = ""
    if cards:
        card = cards[0]
        name = str(card.get("title") or card.get("subjectName") or card.get("vendorCode") or "").strip()
        raw_sizes = card.get("sizes")
        if isinstance(raw_sizes, list):
            sizes = raw_sizes

    if name:
        _PRODUCT_NAME_CACHE[nm_id] = (time.time() + CONTENT_CACHE_TTL_SECONDS, name)
    _CONTENT_SIZES_CACHE[nm_id] = (time.time() + CONTENT_CACHE_TTL_SECONDS, sizes)
    return sizes


def product_name_from_content(nm_id: int) -> str:
    if not nm_id:
        return ""
    cached = _PRODUCT_NAME_CACHE.get(nm_id)
    if cached and time.time() < cached[0]:
        return cached[1]
    content_get_sizes(nm_id)
    cached = _PRODUCT_NAME_CACHE.get(nm_id)
    return cached[1] if cached else ""


def _datefrom_for_day(day_msk: datetime) -> str:
    return day_msk.astimezone(MSK).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()


def stats_orders_for_day(day_msk: datetime) -> List[Dict[str, Any]]:
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/orders"
    params = {"dateFrom": _datefrom_for_day(day_msk), "flag": 1}
    data = http_json("GET", url, headers=stats_headers(), params=params)
    if isinstance(data, dict) and "_http_status" in data:
        raise RuntimeError(f"statistics orders http {data.get('_http_status')}: {str(data.get('_text'))[:300]}")
    return data if isinstance(data, list) else []


def stats_sales_for_day(day_msk: datetime) -> List[Dict[str, Any]]:
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/sales"
    params = {"dateFrom": _datefrom_for_day(day_msk), "flag": 1}
    data = http_json("GET", url, headers=stats_headers(), params=params)
    if isinstance(data, dict) and "_http_status" in data:
        raise RuntimeError(f"statistics sales http {data.get('_http_status')}: {str(data.get('_text'))[:300]}")
    return data if isinstance(data, list) else []


# ---------------- utils ----------------
def int_safe(v: Any, default: int = 0) -> int:
    try:
        if v in (None, ""):
            return default
        return int(float(str(v).replace(",", ".")))
    except Exception:
        return default


def money_safe(v: Any) -> float:
    try:
        if v in (None, ""):
            return 0.0
        if isinstance(v, (int, float)):
            return float(v)
        return float(str(v).replace(",", ".").strip())
    except Exception:
        return 0.0


def best_sum(o: Dict[str, Any], keys: Iterable[str]) -> float:
    for k in keys:
        if k in o and o[k] not in (None, ""):
            val = money_safe(o.get(k))
            if val != 0:
                return val
    for k in keys:
        if k in o:
            return money_safe(o.get(k))
    return 0.0


def fmt_money(v: float) -> str:
    return f"{v:.2f} ₽" if abs(v - round(v)) > 0.001 else f"{int(round(v))} ₽"


def resolve_order_id(order: Dict[str, Any]) -> str:
    for k in ("id", "orderId", "wbOrderId", "rid", "srid"):
        val = order.get(k)
        if val not in (None, ""):
            return str(val)
    return str(hash(json.dumps(order, ensure_ascii=False, sort_keys=True)))


def order_items(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    items = order.get("items")
    if isinstance(items, list) and items:
        return [it for it in items if isinstance(it, dict)]
    return [order]


def skus_text(it: Dict[str, Any]) -> str:
    skus = it.get("skus") or it.get("barcodes") or it.get("barcode") or it.get("sku")
    if isinstance(skus, list):
        return ", ".join(str(x) for x in skus if x is not None)
    return str(skus or "").strip()


def first_sku(it: Dict[str, Any]) -> str:
    skus = it.get("skus") or it.get("barcodes") or it.get("barcode") or it.get("sku")
    if isinstance(skus, list):
        return str(skus[0]) if skus else ""
    return str(skus or "").strip()


def resolve_chrt_id(it: Dict[str, Any]) -> int:
    direct = int_safe(it.get("chrtId") or it.get("chrtID"))
    if direct:
        return direct

    nm_id = int_safe(it.get("nmId") or it.get("nmID"))
    barcode = first_sku(it)
    if not nm_id or not barcode:
        return 0

    # fallback: Content sizes contains chrtID/chrtId and skus list
    for s in content_get_sizes(nm_id):
        s_skus = s.get("skus") or []
        if barcode and str(barcode) in [str(x) for x in s_skus]:
            return int_safe(s.get("chrtId") or s.get("chrtID"))
    return 0


def resolve_warehouse_id(order: Dict[str, Any], it: Dict[str, Any]) -> int:
    return int_safe(
        it.get("warehouseId") or it.get("warehouseID") or order.get("warehouseId") or order.get("warehouseID") or SELLER_WAREHOUSE_ID
    )


def product_name(it: Dict[str, Any]) -> str:
    for k in ("name", "subject", "subjectName", "title", "goodsName"):
        val = str(it.get(k) or "").strip()
        if val:
            return val
    nm_id = int_safe(it.get("nmId") or it.get("nmID"))
    name = product_name_from_content(nm_id)
    if name:
        return name
    article = str(it.get("supplierArticle") or it.get("article") or it.get("vendorCode") or "").strip()
    return article or "Товар"


def order_price(it: Dict[str, Any]) -> float:
    # WB often sends price in kopecks in Marketplace orders.
    raw = best_sum(it, ["convertedPrice", "price", "priceWithDisc", "totalPrice", "forPay"])
    if raw > 100000:  # very likely kopecks
        return raw / 100.0
    # For marketplace order price can also be like 36400 for 364 ₽; divide if no decimal and suspiciously high.
    if raw >= 10000:
        return raw / 100.0
    return raw


def order_qty(it: Dict[str, Any]) -> int:
    qty = int_safe(it.get("quantity") or it.get("qty") or it.get("count") or 1, 1)
    return max(1, qty)


# ---------------- formatting ----------------
def fmt_fbs_order(order: Dict[str, Any]) -> str:
    oid = resolve_order_id(order)
    created = str(order.get("createdAt") or order.get("dateCreated") or order.get("created") or "").strip()
    wh_name = str(order.get("warehouseName") or order.get("warehouse") or "").strip()

    items = order_items(order)
    total_qty = 0
    total_sum = 0.0

    lines: List[str] = [f"🏬 Новый заказ FBS · {SHOP_NAME}"]
    if wh_name:
        lines.append(f"📦 Склад отгрузки: {wh_name}")
    if oid:
        lines.append(f"Заказ: {oid}")
    if created:
        lines.append(f"Дата: {created}")
    lines.append("")

    for it in items:
        qty = order_qty(it)
        price = order_price(it)
        line_sum = price * qty if price else 0.0
        total_qty += qty
        total_sum += line_sum

        nm_id = int_safe(it.get("nmId") or it.get("nmID"))
        chrt_id = resolve_chrt_id(it)
        warehouse_id = resolve_warehouse_id(order, it)
        stock = seller_stock_amount(warehouse_id, chrt_id) if warehouse_id and chrt_id else None
        stock_text = str(stock) if stock is not None else "-"

        lines.append(f"• {product_name(it)}")
        if nm_id:
            lines.append(f"  Артикул WB: {nm_id}")
        article = str(it.get("supplierArticle") or it.get("article") or it.get("vendorCode") or "").strip()
        if article:
            lines.append(f"  Артикул продавца: {article}")
        sku = skus_text(it)
        if sku:
            lines.append(f"  SKU/Баркод: {sku}")
        lines.append(f"  — {qty} шт" + (f" • {fmt_money(line_sum)}" if line_sum else ""))
        lines.append(f"  Остаток FBS: {stock_text}")

    lines.append("")
    lines.append(f"Итого позиций: {total_qty}")
    if total_sum:
        lines.append(f"Сумма: {fmt_money(total_sum)}")
    return "\n".join(lines)


def fmt_daily_report(day_msk: datetime, orders: List[Dict[str, Any]], sales: List[Dict[str, Any]]) -> str:
    sold_count = sold_sum = 0.0
    cancel_count = cancel_sum = 0.0

    for o in orders:
        is_cancel = bool(o.get("isCancel"))
        qty = money_safe(o.get("quantity") or 1) or 1
        price = best_sum(o, ["totalPrice", "priceWithDisc", "forPay", "price"])
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
        qty = money_safe(s.get("quantity") or 1) or 1
        amount = best_sum(s, ["forPay", "priceWithDisc", "totalPrice", "price"])
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
            f"🛒 Продано (заказы): {int(sold_count)} шт • {fmt_money(sold_sum)}",
            f"✅ Выкуп: {int(buy_count)} шт • {fmt_money(buy_sum)}",
            f"❌ Отмены: {int(cancel_count)} шт • {fmt_money(cancel_sum)}",
            f"↩️ Возвраты: {int(ret_count)} шт • {fmt_money(ret_sum)}",
        ]
    )


# ---------------- loops ----------------
async def poll_fbs_loop(conn: sqlite3.Connection) -> None:
    if not WB_MP_TOKEN:
        await asyncio.to_thread(tg_send, "⚠️ WB_MP_TOKEN не задан — уведомления по FBS-заказам отключены.")
        return

    while True:
        try:
            orders = await asyncio.to_thread(mp_get_new_fbs_orders)
            for o in orders:
                oid = resolve_order_id(o)
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


# ---------------- worker entrypoint ----------------
async def run_worker() -> None:
    conn = await asyncio.to_thread(db)

    if not DISABLE_STARTUP_HELLO:
        asyncio.create_task(asyncio.to_thread(
            tg_send,
            f"✅ WB→Telegram запущен (только FBS-заказы + итоги по магазину). {SHOP_NAME}",
        ))

    asyncio.create_task(poll_fbs_loop(conn))
    asyncio.create_task(daily_summary_loop(conn))

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(run_worker())
