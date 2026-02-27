# main.py
import os
import json
import time
import asyncio
import sqlite3
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import ORJSONResponse

app = FastAPI(default_response_class=ORJSONResponse)

# -------------------------
# Config (ENV)
# -------------------------
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

WB_MP_TOKEN = os.getenv("WB_MP_TOKEN", "").strip()               # marketplace-api (FBS/DBS/DBW)
WB_STATS_TOKEN = os.getenv("WB_STATS_TOKEN", "").strip()         # statistics-api (FBW)
WB_FEEDBACKS_TOKEN = os.getenv("WB_FEEDBACKS_TOKEN", "").strip() # feedbacks-api (reviews + questions)
WB_WEBHOOK_SECRET = os.getenv("WB_WEBHOOK_SECRET", "").strip()

WB_CONTENT_TOKEN = os.getenv("WB_CONTENT_TOKEN", "").strip()     # Content API (title, sizes)

SHOP_NAME = os.getenv("SHOP_NAME", "Bright Shop").strip()
DB_PATH = os.getenv("DB_PATH", "/tmp/wb_telegram.sqlite").strip()

POLL_FBS_SECONDS = int(os.getenv("POLL_FBS_SECONDS", "20"))
POLL_FEEDBACKS_SECONDS = int(os.getenv("POLL_FEEDBACKS_SECONDS", "60"))
POLL_QUESTIONS_SECONDS = int(os.getenv("POLL_QUESTIONS_SECONDS", "60"))
POLL_FBW_SECONDS = int(os.getenv("POLL_FBW_SECONDS", "1800"))

# polling выкупов/продаж (statistics/sales)
POLL_SALES_SECONDS = int(os.getenv("POLL_SALES_SECONDS", "120"))

DAILY_SUMMARY_HOUR_MSK = int(os.getenv("DAILY_SUMMARY_HOUR_MSK", "23"))
DAILY_SUMMARY_MINUTE_MSK = int(os.getenv("DAILY_SUMMARY_MINUTE_MSK", "55"))

DISABLE_STARTUP_HELLO = os.getenv("DISABLE_STARTUP_HELLO", "0").strip() == "1"

# Остатки на складе продавца (FBS/DBS/DBW)
SELLER_WAREHOUSE_ID = os.getenv("SELLER_WAREHOUSE_ID", "").strip()

# DEBUG RAW JSON заказов (в TG) — включай только временно
DEBUG_RAW_ORDERS = os.getenv("DEBUG_RAW_ORDERS", "0").strip() == "1"

# WB base URLs
WB_MARKETPLACE_BASE = "https://marketplace-api.wildberries.ru"
WB_STATISTICS_BASE = "https://statistics-api.wildberries.ru"
WB_FEEDBACKS_BASE = "https://feedbacks-api.wildberries.ru"
WB_CONTENT_BASE = "https://content-api.wildberries.ru"
# HTTP статусы/ошибки, которые не стоит слать в TG (временные проблемы WB/сети)
TRANSIENT_HTTP_STATUSES = {429, 502, 503, 504}



# -------------------------
# Helpers: text/format
# -------------------------
def _safe_str(x) -> str:
    return "" if x is None else str(x).strip()

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


def _err_key(prefix: str, e: Exception) -> str:
    """Делаем стабильный ключ ошибок, чтобы не спамить в TG из‑за адресов объектов в тексте исключения."""
    try:
        if isinstance(e, requests.exceptions.RequestException):
            return f"{prefix}:{type(e).__name__}"
    except Exception:
        pass
    return f"{prefix}:{type(e).__name__}:{str(e)[:160]}"

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

def _wb_request_with_429_retry(method: str, url: str, headers: dict, params: Optional[dict], json_payload: Optional[dict], timeout: int) -> requests.Response:
    """Один запрос к WB с простыми ретраями на 429/502/503/504 и сетевые таймауты.
    Важно: НЕ падаем исключением наружу.
    """
    attempts = 0
    backoff = 2
    last_exc: Optional[Exception] = None

    while attempts < 3:
        attempts += 1
        try:
            if method == "GET":
                r = requests.get(url, headers=headers, params=params, timeout=timeout)
            else:
                r = requests.post(url, headers=headers, params=params, json=json_payload, timeout=timeout)
        except requests.exceptions.RequestException as e:
            last_exc = e
            time.sleep(min(backoff, 15))
            backoff *= 2
            continue

        # 429: ждём сколько сказали (если есть), иначе чуть-чуть
        if r.status_code == 429:
            retry = r.headers.get("X-Ratelimit-Retry")
            try:
                wait_s = int(float(retry)) if retry is not None else backoff
            except Exception:
                wait_s = backoff
            time.sleep(max(1, min(wait_s, 30)))
            backoff *= 2
            continue

        # временные 5xx — чуть подождём и повторим
        if r.status_code in (502, 503, 504):
            time.sleep(min(backoff, 15))
            backoff *= 2
            continue

        return r

    # если так и не получилось — пробрасываем как псевдо-ответ через исключение выше уровнем
    if last_exc is not None:
        raise last_exc
    return r  # type: ignore


def wb_get(url: str, token: str, params: Optional[dict] = None, timeout: int = 25) -> Any:
    headers = {"Authorization": token}
    try:
        r = _wb_request_with_429_retry("GET", url, headers=headers, params=params, json_payload=None, timeout=timeout)
    except requests.exceptions.RequestException as e:
        return {
            "__error__": True,
            "status_code": 0,
            "url": url,
            "response_text": str(e),
            "error_type": type(e).__name__,
        }

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

def wb_post(url: str, token: str, payload: dict, params: Optional[dict] = None, timeout: int = 25) -> Any:
    headers = {"Authorization": token}
    try:
        r = _wb_request_with_429_retry("POST", url, headers=headers, params=params, json_payload=payload, timeout=timeout)
    except requests.exceptions.RequestException as e:
        return {
            "__error__": True,
            "status_code": 0,
            "url": url,
            "response_text": str(e),
            "error_type": type(e).__name__,
        }

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

    # Остаток продавца: если SELLER_WAREHOUSE_ID не задан — суммируем по всем складам продавца
    stocks_map: Dict[int, int] = {}
    if chrt_ids:
        stocks_map = mp_get_inventory_total(chrt_ids)

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

        ost_line = "Остаток продавца: -"
        if cid_int and cid_int in stocks_map:
            ost_line = f"Остаток продавца: {stocks_map[cid_int]} шт"

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

    ostatok_line = "Остаток: -"
    q = fbw_stock_quantity(warehouse, barcode, nm_id=nm_id, supplier_article=supplier_article)
    if isinstance(q, int):
        ostatok_line = f"Остаток: {q} шт"

    header = f"🏬 Заказ товара со склада ({warehouse}) · {SHOP_NAME}"

    body = (
        f"📦 Склад отгрузки: {warehouse}\n"
        f"• {product_name}\n"
        f"  Артикул WB: {nm_id or '-'}\n"
        f"  — {qty} шт • Покупка на сумму - {_rub(price)}\n"
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

    kind = "✅ Выкуп" if price_f >= 0 else "↩️ Возврат/отказ"
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
                status = rows[0].get('status_code')
                if isinstance(status, int) and (status in TRANSIENT_HTTP_STATUSES or status == 0):
                    # 429/5xx/таймауты — временно, не спамим TG
                    pass
                else:
                    ek = f"err:stats_sales:{rows[0].get('status_code')}:{rows[0].get('url','')}"
                    if not was_sent(ek):
                        tg_send(f"⚠️ statistics sales error: {rows[0].get('status_code')} {rows[0].get('response_text','')[:300]}")
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


# -------------------------
# Daily summary (заказы + выкупы + возвраты)
# -------------------------
def _sum_orders_for_day(day_msk: datetime) -> Tuple[int, float]:
    """
    'Продажи' как 'заказы' из supplier/orders за сутки (МСК).
    """
    if not WB_STATS_TOKEN:
        return (0, 0.0)

    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/orders"
    day_start = day_msk.replace(hour=0, minute=0, second=0, microsecond=0)
    data = wb_get(url, WB_STATS_TOKEN, params={"dateFrom": iso_msk(day_start)})

    if not isinstance(data, list):
        return (0, 0.0)

    cnt = 0
    sm = 0.0
    for o in data:
        if not isinstance(o, dict):
            continue
        # ограничим только этой датой (на всякий)
        d = _safe_str(o.get("date") or o.get("lastChangeDate") or "")
        if d and not d.startswith(day_start.strftime("%Y-%m-%d")):
            continue

        price = (
            o.get("priceWithDisc")
            or o.get("finishedPrice")
            or o.get("forPay")
            or o.get("totalPrice")
            or o.get("price")
            or 0
        )
        try:
            price_f = float(price)
        except Exception:
            price_f = 0.0

        qty = o.get("quantity") or o.get("qty") or 1
        try:
            qty_i = int(qty)
        except Exception:
            qty_i = 1
        if qty_i <= 0:
            qty_i = 1

        cnt += qty_i
        if price_f > 0:
            sm += price_f * qty_i

    return (cnt, sm)

def daily_summary_text(today: datetime) -> str:
    """Суточная сводка (МСК).
    Разделяем:
    - Продажи (регистрация продаж)
    - Выкупы (оплата/выкуп)
    - Возвраты/отказы
    Примечание: по данным /supplier/sales, различаем по полю paymentSaleId (если есть) и знаку forPay.
    """
    if not WB_STATS_TOKEN:
        return f"⚠️ Суточная сводка: нет WB_STATS_TOKEN · {SHOP_NAME}"

    day_str = today.strftime("%Y-%m-%d")
    url = f"{WB_STATISTICS_BASE}/api/v1/supplier/sales"

    data = wb_get(url, WB_STATS_TOKEN, params={"dateFrom": day_str, "flag": 1})
    if isinstance(data, dict) and data.get("__error__"):
        # 429/502 и т.п. — просто пропускаем сводку, чтобы не засорять TG.
        try:
            sc = int(data.get("status_code") or 0)
        except Exception:
            sc = 0
        if sc in TRANSIENT_HTTP_STATUSES:
            return ""
        return f"⚠️ Суточная сводка: ошибка statistics sales {sc} · {SHOP_NAME}"

    if not isinstance(data, list):
        return f"⚠️ Суточная сводка: нет данных · {SHOP_NAME}"

    sales_cnt = 0
    sales_sum = 0.0

    buyouts_cnt = 0
    buyouts_sum = 0.0

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

        # возвраты/отказы часто идут с отрицательным forPay
        if price_f < 0:
            returns_cnt += 1
            returns_sum += abs(price_f)
            continue

        # эвристика: paymentSaleId присутствует -> выкуп (оплата)
        pay_id = row.get("paymentSaleId") or row.get("paymentSaleID") or row.get("paymentSale_id")
        is_buyout = False
        if pay_id not in (None, "", 0, "0"):
            is_buyout = True

        if is_buyout:
            buyouts_cnt += 1
            buyouts_sum += price_f
        else:
            sales_cnt += 1
            sales_sum += price_f

    return (
        f"📊 Суточная сводка за {day_str} (МСК) · {SHOP_NAME}\n"
        f"Продажи: {sales_cnt} шт · {sales_sum:.2f} ₽\n"
        f"Выкупы: {buyouts_cnt} шт · {buyouts_sum:.2f} ₽\n"
        f"Возвраты/отказы: {returns_cnt} шт · {returns_sum:.2f} ₽\n"
        f"Отзывы: см. уведомления (если были — ты их получил)"
    ).strip()

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
            # Временные сетевые проблемы WB (таймауты/соединение) — в TG не шлём, просто попробуем позже.
            # Если вдруг вылетает НЕ сетевое (логика/код) — сообщим 1 раз.
            if isinstance(e, requests.exceptions.RequestException):
                pass
            else:
                ek = _err_key("err:mp", e)
                if not was_sent(ek):
                    tg_send(f"⚠️ Ошибка marketplace (код): {type(e).__name__} — проверь логи")
                    mark_sent(ek)

        await asyncio.sleep(POLL_FBS_SECONDS)

async def poll_fbw_loop():
    while True:
        try:
            rows = stats_fetch_orders_since("stats_orders_cursor")
            if rows and isinstance(rows[0], dict) and rows[0].get("__error__"):
                status = rows[0].get('status_code')
                if isinstance(status, int) and (status in TRANSIENT_HTTP_STATUSES or status == 0):
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
            ek = _err_key("err:stats", e)
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка statistics polling: {type(e).__name__}")
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
            ek = _err_key("err:feedbacks", e)
            if not was_sent(ek):
                tg_send(f"⚠️ Ошибка feedbacks polling: {type(e).__name__}")
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
                txt = daily_summary_text(target)
                if txt:
                    tg_send(txt)
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
    prime_feedbacks_silently()

    asyncio.create_task(poll_marketplace_loop())
    asyncio.create_task(poll_sales_loop())
    asyncio.create_task(poll_feedbacks_loop())
    asyncio.create_task(poll_fbw_loop())
    asyncio.create_task(daily_summary_loop())
    asyncio.create_task(poll_questions_loop())

    if not DISABLE_STARTUP_HELLO:
        tg_send("✅ WB→Telegram запущен. Жду заказы (FBS/DBS/DBW), FBW (с задержкой), выкупы и отзывы/вопросы.")
