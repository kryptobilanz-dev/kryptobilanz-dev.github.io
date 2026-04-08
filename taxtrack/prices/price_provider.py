# taxtrack/prices/price_provider.py
from __future__ import annotations

import os
import sqlite3
import json
import time
import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, Optional, Tuple, List

from ._versioning import LOGIC_REV
from .provider_master import price_provider as _hybrid_provider
from .token_mapper import map_token

# ============================================================
# CONFIG
# ============================================================

CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

DB_PATH = os.path.join(CACHE_DIR, "eur_price_cache.sqlite")

HISTORIC_FREEZE_DAYS = 3  # ab diesem Alter: "historic_final"

DEFAULT_TTLS = {
    "recent": 7 * 24 * 3600,           # 7 Tage
    "historic_final": 365 * 24 * 3600  # quasi ewig
}

# ============================================================
# SQLITE INIT
# ============================================================

def _connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS eur_prices (
            key TEXT PRIMARY KEY,
            symbol TEXT,
            date TEXT,
            source TEXT,
            payload TEXT,
            fetched_at INTEGER,
            ttl INTEGER
        );
    """)
    return conn

# ============================================================
# KEY / CHECKSUM
# ============================================================

def _stable_key(payload: Dict[str, Any]) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _ts_to_date(ts: int) -> str:
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")

# ============================================================
# IN-MEMORY-CACHE
# ============================================================

_memory: Dict[str, Tuple[float, int, Dict[str, Any]]] = {}
# key -> (fetched_at_ts, ttl, payload)

def _memory_get(key: str) -> Optional[Dict[str, Any]]:
    now = time.time()
    entry = _memory.get(key)
    if not entry:
        return None
    fetched_at, ttl, payload = entry
    if ttl and (now - fetched_at) > ttl:
        _memory.pop(key, None)
        return None
    return payload


def _memory_put(key: str, payload: Dict[str, Any], ttl: int):
    _memory[key] = (time.time(), ttl, payload)

# ============================================================
# DISK-CACHE
# ============================================================

def _disk_get(key: str) -> Optional[Dict[str, Any]]:
    now = int(time.time())
    with _connect() as c:
        row = c.execute(
            "SELECT payload, fetched_at, ttl FROM eur_prices WHERE key = ?",
            (key,)
        ).fetchone()

    if not row:
        return None

    payload_json, fetched_at, ttl = row
    if ttl and (now - fetched_at) > ttl:
        # abgelaufen -> löschen
        with _connect() as c:
            c.execute("DELETE FROM eur_prices WHERE key = ?", (key,))
        return None

    return json.loads(payload_json)


def _disk_put(
    key: str,
    symbol: str,
    date_str: str,
    source: str,
    payload: Dict[str, Any],
    ttl: int,
):
    now = int(time.time())
    with _connect() as c:
        c.execute(
            """
            INSERT INTO eur_prices(key, symbol, date, source, payload, fetched_at, ttl)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(key) DO UPDATE SET
              payload=excluded.payload,
              fetched_at=excluded.fetched_at,
              ttl=excluded.ttl
            """,
            (
                key,
                symbol,
                date_str,
                source,
                json.dumps(payload, separators=(",", ":")),
                now,
                ttl,
            ),
        )

# ============================================================
# QUERY-MODELL
# ============================================================

@dataclass(frozen=True)
class PriceQuery:
    """
    Preisabfrage für EUR-Preise auf Tagesbasis
    (Token + Unix-Timestamp → Tag wird intern normalisiert).
    """
    symbol: str           # Rohsymbol (z.B. "WETH", "ATOM", "USDT")
    ts: int               # Unix-Timestamp (Sekunden, UTC)
    source: str = "hybrid_eur"
    quote_ccy: str = "EUR"
    policy: Optional[str] = None  # z.B. "historic_final"
    chain: Optional[str] = None   # optional, für Debug-Logging (z.B. "avax", "eth")


def _build_key(q: PriceQuery) -> str:
    base_symbol = map_token(q.symbol)
    date_str = _ts_to_date(q.ts)

    payload = {
        "symbol": base_symbol,
        "date": date_str,
        "source": q.source,
        "quote": q.quote_ccy,
        "policy": q.policy,
        "logic_rev": LOGIC_REV,
    }
    return _stable_key(payload)


def _decide_ttl(q: PriceQuery) -> int:
    if q.policy == "historic_final":
        return DEFAULT_TTLS["historic_final"]
    return DEFAULT_TTLS["recent"]

# ============================================================
# PROVIDER-ROUTING (INTERN: HYBRID EUR PROVIDER)
# ============================================================

def _fetch_from_source(q: PriceQuery) -> Dict[str, Any]:
    """
    Ruft den echten Preislieferanten auf.
    Priorität: HybridPriceProvider (CSV + Yahoo + Binance + Kraken) → CoinGecko fallback.
    """
    base_symbol = map_token(q.symbol)
    date_str = _ts_to_date(q.ts)

    # 1) Hybrid EUR pipeline
    print(f"[PRICE FETCH] {base_symbol} date={date_str} trying hybrid_eur")
    eur_price = _hybrid_provider.get_eur_price(base_symbol, q.ts)
    eur_price = float(eur_price) if eur_price is not None else None
    if eur_price is not None and eur_price <= 0:
        # Never treat 0 as a valid price
        eur_price = None
    source = "hybrid_eur"
    meta_note = "CSV/Yahoo/Binance/Kraken pipeline"

    # 2) Fallback: CoinGecko when hybrid returns 0 (avoids duplicate calls via cache)
    if eur_price is None:
        print(f"[PRICE FETCH] {base_symbol} date={date_str} hybrid=0, trying coingecko fallback")
        try:
            from .coingecko_price_provider import get_eur_price_fallback
            cg_price = get_eur_price_fallback(base_symbol, q.ts)
            if cg_price is not None and cg_price > 0:
                eur_price = float(cg_price)
                source = "coingecko"
                meta_note = "CoinGecko fallback (missing in hybrid)"
                print(f"[PRICE FETCH] {base_symbol} date={date_str} coingecko price={eur_price}")
        except Exception as e:
            print(f"[PRICE FETCH] {base_symbol} date={date_str} coingecko failed: {e}")

    return {
        "symbol": base_symbol,
        "raw_symbol": q.symbol,
        "date": date_str,
        "ts": q.ts,
        "price": eur_price,
        "quote": q.quote_ccy,
        "source": source,
        "logic_rev": LOGIC_REV,
        "policy": q.policy,
        "fetched_ts": int(time.time()),
        "meta": {
            "provider": source,
            "note": meta_note
        },
    }

# ============================================================
# PUBLIC API
# ============================================================

def _log_price_resolved(info: Dict[str, Any], ts_fallback: int) -> None:
    """Debug log for every resolved price."""
    symbol = info.get("symbol") or info.get("raw_symbol") or ""
    date_str = info.get("date") or _ts_to_date(ts_fallback)
    source = info.get("source") or "unknown"
    price = info.get("price")
    if price is not None and float(price) > 0:
        print(f"[PRICE DEBUG] {symbol} {date_str} source={source} price={price}")


def get_price(q: PriceQuery) -> Dict[str, Any]:
    """
    Höchste Ebene: RAM → Disk → Hybrid-Provider.
    Liefert immer ein Dict mit 'price' in EUR.
    """
    symbol_display = map_token(q.symbol)
    date_str = _ts_to_date(q.ts)
    chain_str = q.chain or ""
    print(f"[PRICE REQUEST] {symbol_display} timestamp={q.ts} date={date_str} chain={chain_str}")

    key = _build_key(q)
    ttl = _decide_ttl(q)

    # 1) RAM
    hit = _memory_get(key)
    if hit:
        _log_price_resolved(hit, q.ts)
        return hit

    # 2) Disk
    hit = _disk_get(key)
    if hit:
        _memory_put(key, hit, ttl)
        _log_price_resolved(hit, q.ts)
        return hit

    # 3) Provider
    fresh = _fetch_from_source(q)
    if "price" not in fresh:
        raise ValueError(f"Kein Preisfeld in Providerantwort für {q}")

    # If missing, try nearby timestamps (cache-first) before giving up.
    if fresh.get("price") is None:
        base_symbol = map_token(q.symbol)
        # try +/- 1..3 days
        offsets: List[int] = [86400, -86400, 2 * 86400, -2 * 86400, 3 * 86400, -3 * 86400]
        for off in offsets:
            q2 = PriceQuery(symbol=q.symbol, ts=int(q.ts) + off, policy=q.policy, chain=q.chain)
            try:
                alt = _fetch_from_source(q2)
            except Exception:
                alt = None
            if alt and alt.get("price") is not None:
                fresh = dict(fresh)
                fresh["price"] = alt["price"]
                fresh["source"] = alt.get("source") or fresh.get("source")
                fresh.setdefault("meta", {})
                fresh["meta"]["derived_from_nearby_ts"] = q2.ts
                print(f"[PRICE WARN] {base_symbol} date={date_str} missing, derived from nearby ts={q2.ts} price={fresh['price']}")
                break
        if fresh.get("price") is None:
            print(f"[PRICE WARN] {symbol_display} date={date_str} missing price (None)")

    _log_price_resolved(fresh, q.ts)
    _disk_put(key, fresh["symbol"], fresh["date"], fresh["source"], fresh, ttl)
    _memory_put(key, fresh, ttl)
    return fresh


def get_eur_price(
    symbol: str,
    ts: int,
    policy: Optional[str] = None,
    chain: Optional[str] = None,
) -> float | None:
    """
    Bequemer Wrapper für dein bestehendes System:
    - symbol: Rohsymbol aus Transaktion
    - ts: Unix-Timestamp (Sekunden)
    - policy: optional, z.B. "historic_final"
    - chain: optional, für Debug-Logging (z.B. "avax", "eth")

    Rückgabe: EUR-Preis (float)
    """
    sym = (symbol or "").strip().upper()
    if sym == "USD":
        # Base currency anchor: USD is exactly 1.0 EUR-equivalent.
        print("[PRICE BASE] USD base currency applied (1.0)")
        return 1.0

    # Auto-Freeze: alles älter als HISTORIC_FREEZE_DAYS → historic_final
    if policy is None:
        age_days = (time.time() - ts) / 86400.0
        if age_days >= HISTORIC_FREEZE_DAYS:
            policy = "historic_final"

    q = PriceQuery(symbol=symbol, ts=ts, policy=policy, chain=chain)
    info = get_price(q)
    p = info.get("price")
    return float(p) if p is not None else None


def invalidate(q: PriceQuery):
    """
    Manuelles Invalidieren eines Eintrags.
    """
    key = _build_key(q)
    _memory.pop(key, None)
    with _connect() as c:
        c.execute("DELETE FROM eur_prices WHERE key = ?", (key,))
