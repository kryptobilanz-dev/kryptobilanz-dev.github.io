# taxtrack/prices/coingecko_price_provider.py
"""
CoinGecko fallback for tokens missing EUR prices from the hybrid provider.

- Fallback lookup when hybrid returns 0.
- In-memory cache to avoid duplicate API calls for the same (symbol, date).
- Supports ERC20 and common tokens via built-in map + optional token_price_mapping.json.
- Dynamic symbol → CoinGecko id resolution via /search when not covered by static maps.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

# Pfad zur Mapping-Datei (Symbol → Coingecko-ID)
_CONFIG_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "config")
CONFIG_PATH = os.path.join(_CONFIG_DIR, "token_price_mapping.json")

_COINGECKO_BASE = "https://api.coingecko.com/api/v3"
_DYNAMIC_CONFIG_WRITES = (
    os.getenv("TAXTRACK_ALLOW_DYNAMIC_CONFIG_WRITES", "").strip().lower() in {"1", "true", "yes", "on"}
)
_ALLOW_DYNAMIC_CONFIG_WRITES = _DYNAMIC_CONFIG_WRITES
if not _DYNAMIC_CONFIG_WRITES:
    print("[DETERMINISTIC_MODE] dynamic writes disabled")

# Built-in symbol → CoinGecko id (common + ERC20). Avoids requiring mapping file for basics.
_BUILTIN_SYMBOL_TO_ID: Dict[str, str] = {
    "ETH": "ethereum",
    "WETH": "weth",
    "BTC": "bitcoin",
    "SOL": "solana",
    "AVAX": "avalanche-2",
    "MATIC": "matic-network",
    "POL": "matic-network",
    "ARB": "arbitrum",
    "OP": "optimism",
    "BNB": "binancecoin",
    "FTM": "fantom",
    "ATOM": "cosmos",
    "DOT": "polkadot",
    "ADA": "cardano",
    "PENDLE": "pendle",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "AAVE": "aave",
    "MKR": "maker",
    "CRV": "curve-dao-token",
    "LDO": "lido-dao",
    "RETH": "rocket-pool-eth",
    "STETH": "staked-ether",
    "USDT": "tether",
    "USDC": "usd-coin",
    "DAI": "dai",
    "TIA": "celestia",
    "INJ": "injective-protocol",
    "OSMO": "osmosis",
    "JITO": "jito-governance-token",
    "SAGA": "saga-2",
    "SUI": "sui",
    "AKT": "akash-network",
    "STARS": "stargaze",
    "STRD": "stride",
    "DOGE": "dogecoin",
    "XLM": "stellar",
    "XTZ": "tezos",
    "NEAR": "near",
    "GRT": "the-graph",
    "FET": "fetch-ai",
    "MON": "mon",
    "TARA": "taraxa",
}

_mapping_lock = threading.Lock()

# In-memory symbol → id (builtin + file + session dynamic)
_SYMBOL_TO_ID: Dict[str, str] = {}

# Negative cache: upper symbol -> expiry unix time (no id found this session)
_NEGATIVE_ID_CACHE: Dict[str, float] = {}
_NEGATIVE_TTL_SEC = 3600.0

# Session log of dynamically resolved symbols (for diagnostics)
_DYNAMIC_RESOLVED_SESSION: List[str] = []

# In-memory cache: (symbol, date_str) -> (price, fetched_at) to avoid duplicate API calls
_CG_MEMORY_CACHE: Dict[tuple, tuple] = {}
_CG_CACHE_TTL = 3600  # 1 hour in-process

# Dedup search API calls within process
_SEARCH_MEMO: Dict[str, Optional[str]] = {}


def _load_mapping() -> Dict[str, str]:
    """Load token_price_mapping.json; merge with built-in (file overrides)."""
    out = dict(_BUILTIN_SYMBOL_TO_ID)
    if not os.path.exists(CONFIG_PATH):
        return out
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        for k, v in data.items():
            out[k.upper().strip()] = str(v).strip().lower()
    except Exception:
        pass
    return out


def _reload_symbol_table() -> None:
    global _SYMBOL_TO_ID
    _SYMBOL_TO_ID = _load_mapping()


# Initial load
_reload_symbol_table()


def get_dynamic_resolutions_session() -> List[str]:
    """Copy of session log entries like 'ZRO -> layerzero' (diagnostics)."""
    return list(_DYNAMIC_RESOLVED_SESSION)


def clear_dynamic_resolution_log() -> None:
    _DYNAMIC_RESOLVED_SESSION.clear()


def _get_coingecko_id_static(symbol: str) -> Optional[str]:
    """Return CoinGecko id from builtin + file only (no network)."""
    sym = (symbol or "").strip().upper()
    return _SYMBOL_TO_ID.get(sym)


def _is_blocked_symbol_for_dynamic(base: str) -> bool:
    """
    Do not run /search for UNKNOWN, stables mapped elsewhere, LP/vault-like symbols.
    """
    u = (base or "").strip().upper()
    if not u:
        return True
    if u in ("UNKNOWN", "ERC-20"):
        return True
    if u.startswith("ERC20"):
        return True
    # Too short / noisy for high-confidence match
    if len(u) < 2:
        return True
    if u.isdigit():
        return True
    # USD anchor handled by hybrid / stables
    if u == "USD":
        return True

    # LP / vault / pair heuristics (do not auto-resolve)
    if "MOO" in u:
        return True
    if "_LPT" in u or u.endswith("LPT") or ("LP" in u and "PENDLE" in u):
        return True
    if u.count("-") >= 2:
        return True
    if "-" in u and any(x in u for x in ("WETH", "USDC", "USDT", "WBTC", "DAI", "FRAX")):
        return True
    if re.search(r"\bLP\b", u) or "LIQUIDITY" in u:
        return True

    return False


def _is_pool_like_coin(coin: Dict[str, Any]) -> bool:
    """Reject search hits that look like AMM LP / pool receipt tokens."""
    name = (coin.get("name") or "").upper()
    cid = (coin.get("id") or "").lower()
    sym = (coin.get("symbol") or "").upper()
    if "LIQUIDITY" in name and "POOL" in name:
        return True
    if "UNI-V" in sym or "SLP" in sym or "Cake-LP" in name:
        return True
    if "-LP" in cid or cid.endswith("-lp"):
        return True
    if "PANCAKESWAP LP" in name or "UNISWAP V2" in name:
        return True
    return False


def _rank_sort_key(coin: Dict[str, Any]) -> Tuple[int, int]:
    """Lower market_cap_rank first; unranked last."""
    r = coin.get("market_cap_rank")
    if r is None:
        return (1, 999999)
    try:
        return (0, int(r))
    except Exception:
        return (1, 999999)


def _pick_best_search_coin(coins: List[Dict[str, Any]], query_upper: str) -> Optional[Dict[str, Any]]:
    """
    Prefer exact symbol match, then id match, then exact name match.
    Filters pool-like results. Uses market_cap_rank for tie-break (high confidence).
    """
    filtered = [c for c in coins if isinstance(c, dict) and not _is_pool_like_coin(c)]
    if not filtered:
        return None

    qu = query_upper.strip().upper()

    # Tier 1: exact symbol (best market_cap_rank first)
    exact_sym = [c for c in filtered if (c.get("symbol") or "").upper() == qu]
    if exact_sym:
        exact_sym.sort(key=_rank_sort_key)
        return exact_sym[0]

    # Tier 2: id equals query (e.g. query 'ethereum')
    id_match = [c for c in filtered if (c.get("id") or "").lower() == qu.lower()]
    if id_match:
        id_match.sort(key=_rank_sort_key)
        return id_match[0]

    # Tier 3: exact name (case-insensitive)
    name_match = [c for c in filtered if (c.get("name") or "").upper() == qu]
    if name_match:
        name_match.sort(key=_rank_sort_key)
        return name_match[0]

    return None


def _search_coingecko(query: str, max_retries: int = 2) -> List[Dict[str, Any]]:
    url = f"{_COINGECKO_BASE}/search"
    params = {"query": query.strip()}
    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(2.0 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json() or {}
            return list(data.get("coins") or [])
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(1.0 * (attempt + 1))
    return []


def _persist_new_mapping(symbol_upper: str, cg_id: str) -> None:
    """Append to token_price_mapping.json without overriding existing keys."""
    if not _DYNAMIC_CONFIG_WRITES:
        return
    if symbol_upper in _BUILTIN_SYMBOL_TO_ID:
        return
    key_lower = symbol_upper.lower().strip()
    with _mapping_lock:
        data: Dict[str, str] = {}
        if os.path.exists(CONFIG_PATH):
            try:
                with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except Exception:
                data = {}
        if key_lower in data:
            return
        data[key_lower] = cg_id
        tmp = CONFIG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(dict(sorted(data.items())), f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, CONFIG_PATH)
        _SYMBOL_TO_ID[symbol_upper] = cg_id


def _dynamic_resolve_id(base_upper: str) -> Optional[str]:
    """
    CoinGecko /search fallback. Cached in-memory and persisted to token_price_mapping.json.
    Does not override builtin or existing file mappings.
    """
    now = time.time()
    exp = _NEGATIVE_ID_CACHE.get(base_upper)
    if exp is not None and now < exp:
        return None

    if base_upper in _SEARCH_MEMO:
        return _SEARCH_MEMO[base_upper]

    # Static hit (another thread may have updated)
    sid = _SYMBOL_TO_ID.get(base_upper)
    if sid:
        _SEARCH_MEMO[base_upper] = sid
        return sid

    if _is_blocked_symbol_for_dynamic(base_upper):
        _SEARCH_MEMO[base_upper] = None
        return None

    coins = _search_coingecko(base_upper)
    best = _pick_best_search_coin(coins, base_upper)
    if not best:
        _NEGATIVE_ID_CACHE[base_upper] = now + _NEGATIVE_TTL_SEC
        _SEARCH_MEMO[base_upper] = None
        return None

    cg_id = str(best.get("id") or "").strip().lower()
    if not cg_id:
        _NEGATIVE_ID_CACHE[base_upper] = now + _NEGATIVE_TTL_SEC
        _SEARCH_MEMO[base_upper] = None
        return None

    # High confidence: ranked asset or sole exact-symbol match
    rank = best.get("market_cap_rank")
    if rank is None:
        # Allow only if exact symbol match and single hit with that symbol
        sym_hits = [c for c in coins if (c.get("symbol") or "").upper() == base_upper]
        if len(sym_hits) != 1:
            _NEGATIVE_ID_CACHE[base_upper] = now + _NEGATIVE_TTL_SEC
            _SEARCH_MEMO[base_upper] = None
            return None

    _SYMBOL_TO_ID[base_upper] = cg_id
    _SEARCH_MEMO[base_upper] = cg_id
    _persist_new_mapping(base_upper, cg_id)
    _DYNAMIC_RESOLVED_SESSION.append(f"{base_upper} -> {cg_id}")
    print(f"[CG_DYNAMIC] resolved {base_upper} -> {cg_id} (CoinGecko search)")
    return cg_id


def resolve_coingecko_id(symbol: str) -> Optional[str]:
    """
    Map token symbol to CoinGecko id: static (builtin + file) first, then dynamic search.
    Applies map_token() like the price pipeline.
    """
    try:
        from taxtrack.prices.token_mapper import map_token

        base = map_token(symbol)
    except Exception:
        base = (symbol or "").strip().upper()
    base = (base or "").strip().upper()
    if not base:
        return None
    sid = _get_coingecko_id_static(base)
    if sid:
        return sid
    return _dynamic_resolve_id(base)


def _get_coingecko_id(symbol: str) -> Optional[str]:
    """Return CoinGecko id for symbol (static + dynamic)."""
    return resolve_coingecko_id(symbol)


def _date_to_dd_mm_yyyy(date_str: str) -> str:
    """
    Coingecko /history erwartet dd-mm-yyyy im UTC-Kontext.
    date_str kann YYYY-MM-DD oder ISO-String sein.
    """
    try:
        if "T" in date_str:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(date_str)
    except Exception:
        return date_str
    return dt.strftime("%d-%m-%Y")


def _ts_to_date_str(ts: int) -> str:
    """Unix timestamp to YYYY-MM-DD."""
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")


def get_eur_price_fallback(symbol: str, ts: int, max_retries: int = 2) -> Optional[float]:
    """
    Fetch EUR price from CoinGecko for the given symbol and date (from ts).
    Used when the hybrid provider returns 0. Results are cached in-memory to avoid
    duplicate API calls for the same (symbol, date).

    Returns float price in EUR, or None if not found / no mapping / API error.
    """
    if not symbol:
        return None
    try:
        from taxtrack.prices.token_mapper import map_token

        base = map_token(symbol)
    except Exception:
        base = (symbol or "").strip().upper()
    base = (base or "").strip().upper()
    date_str = _ts_to_date_str(ts)
    cache_key = (base, date_str)
    now = time.time()
    if cache_key in _CG_MEMORY_CACHE:
        price, fetched_at = _CG_MEMORY_CACHE[cache_key]
        if (now - fetched_at) < _CG_CACHE_TTL:
            return price
        del _CG_MEMORY_CACHE[cache_key]

    cg_id = _get_coingecko_id(symbol)
    if not cg_id:
        return None

    cg_date = _date_to_dd_mm_yyyy(date_str)
    url = f"{_COINGECKO_BASE}/coins/{cg_id}/history"
    params = {"date": cg_date, "localization": "false"}

    for attempt in range(max_retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 429:
                time.sleep(2.0 * (attempt + 1))
                continue
            r.raise_for_status()
            data = r.json()
            market_data = data.get("market_data") or {}
            current_price = market_data.get("current_price") or {}
            # Prefer EUR; CoinGecko history returns current_price.eur for many coins
            price_val = current_price.get("eur")
            if price_val is None:
                price_val = current_price.get("usd")
                # Rough USD→EUR if only USD available (avoids extra API call)
                if price_val is not None and float(price_val) > 0:
                    price_val = float(price_val) * 0.92
            if price_val is not None and float(price_val) > 0:
                p = float(price_val)
                _CG_MEMORY_CACHE[cache_key] = (p, now)
                return p
            return None
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(1.0 * (attempt + 1))
    return None


def fetch_price(
    symbol: str,
    date: str,
    quote: str = "USD",
    granularity: str = "daily",
    normalize_v2: bool = True,
) -> Dict[str, Any]:
    """
    Holt historischen Preis von Coingecko für ein Token zu einem bestimmten Datum.
    Nutzt /coins/{id}/history.

    Rückgabeformat: Dict mit:
      - symbol
      - date
      - price
      - quote
      - source
      - granularity
      - normalize_v2
      - fetched_ts
      - meta
    """
    try:
        from taxtrack.prices.token_mapper import map_token

        base = map_token(symbol)
    except Exception:
        base = (symbol or "").strip().upper()
    base = (base or "").strip().upper()

    token_id = _get_coingecko_id(symbol)
    if not token_id:
        raise KeyError(f"Kein Coingecko-Mapping für Symbol '{symbol}' (nach map_token: '{base}') gefunden.")

    quote_ccy = quote.upper()

    # Datum in Coingecko-Format
    cg_date = _date_to_dd_mm_yyyy(date)

    url = f"{_COINGECKO_BASE}/coins/{token_id}/history"
    params = {
        "date": cg_date,
        "localization": "false",
    }

    resp = requests.get(url, params=params, timeout=15)
    resp.raise_for_status()
    data = resp.json()

    market_data = data.get("market_data") or {}
    current_price = market_data.get("current_price") or {}
    price_val = current_price.get(quote_ccy.lower())

    if price_val is None:
        # Fallback: ggf. in USD holen und später FX anwenden
        # (für jetzt: einfach Fehlermeldung, damit es dir auffällt)
        raise ValueError(
            f"Coingecko liefert keinen {quote_ccy}-Preis für {symbol} am {date}"
        )

    # Optional: normalize_v2 könnte spätere Rundungs-/Scaling-Logik enthalten.
    # Für jetzt: einfach float().
    price_val = float(price_val)

    return {
        "symbol": (base or symbol or "").upper(),
        "date": date,
        "price": price_val,
        "quote": quote_ccy,
        "source": "coingecko",
        "granularity": granularity,
        "normalize_v2": normalize_v2,
        "fetched_ts": int(datetime.utcnow().timestamp()),
        "meta": {
            "provider": "coingecko",
            "endpoint": "/coins/{id}/history",
            "coingecko_id": token_id,
            "raw_date_param": cg_date,
        },
    }
