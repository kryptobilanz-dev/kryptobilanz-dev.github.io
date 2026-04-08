# taxtrack/prices/token_mapper.py
#
# Maps raw token symbols to canonical price keys used by CSV/Yahoo/Binance/Kraken.
# Does not return prices; missing-market assets stay as distinct symbols (or UNKNOWN).
# Only add mappings that are unambiguous (same economic asset as target).

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

TOKEN_MAP = {
    # ETH Derivatives
    "ETH": "ETH",
    "WETH": "ETH",
    "STETH": "ETH",
    "WSTETH": "ETH",
    "RSETH": "ETH",
    "RSWETH": "ETH",
    "WEETH": "ETH",
    "EZETH": "ETH",
    "EETH": "ETH",
    "SWETH": "ETH",
    "RETH": "ETH",
    "RSETHE": "ETH",
    "RENZO RESTAKED ETH": "ETH",
    "RESTAKE_ETH": "ETH",
    # Bridged / chain-suffixed wrapped ETH (same underlying as ETH for pricing)
    "WETHE": "ETH",
    "WETH.E": "ETH",
    "ETH.E": "ETH",
    # Homoglyph-damaged ETH aliases observed in source CSV symbols (safe normalization)
    "EH": "ETH",
    "W": "ETH",

    # Avalanche wrapped
    "WAVAX": "AVAX",
    "WAVAX.E": "AVAX",

    # BTC wrapped (standard WBTC tracks BTC)
    "WBTC": "BTC",
    "WBTC.E": "BTC",

    # PENDLE LPT
    "PENDLE-LPT": "PENDLE_LPT",
    "ERC20 ***": "UNKNOWN",

    # Pendle Core
    "PENDLE": "PENDLE",

    # Cosmos
    "ATOM": "ATOM",
    "STATOM": "ATOM",
    "MILKATOM": "ATOM",

    # L1 / L2
    "MATIC": "MATIC",
    "POL": "POL",
    "WPOL": "POL",
    "WPOL.E": "POL",
    "WMATIC": "MATIC",
    "WMATIC.E": "MATIC",
    "ARB": "ARB",
    "OP": "OP",
    "AVAX": "AVAX",
    "BNB": "BNB",
    "WBNB": "BNB",
    "WBNB.E": "BNB",
    "FTM": "FTM",
    "WFTM": "FTM",
    "WFTM.E": "FTM",

    # Common Tokens — USD-pegged stables → normalization anchor USD (EUR via FX in engine)
    "USDT": "USD",
    "USDC": "USD",
    "USDT0": "USD",
    "USDCE": "USD",
    "USDCE.E": "USD",
    "USDC.E": "USD",
    "USDT.E": "USD",
    "DAI": "USD",
    "TUSD": "USD",
    "USDE": "USD",
    "BUSD": "USD",
    "USDP": "USD",
    "GUSD": "USD",
    "PYUSD": "USD",
    "LUSD": "USD",
    "USDBC": "USD",
    "AXLUSDC": "USD",
    "USDC.E.E": "USD",
    "USDT.E.E": "USD",
    # Homoglyph-damaged USD stable aliases observed in source CSV symbols (safe normalization)
    "UD": "USD",
    "UDT": "USD",

    # Others
    "TARA": "TARA",
    "MON": "MON",
    "GPT": "GPT",
}

AUTO_TOKEN_MAP_FILE = Path(__file__).resolve().parents[1] / "data" / "config" / "auto_token_map.json"

# Roots that map to USD — used only to normalize bridged variants like USDC.e / USDT.e.e
_STABLE_USD_ROOTS = frozenset(
    k for k, v in TOKEN_MAP.items() if v == "USD"
)


@lru_cache(maxsize=1)
def _load_auto_token_map() -> dict[str, str]:
    try:
        if not AUTO_TOKEN_MAP_FILE.exists():
            return {}
        data = json.loads(AUTO_TOKEN_MAP_FILE.read_text(encoding="utf-8"))
        raw = data.get("token_map") if isinstance(data, dict) else {}
        if not isinstance(raw, dict):
            return {}
        out: dict[str, str] = {}
        for k, v in raw.items():
            ks = normalize(str(k))
            vs = normalize(str(v))
            if ks and vs:
                out[ks] = vs
        return out
    except Exception:
        return {}


def normalize(symbol: str) -> str:
    if not symbol:
        return ""
    return symbol.strip().upper()


def map_token(symbol: str) -> str:
    if not symbol:
        return ""

    sym = normalize(symbol)

    # Synthetic restake lot ids: price by underlying base (LRT_SWETH → SWETH → …)
    if sym.startswith("LRT_") and len(sym) > 4:
        return map_token(sym[4:])

    if sym in TOKEN_MAP:
        return TOKEN_MAP[sym]

    auto_map = _load_auto_token_map()
    if sym in auto_map:
        return auto_map[sym]

    # Bridged stablecoin suffixes (Avalanche / multichain naming): only when root is a known USD stable
    for suffix in (".E.E", ".E"):
        if sym.endswith(suffix) and len(sym) > len(suffix):
            root = sym[: -len(suffix)]
            if root in _STABLE_USD_ROOTS:
                return "USD"

    # ERC20 unknown wildcards
    if sym.startswith("ERC20"):
        return "UNKNOWN"

    return sym


def get_price_id(symbol: str):
    base = map_token(symbol)
    return base.lower()
