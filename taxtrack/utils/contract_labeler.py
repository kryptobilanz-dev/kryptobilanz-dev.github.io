import json
from pathlib import Path

# ------------------------------
# Load Address Map JSON
# ------------------------------
ADDR_MAP_FILE = Path(__file__).resolve().parents[1] / "data" / "config" / "address_map.json"

try:
    ADDRESS_MAP = json.loads(ADDR_MAP_FILE.read_text())
except Exception as e:
    print("[CONTRACT_LABELER] ERROR loading address_map.json:", e)
    ADDRESS_MAP = {}

# normalize keys for case-insensitive matching
NORMALIZED_MAP = {}

for chain, items in ADDRESS_MAP.items():
    chain_key = str(chain).lower()

    NORMALIZED_MAP[chain_key] = {}
    for addr, meta in items.items():
        if isinstance(addr, str):
            addr_key = addr.lower()
        else:
            continue

        # meta kann string ODER objekt sein → konvertieren
        if isinstance(meta, str):
            NORMALIZED_MAP[chain_key][addr_key] = {
                "label": meta,
                "protocol": "",
                "type": "",
                "tags": []
            }
        elif isinstance(meta, dict):
            NORMALIZED_MAP[chain_key][addr_key] = {
                "label": meta.get("label", ""),
                "protocol": meta.get("protocol", ""),
                "type": meta.get("type", ""),
                "tags": meta.get("tags", []) if isinstance(meta.get("tags"), list) else []
            }


# ------------------------------
# Built-in protocol mapping (Phase 2)
# ------------------------------
# This augments address_map.json without changing loaders/pipeline.
# Keys are chain ids (eth/arb/base/op/avax) and "all" for common addresses.
_BUILTIN: dict[str, dict[str, dict]] = {
    "all": {
        # Balancer Vault (same address on multiple chains)
        "0xba12222222228d8ba445958a75a0704d566bf2c8": {
            "label": "balancer_vault",
            "protocol": "balancer",
            "type": "vault",
            "tags": ["dex", "balancer"],
        },
        # 1inch Router (common across chains)
        "0x1111111254eeb25477b68fb85ed929f73a960582": {
            "label": "oneinch_router",
            "protocol": "dex",
            "type": "aggregator",
            "tags": ["dex", "aggregator", "1inch"],
        },
        # 1inch (v6-style) router address seen in datasets
        "0x888888888889758f76e7103c6cbf23abbf58f946": {
            "label": "aggregator_router",
            "protocol": "dex",
            "type": "aggregator",
            "tags": ["dex", "aggregator"],
        },
    },
    "eth": {
        # Uniswap V2 Router
        "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": {
            "label": "uniswap_v2_router",
            "protocol": "uniswap",
            "type": "router",
            "tags": ["dex", "uniswap", "v2"],
        },
        # Uniswap V3 SwapRouter02 (commonly used)
        "0xe592427a0aece92de3edee1f18e0157c05861564": {
            "label": "uniswap_v3_router",
            "protocol": "uniswap",
            "type": "router",
            "tags": ["dex", "uniswap", "v3"],
        },
        # Curve Router NG (Ethereum)
        "0x99a58482bd75cbab83b27ec03ca68ff489b5788f": {
            "label": "curve_router",
            "protocol": "curve",
            "type": "router",
            "tags": ["dex", "curve"],
        },
        # Aave V3 Pool (Ethereum)
        "0x87870bca3f3fd6335c3f4ce8392d69350b4fa4e2": {
            "label": "aave_v3_pool",
            "protocol": "aave",
            "type": "pool",
            "tags": ["lending", "aave", "v3"],
        },
    },
    "avax": {
        # Beefy vault/router commonly appears as counterparty in AVAX datasets
        "0x2e72e1436f1a2b2e0d2fa4394ac06857c7b281b3": {
            "label": "beefy_vault",
            "protocol": "beefy",
            "type": "vault",
            "tags": ["vault", "beefy"],
        },
    },
}


def _merge_builtin() -> None:
    def put(chain_key: str, addr: str, meta: dict) -> None:
        ck = (chain_key or "").lower()
        if ck not in NORMALIZED_MAP:
            NORMALIZED_MAP[ck] = {}
        NORMALIZED_MAP[ck].setdefault(addr.lower(), meta)

    for addr, meta in _BUILTIN.get("all", {}).items():
        for ck in list(NORMALIZED_MAP.keys()) or ["eth"]:
            put(ck, addr, meta)
    for ck, items in _BUILTIN.items():
        if ck == "all":
            continue
        for addr, meta in items.items():
            put(ck, addr, meta)


_merge_builtin()


def label_address(addr: str, chain: str = "eth"):
    """
    Return normalized metadata dict for an address.
    Lowercase matching.
    chain can be "eth" or "1".
    """
    if not addr:
        return {}

    addr_l = addr.lower()
    chain_l = chain.lower()

    # try exact chain (eth)
    if chain_l in NORMALIZED_MAP:
        if addr_l in NORMALIZED_MAP[chain_l]:
            return NORMALIZED_MAP[chain_l][addr_l]

    # fallback: treat chain_id=1 as eth
    if "1" in NORMALIZED_MAP:
        if addr_l in NORMALIZED_MAP["1"]:
            return NORMALIZED_MAP["1"][addr_l]

    return {}
