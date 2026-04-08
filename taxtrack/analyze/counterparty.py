# taxtrack/analyze/counterparty.py
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Datei, in der wir Labels für Adressen speichern
ADDRESS_MAP_PATH = Path("data/address_map.json")


# ---------- Helpers für Map-Handling ----------

def _load_address_map() -> Dict[str, Dict[str, Any]]:
    """Lädt bekannte Adressen (Pendle, Beefy, eigene Wallets, …)."""
    if ADDRESS_MAP_PATH.exists():
        try:
            return json.loads(ADDRESS_MAP_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_address_map(mapping: Dict[str, Dict[str, Any]]) -> None:
    """Speichert die Adress-Tabelle dauerhaft."""
    ADDRESS_MAP_PATH.parent.mkdir(parents=True, exist_ok=True)
    ADDRESS_MAP_PATH.write_text(
        json.dumps(mapping, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _normalize_addr(addr: str | None) -> str:
    return (addr or "").strip().lower()


def _short(addr: str | None) -> str:
    """Schöne Kurzform 0x1234…abcd für das PDF."""
    a = addr or ""
    if len(a) <= 12:
        return a or "-"
    return a[:6] + "…" + a[-4:]


def _learn_address(
    mapping: Dict[str, Dict[str, Any]],
    addr: str | None,
    label: str,
    kind: str,
    chain_id: str,
) -> None:
    """
    Lernfunktion:
    - legt neue Adresse in address_map.json an, wenn noch unbekannt
    - überschreibt nichts, was du evtl. manuell gepflegt hast
    """
    a = _normalize_addr(addr)
    if not a:
        return
    if a in mapping:
        return
    mapping[a] = {
        "label": label,
        "kind": kind,          # z.B. "protocol", "wallet", "internal", "dex"
        "chain_id": chain_id,  # z.B. "1" für Ethereum Mainnet
    }


# ---------- Kernlogik ----------

def _guess_counterparty_kind_and_label(record: Dict[str, Any],
                                       cp_addr: str | None,
                                       wallet_addr: str | None) -> Tuple[str, str]:
    """
    Heuristiken für neue / unbekannte Adressen.
    Gibt (kind, label) zurück.
    """
    
    source = (record.get("source") or "").lower()
    category = (record.get("category") or "").lower()
    method = (record.get("method") or "").lower()
    direction = (record.get("direction") or "").lower()
        # --- HARDCODED PATTERN MATCHES für DEX-Router ---
    if cp_addr:
        a = cp_addr.lower()

        if "e592427a0a" in a:
            return "protocol", "Uniswap V3 Router"

        if "68b3465833" in a:
            return "protocol", "Uniswap V3 Universal Router"

        if "1111111254" in a:
            return "protocol", "1inch Router"

        if "def171fe48" in a:
            return "protocol", "ParaSwap Router"


    # 1) Interne Transfers (falls wir die eigene Wallet kennen)
    w = _normalize_addr(wallet_addr)
    f = _normalize_addr(record.get("from_addr"))
    t = _normalize_addr(record.get("to_addr"))
    if w and (f == w or t == w) and f and t and f != t:
        # unsere Wallet ↔ andere Adresse
        if "internal" in category:
            return "internal", "Internal Transfer"
        # bleibt trotzdem "wallet", aber klarer Text
        return "wallet", "Wallet Transfer"

    # 2) Coinbase / CEX
    if source == "coinbase":
        return "cex", "Coinbase"

    # 3) Swaps & DEX
    if "swap" in method or "swap" in category:
        return "dex", "DEX Swap"

    if any(x in method for x in ["addliquidity", "removeliquidity", "mint", "burn"]):
        return "dex", "DEX / Liquidity"

    # 4) Brücken
    if "bridge" in method or "bridge" in category:
        return "bridge", "Bridge"

    # 5) Staking / Restaking / Rewards
    if any(x in category for x in ["stake", "staking", "reward"]):
        return "staking", "Staking / Reward"

    # 6) Default → normale Wallet
    return "wallet", _short(cp_addr)


def classify_counterparty(
    record: Dict[str, Any],
    mapping: Dict[str, Dict[str, Any]],
    wallet_addr: str | None = None,
    chain_id: str = "1",
    learn: bool = True,
) -> str:
    """
    Ermittelt den Counterparty-Text für EIN Record.
    - Greift zuerst auf address_map.json zu
    - Nutzt dann Heuristik
    - Lernt neue Adressen automatisch dazu
    """
    # Bestimme "Gegen-Adresse": also NICHT unsere Wallet
    from_addr = record.get("from_addr")
    to_addr = record.get("to_addr")
    w = _normalize_addr(wallet_addr)
    f = _normalize_addr(from_addr)
    t = _normalize_addr(to_addr)

    if w:
        if f == w:
            cp_addr = to_addr
        elif t == w:
            cp_addr = from_addr
        else:
            # Fallback – wir raten: to_addr ist Gegenpartei
            cp_addr = to_addr or from_addr
    else:
        cp_addr = to_addr or from_addr

    cp_norm = _normalize_addr(cp_addr)

    # 1) Lookup in address_map.json
    if cp_norm and cp_norm in mapping:
        entry = mapping[cp_norm]

        # Richtige Struktur (dict)
        if isinstance(entry, dict):
            return entry.get("label") or _short(cp_addr)

        # Falsche Struktur (string, int, etc.)
        return str(entry)


    # 2) Heuristik
    kind, label = _guess_counterparty_kind_and_label(record, cp_addr, wallet_addr)

    # 3) Lernen
    if learn and cp_norm:
        _learn_address(mapping, cp_addr, label, kind, chain_id)

    # --- FINAL: unknown fallback ---
    if label.lower() in {"", "-", "wallet transfer"}:
        return "unknown"

    return label



def attach_counterparties(
    records: List[Dict[str, Any]],
    wallet_addr: str | None = None,
    chain_id: str = "1",
    learn: bool = True,
) -> None:
    """
    Hängt für alle Records ein Feld 'counterparty' an
    und aktualisiert data/address_map.json.
    """
    mapping = _load_address_map()

    for r in records:
        label = classify_counterparty(
            record=r,
            mapping=mapping,
            wallet_addr=wallet_addr,
            chain_id=chain_id,
            learn=learn,
        )
        r["counterparty"] = label

    _save_address_map(mapping)

# ------------------------------------------------------------
# 🟦 Mapping Loader (für Tests und interne Nutzung)
# ------------------------------------------------------------

def load_mapping(chain_id: str = "1") -> Dict[str, Dict[str, Any]]:
    """
    Lädt das address_map.json als dict.
    `chain_id` wird ignoriert, damit Tests flexibel sind.
    Gibt IMMER ein dict zurück (niemals None).
    """
    try:
        if ADDRESS_MAP_PATH.exists():
            with ADDRESS_MAP_PATH.open("r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
    except Exception:
        pass

    return {}
