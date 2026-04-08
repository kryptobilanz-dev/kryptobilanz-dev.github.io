# taxtrack/relation_engine.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Any
import json


@dataclass
class Relation:
    addr: str
    label: str
    kind: str = "address"


def _load_json(path: Path) -> dict:
    """Hilfsfunktion: JSON sicher laden (oder {} zurückgeben)."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _normalize(addr: str | None) -> str:
    return (addr or "").strip().lower()


def _infer_counterparty(
    rec: dict,
    address_book: Dict[str, Any],
    known_contracts: Dict[str, Any],
) -> Tuple[str, str]:
    """
    Ermittelt eine menschenlesbare Gegenpartei + Typ.

    Rückgabe:
      (label, kind)
      z.B. ("Pendle", "defi_protocol") oder ("Coinbase → Wallet", "cex_withdrawal")
    """
    from_addr = _normalize(rec.get("from_addr"))
    to_addr = _normalize(rec.get("to_addr"))
    direction = (rec.get("direction") or "").lower()
    source = (rec.get("source") or "").lower()
    token = (rec.get("token") or "").upper()
    method = (rec.get("method") or "").lower()

    # 1️⃣ „Gegenpartei-Adresse“ bestimmen
    if direction == "in":
        cp_addr = from_addr
    elif direction == "out":
        cp_addr = to_addr
    else:
        cp_addr = to_addr or from_addr

    # 2️⃣ known_contracts.json hat Priorität (on-chain Protokolle etc.)
    if cp_addr and cp_addr in known_contracts:
        entry = known_contracts[cp_addr]
        label = entry.get("label") or entry.get("name") or "Contract"
        kind = entry.get("type") or "contract"
        return label, kind

    # 3️⃣ address_map.json (manuelle Overrides: eigene Wallets, CEX-Adressen, Bridges)
    if cp_addr and cp_addr in address_book:
        entry = address_book[cp_addr]
        if isinstance(entry, str):
            return entry, "address"
        label = entry.get("label") or entry.get("name") or "Address"
        kind = entry.get("type") or "address"
        return label, kind

    # 4️⃣ Heuristiken pro Quelle / Token / Methode
    # Coinbase = immer klar CEX
    if source == "coinbase":
        if direction == "in":
            return "Coinbase → Wallet", "cex_withdrawal"
        elif direction == "out":
            return "Wallet → Coinbase", "cex_deposit"
        return "Coinbase", "cex"

    # Pendle / Beefy / Aave – einfache Keyword-Erkennung
    if "pendle" in token or "pendle" in method:
        return "Pendle", "defi_protocol"

    if "beefy" in method:
        return "Beefy", "defi_protocol"

    if "aave" in method:
        return "Aave", "defi_protocol"

    # 5️⃣ Fallback: rohe Adresse, aber gekürzt
    if cp_addr:
        short = cp_addr[:6] + "..." + cp_addr[-4:]
        return short, "address"

    # Nichts sinnvoll erkannt
    return "", ""


def build_relations(records: List[dict]) -> dict:
    """
    Reicher jede Transaktion mit `counterparty` + `counterparty_type` an
    und gibt zusätzlich eine einfache Edge-Liste zurück (für spätere Graphen).
    """
    # Projekt-Root =  .../Stefancore_TaxTrack_v0_3
    root = Path(__file__).resolve().parents[1]

    address_book = _load_json(root / "data" / "address_map.json")
    known_contracts = _load_json(root / "data" / "known_contracts.json")

    edges: List[dict] = []

    for r in records:
        label, kind = _infer_counterparty(r, address_book, known_contracts)
        if label:
            r["counterparty"] = label
            r["counterparty_type"] = kind

        edges.append(
            {
                "source": _normalize(r.get("from_addr")),
                "target": _normalize(r.get("to_addr")),
                "token": r.get("token"),
                "amount": r.get("amount"),
                "counterparty": r.get("counterparty", ""),
                "counterparty_type": r.get("counterparty_type", ""),
            }
        )

    all_addrs = {e["source"] for e in edges if e["source"]} | {
        e["target"] for e in edges if e["target"]
    }

    return {
        "edges": edges,
        "address_count": len(all_addrs),
    }
