# taxtrack/loaders/evm_master_loader.py
from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any

from taxtrack.loaders.auto_detect import load_auto
from taxtrack.prices import get_eur_price
from taxtrack.rules.evaluate import evaluate_batch
from taxtrack.analyze.gains import compute_gains


print(">>> LOADED evm_master_loader.py FROM:", __file__)


def load_evm_folder(
    folder: str,
    wallet: str,
    chain_id: str,
    ts_start: int | None = None,
    ts_end: int | None = None,
) -> Dict[str, Any]:
    """
    Lädt alle EVM CSVs aus einem Ordner.

    Features:
    - Chain-ID wird über Ordnername erzwungen
    - Optionaler früher Zeitfilter (Performance)
    - EUR-Preis-Ermittlung
    - Klassifikation
    - FIFO-Gains
    """

    folder = Path(folder)
    if not folder.exists():
        raise FileNotFoundError(f"EVM folder not found: {folder}")

    # ----------------------------------------------------
    # Chain-ID aus Ordnerpfad erzwingen
    # ----------------------------------------------------
    folder_lower = str(folder).lower()

    if "matic" in folder_lower or "polygon" in folder_lower or "pol" in folder_lower:
        chain_id = "matic"
    elif "arb" in folder_lower or "arbitrum" in folder_lower:
        chain_id = "arb"
    elif "op" in folder_lower or "optimism" in folder_lower:
        chain_id = "op"
    # BNB / BSC / Binance Ordnernamen auf kanonisches "bnb" normalisieren
    elif (
        "bsc" in folder_lower
        or "binance" in folder_lower
        or "bnbchain" in folder_lower
    ):
        chain_id = "bnb"
    else:
        chain_id = "eth"

    raw_rows: List[Any] = []

    # ----------------------------------------------------
    # 1) Dateien laden
    # ----------------------------------------------------
    for file in folder.iterdir():
        if not file.is_file():
            continue
        if file.suffix.lower() not in (".csv", ".txt"):
            continue

        try:
            rows = load_auto(file, wallet, chain_id=chain_id)
            raw_rows.extend(rows)
        except Exception as e:
            print(f"[EVM_MASTER] Fehler beim Laden von {file.name}: {e}")

    # ----------------------------------------------------
    # 2) FRÜHER ZEITFILTER (DAS WAR DER FEHLENDE TEIL)
    # ----------------------------------------------------
    if ts_start is not None and ts_end is not None:
        before = len(raw_rows)
        raw_rows = [
            r for r in raw_rows
            if getattr(r, "timestamp", 0)
            and ts_start <= r.timestamp < ts_end
        ]
        print(f"[EVM_MASTER] Zeitfilter angewendet: {before} → {len(raw_rows)}")

    # ----------------------------------------------------
    # 3) Sortieren & Duplikate
    # ----------------------------------------------------
    raw_rows = [r for r in raw_rows if getattr(r, "timestamp", 0) > 0]
    raw_rows.sort(key=lambda r: r.timestamp)

    dedup = {}
    for r in raw_rows:
        key = (r.tx_hash, r.direction, r.amount, r.timestamp)
        dedup[key] = r

    raw_rows = list(dedup.values())

    # ----------------------------------------------------
    # 4) EUR-Preis setzen
    # ----------------------------------------------------
    enriched_rows = []
    for r in raw_rows:
        try:
            chain = getattr(r, "chain_id", None) or chain_id
            amount = float(r.amount or 0.0)
            print(f"[VALUE CALC] {r.token} amount={amount} timestamp={r.timestamp} chain={chain}")
            eur_price = get_eur_price(r.token, r.timestamp, chain=chain)
            eur_value = eur_price * amount
        except Exception:
            eur_value = 0.0

        item = r.to_dict()
        item["eur_value"] = eur_value
        enriched_rows.append(item)

    # ----------------------------------------------------
    # 5) Klassifikation
    # ----------------------------------------------------
    classified, debug_info = evaluate_batch(enriched_rows, wallet)

    # ----------------------------------------------------
    # 6) FIFO-Gains
    # ----------------------------------------------------
    gains, totals = compute_gains(classified)

    return {
        "raw": raw_rows,
        "classified": classified,
        "debug": debug_info,
        "gains": gains,
        "totals": totals,
    }
