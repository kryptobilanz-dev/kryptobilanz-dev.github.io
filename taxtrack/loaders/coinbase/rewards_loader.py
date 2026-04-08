import re
from pathlib import Path
from dateutil import parser as dtparser

from taxtrack.schemas.RawRow import RawRow
from taxtrack.utils.time import iso_from_unix
from taxtrack.utils.direction import derive_direction, assert_direction_derivation
from taxtrack.validation.raw_row import validate_raw_row, DEBUG_VALIDATION
from taxtrack.prices.provider_master import price_provider


def load_coinbase_rewards(path: Path, wallet: str = ""):
    raw = Path(path).read_text(encoding="utf-8", errors="ignore").splitlines()

    cleaned=[]
    for l in raw:
        if not l.strip():
            continue
        if l.startswith("Transactions"):
            continue
        if l.startswith("User,"):
            continue
        cleaned.append(l)

    # Header suchen
    header_line = None
    for l in cleaned:
        if "Transaction Type" in l and "Timestamp" in l:
            header_line = l
            break

    if not header_line:
        raise ValueError("Rewards Header nicht gefunden!")

    header_cols = header_line.split(",")

    start = cleaned.index(header_line) + 1
    body = cleaned[start:]

    rows = []

    for l in body:
        # Nur die ersten 5 Felder parsen:
        # ID, Timestamp, Transaction Type, Asset, Quantity Transacted
        # Danach alles ignorieren
        parts = l.split(",", 5)   # Max 6 Teile

        if len(parts) < 5:
            continue

        txid, ts_raw, tx_type, asset, amount_raw = parts[:5]

        try:
            ts = int(dtparser.parse(ts_raw).timestamp())
        except:
            continue

        # normalize; ensure token/tx_hash non-empty for RawRow validation
        token = (asset or "").strip().upper() or "UNKNOWN"
        tx_hash = (txid or "").strip() or f"coinbase_rewards:{ts}:{len(rows)}"

        # amount (deutsche Formatierung entfernen)
        a = amount_raw.replace("€", "").replace(".", "").replace(",", ".")
        try:
            amount = float(a)
        except:
            amount = 0.0

        if amount <= 0:
            continue

        # EUR-Wert bestimmen (Preisengine); direction from derive_direction(wallet, from_addr, to_addr)
        print(f"[VALUE CALC] {token} amount={amount} timestamp={ts} chain=coinbase")
        price = price_provider.get_eur_price(token, ts)
        eur_value = round(price * amount, 4)

        from_addr = "coinbase"
        to_addr = (wallet or "").lower().strip() if wallet else "wallet"
        direction = derive_direction(wallet, from_addr, to_addr) if wallet else "in"

        rr = RawRow(
            source="coinbase_rewards",
            tx_hash=tx_hash,
            timestamp=ts,
            dt_iso=iso_from_unix(ts),
            from_addr=from_addr,
            to_addr=to_addr,
            token=token,
            amount=amount,
            direction=direction,
            method="reward",
            fee_token=None,
            fee_amount=0.0,
            category="reward",
            eur_value=eur_value,
            chain_id="coinbase",
            meta={}
        )
        assert_direction_derivation(rr, wallet)
        if DEBUG_VALIDATION:
            validate_raw_row(rr)
        rows.append(rr.to_dict())

    return rows
