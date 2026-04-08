# taxtrack/loaders/etherscan/internal_loader.py
# ZenTaxCore – EVM Internal Loader (internal transactions / traces)

import csv
import io
from pathlib import Path
from dateutil import parser as dtparser

from taxtrack.schemas.RawRow import RawRow
from taxtrack.utils.time import iso_from_unix
from taxtrack.utils.num import to_float
from taxtrack.loaders.generic.generic_loader import _read_file_text_auto
from taxtrack.data.config.chain_config import CHAIN_CONFIG
from taxtrack.utils.token_normalize import normalize_token_symbol
from taxtrack.utils.direction import derive_direction, assert_direction_derivation
from taxtrack.utils.csv_utils import get_field
from taxtrack.validation.raw_row import validate_raw_row, DEBUG_VALIDATION


def load_internal_etherscan(path: Path, wallet: str, chain_id: str = "eth"):
    """
    Loader für Internal-Transaction-Exports (Etherscan, Arbiscan, BaseScan, ...)

    Typische Spalten:
      - UnixTimestamp oder DateTime (UTC)
      - From
      - To / TxTo
      - Value_IN(ETH) / Value_OUT(ETH)
      - Type
    """

    chain_info = CHAIN_CONFIG.get(chain_id, CHAIN_CONFIG["eth"])
    native_symbol = chain_info.get("native_symbol", "ETH")

    text = _read_file_text_auto(path)
    f = io.StringIO(text)
    reader = csv.DictReader(f)

    rows = []
    skipped_missing_tx_hash = 0

    for line in reader:
        # -------------------------
        # Timestamp
        # -------------------------
        ts_raw = (line.get("UnixTimestamp") or line.get("unixTimestamp") or "").strip()
        if ts_raw.isdigit():
            ts = int(ts_raw)
        else:
            dt_str = (
                line.get("DateTime (UTC)")
                or line.get("DateTime")
                or line.get("Date")
                or line.get("date")
                or ""
            )
            if not dt_str:
                ts = 0
            else:
                ts = int(dtparser.parse(dt_str).timestamp())

        dt_iso = iso_from_unix(ts) if ts > 0 else ""
        if ts <= 0:
            continue  # skip rows without valid timestamp (dt_iso would be empty; validation requires it)

        # -------------------------
        # Addresses (case-insensitive: Etherscan uses "From"/"To"/"TxTo")
        # -------------------------
        from_addr = get_field(line, "From", "from").strip().lower()
        to_addr = (get_field(line, "TxTo", "txto") or get_field(line, "To", "to")).strip().lower()

        # -------------------------
        # Amount / Direction
        # -------------------------
        val_in = to_float(
            line.get("Value_IN(ETH)")
            or line.get("Value_IN")
            or line.get("Value")
            or 0
        )
        val_out = to_float(
            line.get("Value_OUT(ETH)")
            or line.get("Value_OUT")
            or 0
        )

        direction = derive_direction(wallet, from_addr, to_addr)
        if direction == "in":
            amount = val_in
        elif direction == "out":
            amount = val_out
        else:
            amount = 0.0

        if amount <= 0:
            continue  # skip zero token movement (call, delegate, etc.)

        token = normalize_token_symbol(native_symbol)

        method = (line.get("Type") or line.get("traceType") or "").strip() or "unknown"
        tx_hash = (
            line.get("Transaction Hash")
            or line.get("Txn Hash")
            or line.get("Txhash")
            or line.get("Hash")
            or ""
        ).strip()
        if not tx_hash:
            skipped_missing_tx_hash += 1
            continue

        row = RawRow(
            source="etherscan",
            tx_hash=tx_hash,
            timestamp=ts,
            dt_iso=dt_iso,
            from_addr=from_addr,
            to_addr=to_addr,
            token=token,
            amount=amount,
            direction=direction,
            method=method,
            fee_token=native_symbol,
            fee_amount=0.0,  # GasFee steckt im normalen TX, nicht im Internal
            category="internal_transfer",
            chain_id=chain_id,
            meta={
                "source_file": str(path),
                "chain_id": chain_id,
                "loader": "etherscan_internal",
            },
        )
        assert_direction_derivation(row, wallet)
        if DEBUG_VALIDATION:
            validate_raw_row(row)
        rows.append(row)

    if skipped_missing_tx_hash:
        print(f"[etherscan_internal] skipped {skipped_missing_tx_hash} row(s): missing tx_hash")
    return rows
