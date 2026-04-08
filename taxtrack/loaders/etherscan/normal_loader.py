# taxtrack/loaders/etherscan/normal_loader.py
# ZenTaxCore – EVM Normal Loader (ETH / native / gemischte Etherscan-Exports)

import csv
import io
from pathlib import Path
from dateutil import parser as dtparser

from taxtrack.schemas.RawRow import RawRow
from taxtrack.utils.time import iso_from_unix
from taxtrack.utils.num import to_float
from taxtrack.loaders.generic.generic_loader import _read_file_text_auto
from taxtrack.data.config.chain_config import CHAIN_CONFIG
from taxtrack.utils.gas import unify_gas_fee
from taxtrack.utils.token_normalize import normalize_token_symbol
from taxtrack.utils.direction import derive_direction, assert_direction_derivation
from taxtrack.utils.csv_utils import get_field
from taxtrack.validation.raw_row import validate_raw_row, DEBUG_VALIDATION


def load_etherscan(path: Path, wallet: str, chain_id: str = "eth"):
    """
    Universeller Loader für Etherscan-/Arbiscan-/BaseScan-/BscScan-Exports
    (normale Transaktionen – inkl. evtl. ERC20-Zeilen in diesem Export).

    Unterstützt u.a. Spaltennamen:
      - UnixTimestamp oder DateTime (UTC) / DateTime / Date
      - From / To
      - Value_IN(ETH) / Value_OUT(ETH) / Value_IN / Value_OUT
      - TxnFee(ETH) / TxnFee(...) oder gasUsed + (effectiveGasPrice|gasPrice)
      - Type, Method, FunctionName

    chain_id:
      "eth", "arb", "op", "base", "bnb", "matic", "avax", "ftm"
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
                # Fallback: 0 → später aussortieren
                ts = 0
            else:
                ts = int(dtparser.parse(dt_str).timestamp())

        dt_iso = iso_from_unix(ts) if ts > 0 else ""
        if ts <= 0:
            continue  # skip rows without valid timestamp (dt_iso would be empty; validation requires it)

        # -------------------------
        # Addresses (case-insensitive: Etherscan uses "From"/"To")
        # -------------------------
        from_addr = get_field(line, "From", "from").strip().lower()
        to_addr = get_field(line, "To", "to").strip().lower()
        direction = derive_direction(wallet, from_addr, to_addr)

        # -------------------------
        # Method
        # -------------------------
        method = (
            line.get("Method")
            or line.get("Function")
            or line.get("FunctionName")
            or line.get("functionName")
            or ""
        )

        # -------------------------
        # Token & Amount
        # -------------------------
        # Default: native Token der Chain
        token_raw = (
            line.get("TokenSymbol")
            or line.get("Token")
            or native_symbol
        )
        if direction == "in":
            amount = to_float(
                line.get("Value_IN")
                or line.get("Value_IN(ETH)")
                or line.get("Value_IN (ETH)")
                or line.get("Value")
                or line.get("Amount")
            )
        elif direction == "out":
            amount = to_float(
                line.get("Value_OUT")
                or line.get("Value_OUT(ETH)")
                or line.get("Value_OUT (ETH)")
                or line.get("Value")
                or line.get("Amount")
            )
        else:
            amount = 0.0

        if amount <= 0:
            continue  # skip zero token movement (approve, call, multicall noise)

        token = normalize_token_symbol(token_raw or native_symbol)

        # -------------------------
        # Fee (Gas)
        # -------------------------
        fee_token, fee_amount = unify_gas_fee(line, chain_info)

        # -------------------------
        # Category
        # -------------------------
        type_field = (line.get("Type") or line.get("type") or "").upper()

        if "ERC20" in type_field:
            category = "erc20_transfer"
        elif type_field == "IN":
            category = "native_transfer_in"
        elif type_field == "OUT":
            category = "native_transfer_out"
        else:
            # Fallback: aus Richtung ableiten, wenn kein Type-Feld existiert
            if direction == "in" and amount > 0:
                category = "native_transfer_in"
            elif direction == "out" and amount > 0:
                category = "native_transfer_out"
            else:
                category = "unknown"
        # -------------------------
        # Tx Hash (skip row if missing for data integrity)
        # -------------------------
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

        method_val = (method or "unknown").strip() or "unknown"
        row = RawRow(
            source="etherscan",
            tx_hash=tx_hash,
            timestamp=ts,
            dt_iso=dt_iso,
            from_addr=from_addr,
            to_addr=to_addr,
            token=token,
            amount=amount,
            amount_raw=str(amount) if amount is not None else None,
            decimals=chain_info.get("decimals", 18),
            direction=direction,
            method=method_val,
            fee_token=fee_token,
            fee_amount=fee_amount,
            category=category,
            chain_id=chain_id,
            meta={
                "source_file": str(path),
                "chain_id": chain_id,
                "loader": "etherscan_normal",
            },
        )
        assert_direction_derivation(row, wallet)
        if DEBUG_VALIDATION:
            validate_raw_row(row)
        rows.append(row)
    if skipped_missing_tx_hash:
        print(f"[etherscan_normal] skipped {skipped_missing_tx_hash} row(s): missing tx_hash")
    return rows
