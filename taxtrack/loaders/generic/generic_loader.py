# taxtrack/loaders/generic/generic_loader.py

import csv
import io
from pathlib import Path
from dateutil import parser as dtparser

from taxtrack.schemas.RawRow import RawRow
from taxtrack.utils.time import iso_from_unix
from taxtrack.utils.direction import derive_direction, assert_direction_derivation
from taxtrack.utils.csv_utils import get_field
from taxtrack.validation.raw_row import validate_raw_row, DEBUG_VALIDATION


def _read_file_text_auto(path: Path) -> str:
    """
    Öffnet eine CSV absolut robust:
    - liest als Bytes
    - erkennt BOM / UTF-16 / UTF-8 / CP1252
    - decodiert den gesamten Text
    """
    raw = path.read_bytes()

    # UTF-8 BOM
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw[3:].decode("utf-8", errors="ignore")

    # UTF-16 LE BOM (FF FE)
    if raw.startswith(b"\xff\xfe"):
        return raw.decode("utf-16-le", errors="ignore")

    # UTF-16 BE BOM (FE FF)
    if raw.startswith(b"\xfe\xff"):
        return raw.decode("utf-16-be", errors="ignore")

    # Versuche UTF-8 normal
    try:
        return raw.decode("utf-8", errors="strict")
    except Exception:
        pass

    # CP1252 / Windows ANSI
    try:
        return raw.decode("cp1252", errors="strict")
    except Exception:
        pass

    # Latin-1 als Fallback
    try:
        return raw.decode("latin1", errors="ignore")
    except Exception:
        pass

    # Letzter Ausweg
    return raw.decode("utf-8", errors="ignore")


def load_generic(path: Path, wallet: str, chain_id: str = ""):
    """
    Universeller CSV-Loader – für jede Datei mit beliebigem Encoding.
    chain_id: optional; passed to RawRow (e.g. from load_auto path or caller).
    """

    text = _read_file_text_auto(path)
    f = io.StringIO(text)
    reader = csv.DictReader(f)

    rows = []
    chain = (chain_id or "").strip().lower() or "generic"

    for line in reader:
        ts_raw = (line.get("timestamp") or "").strip()
        if not ts_raw:
            continue

        # UNIX oder ISO?
        if ts_raw.isdigit():
            unix = int(ts_raw)
        else:
            unix = int(dtparser.parse(ts_raw).timestamp())

        # Data integrity: non-empty tx_hash/token/method; direction from wallet/addresses only
        tx_hash = (line.get("tx_hash", "") or "").strip()
        if not tx_hash:
            tx_hash = f"generic:{unix}:{len(rows)}"
        token = (line.get("token") or "ETH").strip().upper() or "ETH"
        from_addr = get_field(line, "from", "From").strip().lower()
        to_addr = get_field(line, "to", "To").strip().lower()
        direction = derive_direction(wallet, from_addr, to_addr)
        method = (line.get("method", "Transfer") or "Transfer").strip() or "Transfer"

        row = RawRow(
            source="generic",
            tx_hash=tx_hash,
            timestamp=unix,
            dt_iso=iso_from_unix(unix),
            from_addr=from_addr,
            to_addr=to_addr,
            token=token,
            amount=float(line.get("amount", 0) or 0),
            direction=direction,
            method=method,
            fee_token=line.get("fee_token") or None,
            fee_amount=float(line.get("fee_amount", 0) or 0),
            chain_id=chain,
            meta={"source_file": str(path), "chain_id": chain},
        )
        if float(getattr(row, "amount", 0.0) or 0.0) <= 0:
            # Keep behavior aligned with swap/FIFO engines: only positive token movements become RawRows.
            continue
        assert_direction_derivation(row, wallet)
        if DEBUG_VALIDATION:
            validate_raw_row(row)
        rows.append(row)

    return rows
