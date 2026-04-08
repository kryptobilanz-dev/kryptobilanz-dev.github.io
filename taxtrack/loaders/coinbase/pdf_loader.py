from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any
from dateutil import parser as dtparser
import pdfplumber

from taxtrack.schemas.RawRow import RawRow
from taxtrack.utils.time import iso_from_unix
from taxtrack.utils.direction import derive_direction, assert_direction_derivation
from taxtrack.validation.raw_row import validate_raw_row, DEBUG_VALIDATION


def _to_float(s: str) -> float:
    if s is None:
        return 0.0
    s = str(s).strip()
    if not s:
        return 0.0

    for ch in ["€", "$", "£"]:
        s = s.replace(ch, "")
    s = s.replace(" ", "")

    if "," in s and "." not in s:
        s = s.replace(".", "")
        s = s.replace(",", ".")

    try:
        return float(s)
    except:
        return 0.0


def _normalize_header_cell(cell: Any) -> str:
    return str(cell or "").replace("\n", " ").strip().lower()


def _compact(s: str) -> str:
    return s.replace(" ", "")


def load_coinbase_pdf(path: Path, wallet: str = "") -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    with pdfplumber.open(path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            tables = page.extract_tables()
            if not tables:
                continue

            for table in tables:
                if not table or not table[0]:
                    continue

                header = table[0]
                norm_hdr = [_normalize_header_cell(h) for h in header]
                compact_hdr = [_compact(h) for h in norm_hdr]

                if not any("timestamp" in h for h in compact_hdr):
                    continue

                def find_idx(candidates):
                    for c in candidates:
                        if c in compact_hdr:
                            return compact_hdr.index(c)
                    return None

                i_ts = find_idx(["timestamp"])
                i_type = find_idx(["transactiontype", "type"])
                i_asset = find_idx(["asset"])
                i_qty = find_idx(["quantitytransacted", "quantity"])
                i_total = find_idx(["total1", "total", "total 1"])

                if None in (i_ts, i_type, i_asset, i_qty, i_total):
                    continue

                for row_idx, row in enumerate(table[1:]):
                    if not row:
                        continue

                    cells = [c or "" for c in row]

                    ts_txt = " ".join(str(cells[i_ts]).split())
                    try:
                        ts = int(dtparser.parse(ts_txt).timestamp())
                    except:
                        continue

                    tx_type_raw = " ".join(str(cells[i_type]).split())
                    tx_type = tx_type_raw.lower()

                    asset = str(cells[i_asset]).strip().upper()
                    amount = _to_float(cells[i_qty])
                    eur_value = _to_float(cells[i_total])
                    if amount <= 0:
                        continue

                    # ----------------------------
                    #     METHOD + DIRECTION
                    # ----------------------------

                    # Direction/method: use allowed set {in, out, internal, unknown}
                    direction = "unknown"
                    method = (tx_type_raw or "unknown").strip() or "unknown"

                    # --- 1) REWARDS / STAKING (MUSS GANZ OBEN) ---
                    if (
                        "staking income" in tx_type
                        or "staking reward" in tx_type
                        or ("staking" in tx_type and "reward" in tx_type)
                        or ("earn" in tx_type and "reward" in tx_type)
                        or tx_type in {"reward", "rewards", "earning reward"}
                    ):
                        direction = "in"
                        method = "staking_reward"

                    # --- 2) Learning Reward ---
                    elif "learning reward" in tx_type:
                        direction = "in"
                        method = "learning_reward"

                    # --- 3) Staking Transfer ---
                    elif "retail staking transfer" in tx_type:
                        direction = "internal"
                        method = "staking_transfer"

                    # --- 4) Unstaking ---
                    elif "unstaking" in tx_type:
                        direction = "internal"
                        method = "unstake"

                    # --- 5) Convert / Trade / Swap ---
                    elif "convert" in tx_type or "trade" in tx_type or "swap" in tx_type:
                        direction = "unknown"
                        method = "convert"

                    # --- 6) Receive / Deposit ---
                    elif "receive" in tx_type or "deposit" in tx_type:
                        direction = "in"
                        method = "receive"

                    # --- 7) Send / Withdraw ---
                    elif "send" in tx_type or "withdraw" in tx_type:
                        direction = "out"
                        method = "send"

                    # Debug-Ausgabe für jede Methode
                    print("[DEBUG METHOD]", method, "| RAW:", tx_type_raw)

                    # Set from_addr/to_addr so derive_direction(wallet, from_addr, to_addr) matches
                    if wallet:
                        w = (wallet or "").lower().strip()
                        if direction == "in":
                            from_addr, to_addr = "coinbase", w
                        elif direction == "out":
                            from_addr, to_addr = w, "coinbase"
                        elif direction == "internal":
                            from_addr, to_addr = w, w
                        else:
                            from_addr, to_addr = "coinbase", "coinbase"
                        direction = derive_direction(wallet, from_addr, to_addr)
                    else:
                        from_addr, to_addr = "coinbase", "wallet"

                    # Data integrity: chain_id="coinbase"; token non-empty
                    token_val = (asset or "").strip().upper() or "UNKNOWN"
                    rr = RawRow(
                        source="coinbase_pdf",
                        tx_hash=f"cbpdf:{page_idx+1}:{row_idx+1}",
                        timestamp=ts,
                        dt_iso=iso_from_unix(ts),
                        from_addr=from_addr,
                        to_addr=to_addr,
                        token=token_val,
                        amount=amount,
                        direction=direction,
                        method=method,
                        fee_token=None,
                        fee_amount=0.0,
                        category="",
                        eur_value=eur_value,
                        chain_id="coinbase",
                        meta={
                            "tx_type_raw": tx_type_raw,
                            "pdf_page": page_idx + 1,
                            "pdf_path": str(path),
                        },
                    )
                    assert_direction_derivation(rr, wallet)
                    if DEBUG_VALIDATION:
                        validate_raw_row(rr)
                    rows.append(rr.to_dict())

    print(f"[coinbase_pdf_loader] {len(rows)} Rows aus {path.name} geladen.")
    return rows
