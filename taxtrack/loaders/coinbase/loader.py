import re
from pathlib import Path
from dateutil import parser as dtparser

from taxtrack.schemas.RawRow import RawRow
from taxtrack.utils.time import iso_from_unix
from taxtrack.utils.direction import derive_direction, assert_direction_derivation
from taxtrack.validation.raw_row import validate_raw_row, DEBUG_VALIDATION
from taxtrack.prices.provider_master import price_provider
from taxtrack.loaders.coinbase.coinbase_rules import apply_coinbase_rules


def load_coinbase(path: Path, wallet: str = ""):
    """
    UNIVERSAL COINBASE LOADER (2024+)
    Funktioniert mit Rewards-, Staking- und Transaction-Exports,
    selbst wenn Spalten durch Kommas, €-Werte oder fehlende Quotes durcheinandergehen.
    """

    raw = path.read_text(encoding="utf-8", errors="ignore").splitlines()

    # 1) remove empty / metadata lines
    lines = []
    for l in raw:
        if not l.strip():
            continue
        if l.startswith("Transactions"):
            continue
        if l.startswith("User,"):
            continue
        lines.append(l)

    # 2) find header
    header_line = None
    for l in lines:
        if "Transaction Type" in l and "Timestamp" in l:
            header_line = l
            break

    if header_line is None:
        raise ValueError("Kein gültiger Coinbase-Header gefunden!")

    headers = header_line.split(",")

    # 3) read data lines after header
    data = lines[lines.index(header_line)+1:]

    out = []

    for l in data:
        # split only on top-level commas → CUSTOM REGEX
        cols = re.split(r',(?![^"]*")', l)

        # normalize length
        if len(cols) < len(headers):
            cols += [""] * (len(headers) - len(cols))

        row = dict(zip(headers, cols))

        ts_raw = row.get("Timestamp")
        if not ts_raw:
            continue

        try:
            ts = int(dtparser.parse(ts_raw).timestamp())
        except:
            continue

        # numeric parsing
        def to_float(x: str):
            if not x:
                return 0.0
            x = x.replace("€","").replace(",","").strip()
            try:
                return float(x)
            except:
                return 0.0

        action = (row.get("Transaction Type") or "").lower()
        token  = (row.get("Asset") or "").upper()

        amount = to_float(row.get("Quantity Transacted"))
        fee = to_float(row.get("Fees and/or Spread"))

        if amount <= 0:
            # Keep behavior aligned with swap/FIFO engines: only positive token movements become RawRows.
            continue

        if fee > 0:
            fee = -fee

        print(f"[VALUE CALC] {token} amount={amount} timestamp={ts} chain=")
        price = price_provider.get_eur_price(token, ts)
        eur_value = round(price * amount, 2)

        # Data integrity: chain_id always "coinbase"; direction from derive_direction(wallet, from_addr, to_addr)
        tx_hash = (row.get("ID") or "").strip()
        if not tx_hash:
            tx_hash = f"coinbase:{ts}:{len(out)}"
        token_val = (token or "UNKNOWN").strip().upper() or "UNKNOWN"
        method_val = (action or "unknown").strip() or "unknown"

        is_in = any(x in action for x in ["reward", "receive", "staking"])
        if wallet:
            from_addr = "coinbase" if is_in else (wallet or "").lower().strip()
            to_addr = (wallet or "").lower().strip() if is_in else "coinbase"
            direction = derive_direction(wallet, from_addr, to_addr)
        else:
            from_addr = "coinbase"
            to_addr = "wallet"
            direction = "in" if is_in else "out"

        rr = RawRow(
            source="coinbase",
            tx_hash=tx_hash,
            timestamp=ts,
            dt_iso=iso_from_unix(ts),
            from_addr=from_addr,
            to_addr=to_addr,
            token=token_val,
            amount=amount,
            direction=direction,
            method=method_val,
            fee_token="EUR",
            fee_amount=fee,
            category="",
            eur_value=eur_value,
            chain_id="coinbase",
            meta={"note": row.get("Notes") or ""}
        )
        assert_direction_derivation(rr, wallet)
        if DEBUG_VALIDATION:
            validate_raw_row(rr)
        out.append(rr)

    dict_rows = [r.to_dict() for r in out]

    # --- DUP DEBUG (Phase 1): Counts + Duplicate dump ---
    from collections import defaultdict
    import csv
    from pathlib import Path

    total_rows = len(dict_rows)

    dup_map = defaultdict(list)
    for r in dict_rows:
        key = (
            r.get("timestamp"),
            (r.get("token") or "").upper(),
            r.get("amount"),
            r.get("fee_amount"),
            (r.get("method") or "").lower(),
            (r.get("tx_hash") or ""),
        )
        dup_map[key].append(r)

    unique_rows = []
    duplicates = []
    for key, group in dup_map.items():
        if len(group) == 1:
            unique_rows.append(group[0])
        else:
            duplicates.extend(group)

    out_path = Path("data/out/dup_debug.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["timestamp", "token", "amount", "fee_amount", "method", "tx_hash"]
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for d in duplicates:
            writer.writerow({k: d.get(k, "") for k in fieldnames})

    print(f"[DUP_DEBUG] Total: {total_rows} | Unique: {len(unique_rows)} | Duplicates: {len(duplicates)}")
    print(f"[DUP_DEBUG] Wrote: {out_path}")

    return apply_coinbase_rules(unique_rows)

