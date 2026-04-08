#!/usr/bin/env python3
"""
Debug tool: show why direction becomes "unknown".

Loads wallet data like raw_data_report, then for each row with direction=="unknown"
prints tx_hash, wallet, from_addr, to_addr, token, amount and the comparisons
(wallet == from_addr, wallet == to_addr). Limited to first 30 rows.

Usage:
  python -m taxtrack.tools.debug_direction --year 2025
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from taxtrack.tools.raw_data_report import discover_wallet_data, load_transactions


def _row_to_dict(r):
    if hasattr(r, "to_dict"):
        return r.to_dict()
    return r if isinstance(r, dict) else {}


def main():
    import argparse
    ap = argparse.ArgumentParser(description="Debug why direction is 'unknown' (first 30 rows).")
    ap.add_argument("--year", type=int, default=2025, help="Tax year")
    ap.add_argument("--root", default=None, help="Project root")
    ap.add_argument("--limit", type=int, default=30, help="Max unknown rows to print (default 30)")
    args = ap.parse_args()

    root = Path(args.root) if args.root else ROOT
    wallet_data = discover_wallet_data(root, args.year)
    if not wallet_data:
        print("No wallet data discovered.", file=sys.stderr)
        sys.exit(1)

    primary_wallet = (wallet_data[0].get("wallet") or "").lower().strip()
    rows = load_transactions(wallet_data, args.year)
    if not rows:
        print(f"No transactions in tax year {args.year}.", file=sys.stderr)
        sys.exit(1)

    # Normalize to dicts if needed
    dicts = [_row_to_dict(r) for r in rows]
    wallet_l = primary_wallet.lower()
    unknown_count = 0
    for r in dicts:
        direction = (r.get("direction") or "").strip().lower()
        if direction != "unknown":
            continue
        unknown_count += 1
        if unknown_count > args.limit:
            break

        from_addr = (r.get("from_addr") or r.get("from") or "").strip()
        to_addr = (r.get("to_addr") or r.get("to") or "").strip()
        from_l = from_addr.lower()
        to_l = to_addr.lower()

        print("---")
        print("tx_hash:    ", r.get("tx_hash") or "")
        print("wallet:     ", primary_wallet)
        print("from_addr:  ", from_addr)
        print("to_addr:    ", to_addr)
        print("token:      ", r.get("token") or "")
        print("amount:     ", r.get("amount"))
        print("wallet == from_addr:", wallet_l == from_l)
        print("wallet == to_addr:  ", wallet_l == to_l)
        print()

    total_unknown = sum(1 for r in dicts if (r.get("direction") or "").strip().lower() == "unknown")
    print(f"[debug_direction] Total rows: {len(dicts)} | unknown: {total_unknown} | printed: min({args.limit}, {total_unknown})", file=sys.stderr)


if __name__ == "__main__":
    main()
