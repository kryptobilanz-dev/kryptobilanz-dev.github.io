# taxtrack/root/run_wallet.py
"""
Full pipeline for a single wallet: auto-fetch if needed -> run_pipeline -> report.

Steps:
  1. For each chain, detect if normal.csv, erc20.csv, internal.csv are missing
     in data/inbox/<wallet>/<chain_id>/; if missing, fetch from Etherscan-compatible
     API and write the three CSVs (unless --skip-download).
  2. Call run_pipeline (load -> classify -> gains -> economic -> vault_exits -> report)
  3. PDF and audit CSV written to data/out/reports/<wallet>/

Usage:
  python -m taxtrack.root.run_wallet --wallet 0x123... --chains eth,arb,base,op --year 2025
  (CSVs are fetched automatically when missing; use --skip-download to use only existing CSVs)
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Any, Dict, List

from taxtrack.root.pipeline import run_pipeline
from taxtrack.download.wallet_fetcher import (
    count_inbox_chain_rows,
    ensure_transactions_for_wallet_chain,
)
from taxtrack.data.config.chain_config import CHAIN_CONFIG

SUPPORTED_CHAINS = ["eth", "arb", "base", "op", "avax"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run tax pipeline for a wallet; fetch transactions if CSVs missing.")
    p.add_argument("--wallet", required=True, help="Wallet address (e.g. 0x123...)")
    p.add_argument("--chains", default="eth,arb,base,op,avax", help="Comma-separated chains (default: eth,arb,base,op,avax)")
    p.add_argument("--year", type=int, required=True, help="Tax year (e.g. 2025)")
    p.add_argument("--api-key", default=None, help="Etherscan API key (or ETHERSCAN_API_KEY env)")
    p.add_argument("--skip-download", action="store_true", help="Do not fetch; use only existing inbox CSVs")
    p.add_argument(
        "--allow-zero-txs",
        action="store_true",
        help="Allow run when total raw rows are 0 (inactive wallets/testing).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    wallet = args.wallet.strip().lower()
    if not wallet.startswith("0x") or len(wallet) < 10:
        print("[ERROR] Invalid wallet address.")
        return

    ROOT = Path(__file__).resolve().parents[1]
    inbox_root = ROOT / "data" / "inbox"
    out_reports_root = ROOT / "data" / "out" / "reports"
    wallet_inbox = inbox_root / wallet
    wallet_out = out_reports_root / wallet

    chain_list = [c.strip().lower() for c in args.chains.split(",") if c.strip()]
    chain_list = [c for c in chain_list if c in SUPPORTED_CHAINS and c in CHAIN_CONFIG]
    if not chain_list:
        print("[ERROR] No supported chains. Use one or more of:", ", ".join(SUPPORTED_CHAINS))
        return

    # ---------- Step 1: Ensure transaction CSVs exist (fetch when missing / empty) ----------
    chain_csv_source: Dict[str, str] = {}
    if not args.skip_download:
        print("[1/3] Ensuring transaction data...")
        api_key = args.api_key or os.environ.get("ETHERSCAN_API_KEY", "").strip() or None
        ingest_outcomes = []
        for chain_id in chain_list:
            o = ensure_transactions_for_wallet_chain(wallet, chain_id, inbox_root, api_key=api_key)
            ingest_outcomes.append((chain_id, o))
            print(
                f"  {chain_id}: {o.message} | raw_rows={o.raw_row_total} | {o.api_status}"
            )
            if o.ok:
                chain_csv_source[chain_id] = "auto-fetched" if not o.skipped_use_existing else "existing files"
            else:
                chain_csv_source[chain_id] = "failed"

        failures = [(c, o.message) for c, o in ingest_outcomes if not o.ok]
        if failures:
            print("DATA INGEST STATUS: FAILED")
            for c, msg in failures:
                print(f"  chain {c}: {msg}")
            print("[ERROR] Fix API key / network or use --skip-download with valid inbox CSVs.")
            return

        total_raw = sum(o.raw_row_total for _, o in ingest_outcomes)
        if total_raw == 0 and not args.allow_zero_txs:
            print(
                "DATA INGEST STATUS: FAILED — 0 raw transaction rows. "
                "Use --allow-zero-txs only for genuinely empty wallets."
            )
            return
        print(f"DATA INGEST STATUS: OK — {total_raw} raw rows total")
    else:
        print("[1/3] Skip fetch (--skip-download); using existing inbox CSVs only")
        total_raw = 0
        for chain_id in chain_list:
            chain_csv_source[chain_id] = "existing files"
            total_raw += count_inbox_chain_rows(inbox_root, wallet, chain_id)
        if total_raw == 0 and not args.allow_zero_txs:
            print("DATA INGEST STATUS: FAILED — 0 raw rows in inbox (nothing to process).")
            return
        print(f"DATA INGEST STATUS: OK (existing files) — {total_raw} raw rows total")

    # ---------- Step 2: Build wallet_data and run pipeline ----------
    print("[2/3] Running tax pipeline...")
    wallet_data: List[Dict[str, Any]] = []
    for chain_id in chain_list:
        chain_dir = wallet_inbox / chain_id
        wallet_data.append({
            "wallet": wallet,
            "chain_id": chain_id,
            "base_dir": chain_dir,
        })

    config = {
        "output_dir": wallet_out,
        "primary_wallet": wallet,
        "chain_csv_source": chain_csv_source,
        "debug_info": {
            "wallet": wallet,
            "year": args.year,
            "from": f"{args.year}-01-01",
            "to": f"{args.year}-12-31",
        },
    }

    run_pipeline(wallet_data, args.year, config)

    print("[3/3] Done.")
    print("[DONE] OK")


if __name__ == "__main__":
    main()
