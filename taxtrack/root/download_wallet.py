# taxtrack/root/download_wallet.py
"""
Download wallet transaction data from Etherscan-compatible APIs.

Stores CSVs in: data/inbox/<wallet>/<chain>/normal.csv, erc20.csv, internal.csv

Usage:
  python -m taxtrack download-wallet --wallet 0x123... --chains eth,arb
  python -m taxtrack.root.download_wallet --wallet 0x123... --chains eth,arb
  python -m taxtrack.root.download_wallet --wallet 0x123... --chains eth,arb --api-key YOUR_KEY
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from taxtrack.data.config.chain_config import CHAIN_CONFIG
from taxtrack.download.etherscan_fetcher import MissingExplorerAPIKeyError, download_chain

SUPPORTED_CHAINS = ["eth", "arb", "base", "op", "avax", "bnb", "ftm", "matic"]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Download wallet tx data (normal, ERC20, internal) from Etherscan-compatible APIs."
    )
    p.add_argument(
        "--wallet",
        required=True,
        help="Wallet address (e.g. 0x123...)",
    )
    p.add_argument(
        "--chains",
        default="eth,arb,base,op,avax,bnb,ftm,matic",
        help="Comma-separated chain ids (default: eth,arb,base,op,avax,bnb,ftm,matic)",
    )
    p.add_argument(
        "--api-key",
        default=None,
        help="Etherscan-compatible API key (or set ETHERSCAN_API_KEY)",
    )
    p.add_argument(
        "--inbox",
        default=None,
        help="Inbox root directory (default: taxtrack/data/inbox)",
    )
    p.add_argument(
        "--start-block",
        type=int,
        default=0,
        help="Start block (default: 0)",
    )
    p.add_argument(
        "--end-block",
        type=int,
        default=99999999,
        help="End block (default: 99999999)",
    )
    p.add_argument(
        "--max-retries",
        type=int,
        default=4,
        help="Max request retries per call (default: 4)",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    args = parse_args(argv)

    wallet = args.wallet.strip().lower()
    if not wallet.startswith("0x") or len(wallet) < 10:
        print("[ERROR] Invalid wallet address.")
        return

    # Chain list: normalize and filter to supported
    chain_list = [c.strip().lower() for c in args.chains.split(",") if c.strip()]
    chain_list = [c for c in chain_list if c in SUPPORTED_CHAINS and c in CHAIN_CONFIG]
    if not chain_list:
        print("[ERROR] No supported chains. Use one or more of:", ", ".join(SUPPORTED_CHAINS))
        return

    api_key = args.api_key or os.environ.get("ETHERSCAN_API_KEY", "").strip() or None

    # Output: data/inbox/<wallet>/<chain>/
    if args.inbox:
        inbox_root = Path(args.inbox)
    else:
        root = Path(__file__).resolve().parents[1]
        inbox_root = root / "data" / "inbox"
    wallet_dir = inbox_root / wallet

    print(f"[DOWNLOAD] Wallet: {wallet}")
    print(f"[DOWNLOAD] Chains: {', '.join(chain_list)}")
    print(f"[DOWNLOAD] Out:    {wallet_dir}")
    if api_key:
        print("[DOWNLOAD] API key: set")
    else:
        print("[DOWNLOAD] API key: not set (rate limits may apply)")

    successful_chains = []
    failed_chains = []
    chain_csv_counts = {}

    for chain_id in chain_list:
        chain_dir = wallet_dir / chain_id
        print(f"\n[CHAIN] {chain_id} -> {chain_dir}")
        try:
            paths = download_chain(
                chain_id=chain_id,
                address=wallet,
                output_dir=chain_dir,
                api_key=api_key,
                start_block=args.start_block,
                end_block=args.end_block,
                max_retries=args.max_retries,
            )
            csv_count = 0
            for name, path in paths.items():
                if name in {"normal", "erc20", "internal"}:
                    csv_count += 1
                print(f"  {name}.csv: {path} ({path.stat().st_size} bytes)")
            chain_csv_counts[chain_id] = csv_count
            successful_chains.append(chain_id)
            print(f"[DOWNLOAD][OK] chain={chain_id} csv_files={csv_count}")
        except MissingExplorerAPIKeyError as e:
            chain_csv_counts[chain_id] = 0
            failed_chains.append(chain_id)
            if not api_key:
                print(f"[DOWNLOAD][SKIPPED] chain={chain_id} reason=no_api_key")
            print(f"[DOWNLOAD][FAILED] chain={chain_id} error={e}")
            continue
        except Exception as e:
            chain_csv_counts[chain_id] = 0
            failed_chains.append(chain_id)
            print(f"[DOWNLOAD][FAILED] chain={chain_id} error={e}")
            continue

    print("\n[DOWNLOAD][SUMMARY]")
    print(f"  successful_chains: {successful_chains}")
    print(f"  failed_chains: {failed_chains}")
    print("  csv_files_per_chain:")
    for c in chain_list:
        print(f"    - {c}: {chain_csv_counts.get(c, 0)}")
    print("\n[DONE] Download complete.")


if __name__ == "__main__":
    main()
