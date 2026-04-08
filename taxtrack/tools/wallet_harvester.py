"""
Wallet Harvester

Runs the existing pipeline for many wallets, persists results to:
  taxtrack/data/harvest/<wallet>/<year>/classified.json
  taxtrack/data/harvest/<wallet>/<year>/gains.json

This is a surrounding data collection layer:
- does not modify pipeline logic
- does not change classification, swaps, or pricing
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from taxtrack.data.config.chain_config import CHAIN_CONFIG
from taxtrack.download.wallet_fetcher import (
    DataIngestFailedError,
    IngestOutcome,
    ensure_transactions_for_wallet_chain,
)
from taxtrack.root.pipeline import run_pipeline


DEFAULT_CHAINS = ("eth", "arb", "base", "op", "avax", "matic", "bnb", "ftm")


def read_wallet_list(path: Path) -> List[str]:
    wallets: List[str] = []
    if not path.exists():
        return wallets
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        wallets.append(s.lower())
    # de-dupe while preserving order
    seen = set()
    out = []
    for w in wallets:
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


def parse_chains(chains_csv: str) -> List[str]:
    chains = [c.strip().lower() for c in (chains_csv or "").split(",") if c.strip()]
    if not chains:
        chains = list(DEFAULT_CHAINS)
    # keep only chains we actually know how to fetch/label
    return [c for c in chains if c in CHAIN_CONFIG]


def _repo_taxtrack_root() -> Path:
    # taxtrack/tools -> taxtrack
    return Path(__file__).resolve().parents[1]


def _inbox_root() -> Path:
    return _repo_taxtrack_root() / "data" / "inbox"


def _harvest_root() -> Path:
    return _repo_taxtrack_root() / "data" / "harvest"


def harvest_wallet(
    wallet: str,
    year: int,
    chains: Sequence[str],
    *,
    api_key: Optional[str] = None,
    write_gains: bool = True,
    allow_zero_txs: bool = False,
) -> Dict[str, Any]:
    """
    Ensures CSVs exist (fetch when missing or empty), runs the full pipeline, writes JSON outputs.
    Returns the pipeline output dict.

    Raises DataIngestFailedError when any chain fails to ingest or when total raw rows is 0
    (unless allow_zero_txs=True).
    """
    wallet = (wallet or "").strip().lower()
    chains = [c.strip().lower() for c in chains if c.strip()]
    chains = [c for c in chains if c in CHAIN_CONFIG]

    inbox = _inbox_root()
    harvest_dir = _harvest_root() / wallet / str(year)
    harvest_dir.mkdir(parents=True, exist_ok=True)

    # Ensure CSVs for each chain (fail fast on API / empty data)
    outcomes: List[Tuple[str, IngestOutcome]] = []
    for chain_id in chains:
        o = ensure_transactions_for_wallet_chain(wallet, chain_id, inbox, api_key=api_key)
        outcomes.append((chain_id, o))
        print(
            f"[INGEST] {wallet} {chain_id}: {o.message} | "
            f"raw_rows={o.raw_row_total} | {o.api_status}"
        )

    failures = [(c, o.message) for c, o in outcomes if not o.ok]
    if failures:
        print("DATA INGEST STATUS: FAILED")
        for c, msg in failures:
            print(f"  chain {c}: {msg}")
        raise DataIngestFailedError(
            f"Ingest failed for {wallet}: " + "; ".join(f"{c}: {m}" for c, m in failures)
        )

    total_raw = sum(o.raw_row_total for _, o in outcomes)
    if total_raw == 0 and not allow_zero_txs:
        print(
            "DATA INGEST STATUS: FAILED — 0 raw transaction rows across all chains "
            "(no usable inbox data). Use --allow-zero-txs only if the wallet is genuinely empty."
        )
        raise DataIngestFailedError(
            f"No transaction rows ingested for {wallet}; refusing to run pipeline on empty data."
        )

    print(f"DATA INGEST STATUS: OK — {total_raw} raw rows total across {len(chains)} chain(s)")

    wallet_data: List[Dict[str, Any]] = []
    for chain_id in chains:
        wallet_data.append(
            {
                "wallet": wallet,
                "chain_id": chain_id,
                "base_dir": str(inbox / wallet / chain_id),
            }
        )

    config = {
        "output_dir": None,
        "primary_wallet": wallet,
        "chain_csv_source": {c: "existing files" for c in chains},
        "debug_info": {"wallet": wallet, "year": year},
    }

    out = run_pipeline(wallet_data, year, config)

    classified = out.get("classified_dicts") or []
    gains = out.get("economic_gains") if write_gains else None

    (harvest_dir / "classified.json").write_text(
        json.dumps(classified, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if write_gains:
        (harvest_dir / "gains.json").write_text(
            json.dumps(gains or [], ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return out


def wallets_from_cli(wallet_args: Optional[Sequence[str]]) -> List[str]:
    """Parse --wallet (repeatable); each value may be comma-separated. De-dupe, preserve order."""
    if not wallet_args:
        return []
    raw: List[str] = []
    for item in wallet_args:
        for part in (item or "").split(","):
            s = part.strip().lower()
            if not s or s.startswith("#"):
                continue
            raw.append(s)
    seen: set = set()
    out: List[str] = []
    for w in raw:
        if w not in seen:
            seen.add(w)
            out.append(w)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description="Harvest many wallets and write classified/gains JSON.")
    p.add_argument(
        "--wallet",
        action="append",
        default=None,
        metavar="ADDRESS",
        help="Harvest this wallet address (repeatable). Comma-separated in one flag is OK. "
        "If omitted, wallets are read from --wallet-list.",
    )
    p.add_argument(
        "--wallet-list",
        default="wallet_list.txt",
        help="Path to wallet list (one per line). Used only when --wallet is not given.",
    )
    p.add_argument("--chains", default=",".join(DEFAULT_CHAINS), help="Comma-separated chains")
    p.add_argument("--year", type=int, required=True, help="Tax year (e.g. 2025)")
    p.add_argument("--api-key", default=None, help="Explorer API key (or ETHERSCAN_API_KEY env)")
    p.add_argument("--no-gains", action="store_true", help="Do not write gains.json")
    p.add_argument(
        "--allow-zero-txs",
        action="store_true",
        help="Allow pipeline run when inbox has 0 raw rows (inactive wallet / testing only).",
    )
    args = p.parse_args()

    wallets = wallets_from_cli(args.wallet)
    if not wallets:
        wallets = read_wallet_list(Path(args.wallet_list))
    chains = parse_chains(args.chains)
    api_key = args.api_key or os.environ.get("ETHERSCAN_API_KEY", "").strip() or None

    if not wallets:
        print("[HARVEST] No wallets found in list.")
        return
    if not chains:
        print("[HARVEST] No valid chains.")
        return

    for w in wallets:
        try:
            harvest_wallet(
                w,
                args.year,
                chains,
                api_key=api_key,
                write_gains=not args.no_gains,
                allow_zero_txs=args.allow_zero_txs,
            )
            print(f"[HARVEST] wallet {w} done")
        except DataIngestFailedError as e:
            print(f"[HARVEST][INGEST FAILED] wallet {w}: {e}")
        except Exception as e:
            print(f"[HARVEST][ERROR] wallet {w} failed: {e!r}")


if __name__ == "__main__":
    main()

