"""
Batch Runner

Reads wallet_list.txt, runs wallet harvester per wallet, updates unknown registry,
prints progress and final top unknown report.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import List
import json

from taxtrack.tools.wallet_harvester import read_wallet_list, parse_chains, harvest_wallet
from taxtrack.tools.unknown_registry import load_registry, save_registry, update_registry_from_classified, print_top


def _repo_taxtrack_root() -> Path:
    return Path(__file__).resolve().parents[1]

def _harvest_root() -> Path:
    return _repo_taxtrack_root() / "data" / "harvest"


def _print_ambiguous_samples(
    wallets: List[str],
    year: int,
    reg_path: Path,
    *,
    top_n: int,
    sample_n: int = 3,
) -> None:
    """
    Optional debug: print a few sample transactions for top ambiguous entries.
    Reads harvested classified.json files (no pipeline changes).
    """
    try:
        reg = load_registry(reg_path)
        amb = reg.ambiguous_transfers if isinstance(reg.ambiguous_transfers, dict) else {}
        by_method = dict(amb.get("by_method") or {})
        by_protocol = dict(amb.get("by_protocol") or {})
        by_contract = dict(amb.get("by_contract") or {})
    except Exception:
        return

    def top_keys(m: dict) -> List[str]:
        return [k for k, _v in sorted(m.items(), key=lambda kv: (-int(kv[1]), kv[0]))[:top_n]]

    top_methods = set(top_keys(by_method))
    top_protocols = set(top_keys(by_protocol))
    top_contracts = set(top_keys(by_contract))

    if not (top_methods or top_protocols or top_contracts):
        return

    print("AMBIGUOUS TRANSFER SAMPLES")
    printed = 0
    for w in wallets:
        p = _harvest_root() / w / str(year) / "classified.json"
        if not p.exists():
            continue
        try:
            rows = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        for d in rows if isinstance(rows, list) else []:
            if (d.get("category") or "").lower() != "transfer":
                continue
            meta = d.get("meta") if isinstance(d.get("meta"), dict) else {}
            m = (d.get("method") or "").strip()
            proto = (meta.get("cp_protocol") or "").strip().lower()
            cp_addr = (meta.get("cp_addr") or "").strip().lower()
            if (m in top_methods) or (proto in top_protocols) or (cp_addr in top_contracts):
                print(
                    f"  tx={d.get('tx_hash')} chain={d.get('chain_id')} dir={d.get('direction')} "
                    f"method={m} proto={proto or '<none>'} cp={cp_addr or '<none>'}"
                )
                printed += 1
                if printed >= sample_n:
                    print()
                    return
    print()


def main() -> None:
    p = argparse.ArgumentParser(description="Batch-run many wallets and build unknown registry.")
    p.add_argument("--wallet-list", default="wallet_list.txt", help="Path to wallet_list.txt")
    p.add_argument("--chains", default="eth,arb,base,op,avax,matic,bnb,ftm", help="Comma-separated chains")
    p.add_argument("--year", type=int, required=True, help="Tax year (e.g. 2025)")
    p.add_argument("--api-key", default=None, help="Explorer API key (or ETHERSCAN_API_KEY env)")
    p.add_argument("--registry", default=None, help="Path to unknown_registry.json (default: taxtrack/data/registry/unknown_registry.json)")
    p.add_argument("--top", type=int, default=20, help="Top N to print at end")
    p.add_argument("--debug-ambiguous-samples", action="store_true", help="Print a few sample ambiguous transfer txs")
    args = p.parse_args()

    wallets = read_wallet_list(Path(args.wallet_list))
    chains = parse_chains(args.chains)
    api_key = args.api_key or os.environ.get("ETHERSCAN_API_KEY", "").strip() or None

    if not wallets:
        print("[BATCH] No wallets found.")
        return
    if not chains:
        print("[BATCH] No valid chains.")
        return

    reg_path = Path(args.registry) if args.registry else (_repo_taxtrack_root() / "data" / "registry" / "unknown_registry.json")
    reg = load_registry(reg_path)

    for w in wallets:
        try:
            out = harvest_wallet(w, args.year, chains, api_key=api_key, write_gains=True)
            cls = out.get("classified_dicts") or []
            reg = update_registry_from_classified(reg, cls, collect_unknown_only=True)
            save_registry(reg_path, reg)
            print(f"[HARVEST] wallet {w} done")
            print("[REGISTRY] updated")
        except Exception as e:
            print(f"[BATCH][ERROR] wallet {w} failed: {e!r}")
            continue

    print_top(reg, top_n=int(args.top))
    if args.debug_ambiguous_samples:
        _print_ambiguous_samples(wallets, int(args.year), reg_path, top_n=int(args.top), sample_n=5)
    print(f"[REGISTRY] file: {reg_path}")


if __name__ == "__main__":
    main()

