# taxtrack/root/run_reference.py
# Reproduzierbarer Auswertungslauf für Wallet + Chain
# ------------------------------------------------------------
# ZenTaxCore Test Runner – stabiler Referenz-Run
# - economic_gains (§23 EStG)
# - reward_events (§22 Nr. 3 EStG)
# ------------------------------------------------------------

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any, List

from taxtrack.root.pipeline import run_pipeline


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser("ZenTaxCore Test Runner")
    p.add_argument("--run", required=True, help="Name unter data/test_runs/")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--chain", default="eth", help="z.B. eth oder eth,arb,op")
    return p.parse_args()


# ------------------------------------------------------------
# Wallet-Config laden (schema-tolerant)
# ------------------------------------------------------------

def load_wallet_config(run_dir: Path) -> Dict[str, Any]:
    for name in ("wallet.json", "wallets.json"):
        p = run_dir / name
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
            print(f"[TEST] Wallet config file: {p}")
            print(f"[TEST] Wallet config type: {type(data)}")

            if isinstance(data, list):
                if not data:
                    raise ValueError("wallets.json ist leer")
                data = data[0]

            wallet_address = data.get("wallet_address") or data.get("wallet")
            if not wallet_address:
                raise ValueError("Wallet-Adresse fehlt")

            chains = data.get("chains") or [data.get("chain", "eth")]

            return {
                "wallet_alias": data.get("wallet_alias", "test_wallet"),
                "wallet_address": wallet_address.lower(),
                "chains": chains,
            }

    raise FileNotFoundError("wallet.json / wallets.json nicht gefunden")


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------

def main():
    args = parse_args()

    ROOT = Path(__file__).resolve().parents[1]
    run_dir = ROOT / "data" / "test_runs" / args.run

    wallet_cfg = load_wallet_config(run_dir)
    wallet = wallet_cfg["wallet_address"]
    wallet_alias = wallet_cfg["wallet_alias"]

    print(f"[TEST] Wallet alias   : {wallet_alias}")
    print(f"[TEST] Wallet address : {wallet}")

    chains = [c.strip().lower() for c in args.chain.split(",") if c.strip()]

    # Require all three CSVs per chain (reference run contract)
    csvs = ["normal.csv", "erc20.csv", "internal.csv"]
    wallet_data: List[Dict[str, Any]] = []
    for chain in chains:
        chain_dir = run_dir / chain
        print(f"[TEST] Chain-Dir: {chain_dir}")
        for c in csvs:
            if not (chain_dir / c).exists():
                raise FileNotFoundError(f"Fehlt: {chain_dir / c}")
        wallet_data.append({
            "wallet": wallet,
            "chain_id": chain,
            "base_dir": chain_dir,
        })

    out_dir = ROOT / "data" / "out" / "test_runs"
    config = {
        "output_dir": out_dir,
        "report_label": f"{args.run}_{args.chain}_{args.year}",
        "primary_wallet": wallet,
        "debug_info": {
            "wallet": wallet,
            "wallet_alias": wallet_alias,
            "chain": args.chain,
            "year": args.year,
            "from": f"{args.year}-01-01",
            "to": f"{args.year}-12-31",
        },
        "pdf_filename": f"{args.run}_{args.chain}_{args.year}_report.pdf",
        "audit_filename": f"tax_audit_{args.year}.csv",
    }

    run_pipeline(wallet_data, args.year, config)
    print("[DONE] OK")


if __name__ == "__main__":
    main()
