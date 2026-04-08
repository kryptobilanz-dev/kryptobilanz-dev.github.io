# taxtrack/root/main_evm.py
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from taxtrack.utils.cache import load_cache, save_cache

# ----------------------------------------------------------
# System Imports
# ----------------------------------------------------------
from taxtrack.loaders.evm_master_loader import load_evm_folder
from taxtrack.pdf.pdf_report import build_pdf

from taxtrack.analyze.swap_engine import extract_swaps
from taxtrack.analyze.economic_events import (
    economic_legs_from_swaps,
    fifo_from_economic_legs,
)

from taxtrack.utils.time_range import resolve_timerange


# ----------------------------------------------------------
# CLI
# ----------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(prog="zentaxcore-evm")

    parser.add_argument("--wallet", required=True)
    parser.add_argument("--chain-id", required=True)      # eth oder eth,arb,op
    parser.add_argument("--year", type=int)
    parser.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--out", default="taxtrack/data/out")

    return parser.parse_args()


def split_chains(chain_arg: str) -> list[str]:
    return [c.strip().lower() for c in chain_arg.split(",") if c.strip()]


# ----------------------------------------------------------
# MAIN
# ----------------------------------------------------------

def main():
    print("[DEBUG] main_evm.py STARTED")

    args = parse_args()

    ROOT = Path(__file__).resolve().parents[1]
    inbox = ROOT / "data" / "inbox"

    wallet_alias = args.wallet
    chains = split_chains(args.chain_id)

    print("[DEBUG] Chains parsed:", chains)

    if not chains:
        print("[ERROR] Keine Chains angegeben.")
        sys.exit(1)

    # ------------------------------------------------------
    # Wallet-Adresse
    # ------------------------------------------------------
    wallets_file = ROOT / "data" / "config" / "wallets.json"
    try:
        wallet_map = json.loads(wallets_file.read_text())
        real_wallet = wallet_map.get(wallet_alias, wallet_alias).lower()
    except Exception:
        real_wallet = wallet_alias.lower()

    # ------------------------------------------------------
    # Zeitraum
    # ------------------------------------------------------
    ts_start, ts_end = resolve_timerange(
        year=args.year,
        from_date=args.from_date,
        to_date=args.to_date,
    )

    print(
        "[INFO] Zeitraum:",
        datetime.fromtimestamp(ts_start),
        "→",
        datetime.fromtimestamp(ts_end),
    )

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("[DEBUG] Entering chain loop")

    # ------------------------------------------------------
    # Pro Chain ausführen
    # ------------------------------------------------------
    for chain in chains:
        print("\n" + "=" * 80)
        print(f"[RUN] Chain: {chain}")
        print("=" * 80)

        csv_folder = inbox / wallet_alias / chain
        print("[DEBUG] CSV folder:", csv_folder)

        if not csv_folder.exists():
            print(f"[WARN] Ordner fehlt, überspringe: {csv_folder}")
            continue

        # --------------------------------------------------
        # Loader + Cache
        # --------------------------------------------------
        from taxtrack.utils.cache import load_cache, save_cache

        cached = load_cache(real_wallet, chain, ts_start, ts_end)

        if cached:
            classified = cached["classified"]
            gains = cached["gains"]
            totals = cached["totals"]


        else:
            print("[CACHE] Miss – lade neu")
            result = load_evm_folder(
                folder=str(csv_folder),
                wallet=real_wallet,
                chain_id=chain,
                ts_start=ts_start,
                ts_end=ts_end,
            )

            raw_rows = result["raw"]
            classified = result["classified"]
            gains = result["gains"]
            totals = result["totals"]

            save_cache(
                real_wallet,
                chain,
                ts_start,
                ts_end,
                {
                    "classified": classified,
                    "gains": gains,
                    "totals": totals,
                }
            )



        if not cached:
            print(f"[INFO] RawRows     : {len(raw_rows)}")
        else:
            print(f"[INFO] RawRows     : (aus Cache übersprungen)")

        print(f"[INFO] Classified  : {len(classified)}")
        print(f"[INFO] Gains       : {len(gains)}")

        if not classified:
            print("[WARN] Keine Daten → kein PDF")
            continue

        # --------------------------------------------------
        # Swaps → Economic Events (nur ohne Cache!)
        # --------------------------------------------------
        if cached:
            print("[CACHE] Swaps/FIFO werden übersprungen (RawRows nicht im Cache)")
            sell_events = []
            inventory = {}
        else:
            swaps = extract_swaps(chain, raw_rows)
            legs = economic_legs_from_swaps(swaps)
            sell_events, inventory = fifo_from_economic_legs(legs)

            print(f"[INFO] Swaps        : {len(swaps)}")
            print(f"[INFO] Legs         : {len(legs)}")
            print(f"[INFO] SellEvents   : {len(sell_events)}")



        # --------------------------------------------------
        # PDF
        # --------------------------------------------------
        period_label = (
            f"{args.from_date}_to_{args.to_date}"
            if args.from_date and args.to_date
            else str(args.year)
        )

        out_file = out_dir / f"{wallet_alias}_{chain}_{period_label}_evm_report.pdf"

        records_for_pdf = [
            c.to_dict() if hasattr(c, "to_dict") else c
            for c in classified
        ]

        debug_info = {
            "wallet": real_wallet,
            "wallet_alias": wallet_alias,
            "chain": chain,
            "year": args.year,
            "from": args.from_date,
            "to": args.to_date,
            "totals": totals,
            "gains": gains,
            "sell_events": [e.__dict__ for e in sell_events],
            "inventory": [
                lot.__dict__
                for token_lots in inventory.values()
                for lot in token_lots
            ],
        }

        print(f"[INFO] Erstelle PDF: {out_file}")
        build_pdf(records_for_pdf, totals, debug_info, str(out_file))

    print("\n==========================================")
    print("  FERTIG")
    print("==========================================\n")


if __name__ == "__main__":
    main()
