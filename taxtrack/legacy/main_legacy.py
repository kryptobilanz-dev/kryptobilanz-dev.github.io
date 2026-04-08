"""
Legacy ZenTaxCore runner (single wallet alias + chain folder, simplified pipeline).

This was formerly ``taxtrack/root/main.py``. Prefer:

- ``python -m taxtrack customer ...`` (unified ``run_pipeline``)
- ``python -m taxtrack legacy ...`` for this flow

Unified pipeline: ``taxtrack.root.pipeline.run_pipeline``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path = [ROOT] + [p for p in sys.path if "Stefancore_TaxTrack_v0_3" not in p]

from taxtrack.loaders.auto_detect import load_auto
from taxtrack.rules.evaluate import evaluate_batch
from taxtrack.analyze.gains import compute_gains
from taxtrack.pdf.pdf_report import build_pdf

_TAXTRACK_ROOT = Path(__file__).resolve().parent.parent


def parse_args(argv: List[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="taxtrack-legacy")
    parser.add_argument("--wallet", required=True)
    parser.add_argument("--chain-id", default="")
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--out", default="data/out")
    return parser.parse_args(argv)


def main(argv: List[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    args = parse_args(argv)

    inbox = _TAXTRACK_ROOT / "data" / "inbox"
    wallet_alias = args.wallet
    wallets_file = _TAXTRACK_ROOT / "data" / "config" / "wallets.json"

    try:
        wallet_map = json.loads(wallets_file.read_text(encoding="utf-8"))
        real_wallet = wallet_map.get(wallet_alias, wallet_alias)
    except Exception:
        real_wallet = wallet_alias

    args.wallet = str(real_wallet).lower()

    chain_dir = inbox / wallet_alias / args.chain_id
    print("DEBUG chain_dir =", chain_dir)
    print("DEBUG chain_dir exists:", chain_dir.exists())
    print("DEBUG files found:", list(chain_dir.glob("*")))
    print("DEBUG csv files:", list(chain_dir.glob("*.csv")))

    if not chain_dir.exists():
        print(f"[ERROR] Chain-Ordner existiert nicht: {chain_dir}")
        return

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starte ZenTaxCore (legacy) für Wallet: {args.wallet}")

    # Same as historical main.py: load from inbox/<resolved wallet>/<chain_id>/
    wallet_dir = inbox / args.wallet
    chain_dir = wallet_dir / args.chain_id
    files = sorted(chain_dir.glob("*.csv"))

    raw_txs: List[Dict[str, Any]] = []

    for f in files:
        rows = load_auto(f, args.wallet)
        print(f"[LOAD] {f.name}: {len(rows)} Rows geladen.")
        if not rows:
            continue
        raw_txs.extend(rows)

        normed = []
        for r in raw_txs:
            if hasattr(r, "to_dict"):
                normed.append(r.to_dict())
            else:
                normed.append(r)
        raw_txs = normed

    print(f"[{datetime.now().strftime('%H:%M:%S')}] [LOAD] {len(raw_txs)} Roh-Transaktionen geladen.")

    y = args.year
    ts_start = int(datetime(y, 1, 1).timestamp())
    ts_end = int(datetime(y + 1, 1, 1).timestamp())

    filtered = [r for r in raw_txs if ts_start <= r["timestamp"] < ts_end]
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [FILTER] {len(filtered)} Transaktionen nach Jahr {y} gefiltert.")

    classified, debug_info = evaluate_batch(filtered, args.wallet)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [CLASSIFY] {len(classified)} Items klassifiziert.")

    gains, totals = compute_gains(classified)
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [GAINS] {len(gains)} steuerrelevante Events.")

    debug_info = {
        "wallet": args.wallet,
        "year": args.year,
        "totals": totals,
        "gains": gains,
    }
    out_file = out_dir / f"{args.wallet}_{args.year}_report.pdf"

    records_for_pdf = []
    for c in classified:
        if hasattr(c, "to_dict"):
            records_for_pdf.append(c.to_dict())
        else:
            records_for_pdf.append(c)

    pdf_path = build_pdf(records_for_pdf, totals, debug_info, str(out_file))

    print(f"[{datetime.now().strftime('%H:%M:%S')}] FERTIG! PDF gespeichert unter: {pdf_path}")


if __name__ == "__main__":
    main()
