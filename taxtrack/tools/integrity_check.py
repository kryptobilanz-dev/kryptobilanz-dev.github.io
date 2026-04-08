"""
Read-only integrity check on harvest outputs (classified + economic gains).

  python -m taxtrack.tools.integrity_check --wallet 0x... --year 2025
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple


def _root() -> Path:
    return Path(__file__).resolve().parents[1]


def _harvest_dir(wallet: str, year: int) -> Path:
    w = (wallet or "").strip().lower()
    return _root() / "data" / "harvest" / w / str(year)


def _load_json(path: Path) -> Any:
    if not path.is_file():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


PVG_DUP_CATS = frozenset({"swap", "sell", "position_exit", "lp_remove"})

MUST_GAIN_OUT = frozenset(
    {
        "lp_remove",
        "pendle_redeem",
        "restake_out",
        "sell",
        "vault_exit",
        "position_exit",
        "trade",
        "stable_swap",
    }
)

SKIP_INTERNAL = frozenset({"internal_transfer", "self_transfer"})


def run_checks(
    wallet: str,
    year: int,
) -> Tuple[List[str], List[str], List[str], str]:
    critical: List[str] = []
    warnings: List[str] = []
    passed: List[str] = []

    hdir = _harvest_dir(wallet, year)
    if not hdir.is_dir():
        critical.append(f"Harvest dir missing: {hdir}")
        return critical, warnings, passed, "NOT SAFE"

    classified = _load_json(hdir / "classified.json")
    economic = _load_json(hdir / "gains.json")
    tax_ready = _load_json(hdir / "economic_gains_tax_ready.json")
    tax_summary = _load_json(hdir / "tax_summary.json")

    if not isinstance(classified, list):
        critical.append("classified.json missing or not a list")
        classified = []
    if not isinstance(economic, list):
        critical.append("gains.json missing or not a list")
        economic = []

    # --- 1 DOUBLE COUNTING ---
    by_tx_pvg: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in economic:
        txh = str(row.get("tx_hash") or "").lower()
        cat = (row.get("category") or "").lower()
        if cat in PVG_DUP_CATS:
            by_tx_pvg[txh].append(row)

    dup_found = False
    for txh, rows in by_tx_pvg.items():
        if not txh:
            continue
        if len(rows) > 1:
            dup_found = True
            cats = [r.get("category") for r in rows]
            pnls = [r.get("pnl_eur", r.get("gain")) for r in rows]
            critical.append(
                f"[DOUBLE] tx {txh[:18]}… {len(rows)} PVG rows categories={cats} pnl={pnls}"
            )
    if not dup_found and economic:
        passed.append("No duplicate PVG economic rows per tx_hash")

    # --- 2 FEE CONSISTENCY ---
    class_fee_by_tx: Dict[str, float] = defaultdict(float)
    for r in classified:
        txh = str(r.get("tx_hash") or "").lower()
        try:
            class_fee_by_tx[txh] += float(r.get("fee_eur") or 0.0)
        except (TypeError, ValueError):
            pass

    econ_by_tx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in economic:
        txh = str(row.get("tx_hash") or "").lower()
        if txh:
            econ_by_tx[txh].append(row)

    fee_tol = 0.15
    for txh, erows in econ_by_tx.items():
        if not erows:
            continue
        csum = class_fee_by_tx.get(txh, 0.0)
        esum = sum(float(r.get("fees_eur") or 0.0) for r in erows)
        if len(erows) > 1 and esum > fee_tol:
            warnings.append(
                f"[FEE] tx {txh[:18]}… {len(erows)} economic rows combined fees_eur={esum:.2f} "
                f"(possible duplicate fee application)"
            )
        elif csum > 0.25 and esum < 0.01:
            warnings.append(
                f"[FEE] tx {txh[:18]}… classified fees ~{csum:.2f} € but economic fees_eur={esum:.2f}"
            )
        elif esum > csum + 1.0 and csum > 0:
            warnings.append(
                f"[FEE] tx {txh[:18]}… economic fees {esum:.2f} >> classified sum {csum:.2f}"
            )

    if economic and not any("[FEE]" in w for w in warnings):
        passed.append("No obvious fee duplication/mismatch vs classified (heuristic)")

    # --- 3 FIFO / meta ---
    neg_hints = 0
    fifo_notes = 0
    for r in classified:
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        if meta.get("negative_balance") or meta.get("shortfall"):
            neg_hints += 1
        note = (r.get("note") or "") + str(meta.get("fifo", ""))
        if "shortfall" in note.lower() or "negative" in note.lower():
            fifo_notes += 1
    if neg_hints or fifo_notes:
        warnings.append(
            f"[FIFO] classified rows hinting negative balance / shortfall: {neg_hints + fifo_notes}"
        )
    else:
        passed.append("No negative-balance hints in classified meta (limited signal)")

    # --- 4 PRICE ---
    bad_price_tx: Set[str] = set()
    for r in classified:
        txh = str(r.get("tx_hash") or "").lower()
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        try:
            ev = float(r.get("eur_value") or 0.0)
        except (TypeError, ValueError):
            ev = 0.0
        d = (r.get("direction") or "").lower()
        c = (r.get("category") or "").lower()
        if meta.get("valuation_missing"):
            bad_price_tx.add(txh)
        if ev <= 0 and d in ("out", "swap") and c not in SKIP_INTERNAL:
            bad_price_tx.add(txh)
    if bad_price_tx:
        warnings.append(
            f"[PRICE] {len(bad_price_tx)} tx_hash with valuation_missing or eur<=0 on out/swap "
            f"(sample: {', '.join(sorted(bad_price_tx)[:5])}…)"
        )
    else:
        passed.append("No obvious price flags on sampled logic")

    # --- 5 TAX CONSISTENCY ---
    if isinstance(tax_ready, list) and isinstance(tax_summary, dict):
        try:
            sum_gain = sum(
                float(r.get("gain") or 0.0)
                for r in tax_ready
                if r.get("included_in_annual_totals", True) is not False
            )
            total = float(tax_summary.get("total_gains_net_eur") or 0.0)
            if abs(sum_gain - total) > 1.0:
                critical.append(
                    f"[TAX] tax_ready sum(gain)={sum_gain:.2f} vs tax_summary.total_gains_net_eur={total:.2f} "
                    f"(diff {abs(sum_gain - total):.2f} €)"
                )
            else:
                passed.append("tax_ready net gains align with tax_summary (<=1 €)")
        except (TypeError, ValueError) as e:
            warnings.append(f"[TAX] Could not compare tax_ready vs summary: {e}")
    else:
        warnings.append(
            "[TAX] economic_gains_tax_ready.json or tax_summary.json not present — skip deep tax check"
        )

    # --- 6 DATA LOSS ---
    gains_tx: Set[str] = set()
    for row in economic:
        h = str(row.get("tx_hash") or "").lower()
        if h:
            gains_tx.add(h)

    swap_tx: Set[str] = set()
    disposal_tx: Set[str] = set()
    for r in classified:
        txh = str(r.get("tx_hash") or "").lower()
        if not txh:
            continue
        c = (r.get("category") or "").lower()
        d = (r.get("direction") or "").lower()
        if c in SKIP_INTERNAL or d == "internal":
            continue
        if c == "swap" and d == "swap":
            swap_tx.add(txh)
        if d == "out" and c in MUST_GAIN_OUT:
            disposal_tx.add(txh)

    miss_swap = sorted(swap_tx - gains_tx)
    if miss_swap:
        critical.append(
            f"[DATA LOSS] {len(miss_swap)} unified swap txs not in gains.json "
            f"(sample: {miss_swap[:6]})"
        )
    elif swap_tx:
        passed.append("All classified swap (direction=swap) txs appear in gains.json")

    miss_disp = sorted(disposal_tx - gains_tx)
    if miss_disp:
        warnings.append(
            f"[DATA LOSS] {len(miss_disp)} disposal-category outs not in gains.json "
            f"(sample: {miss_disp[:6]})"
        )

    # Verdict
    if critical:
        verdict = "NOT SAFE"
    elif len(warnings) >= 3:
        verdict = "PARTIAL"
    elif warnings:
        verdict = "PARTIAL"
    else:
        verdict = "SAFE"

    return critical, warnings, passed, verdict


def main() -> None:
    ap = argparse.ArgumentParser(description="TaxTrack harvest integrity check")
    ap.add_argument("--wallet", required=True, help="Wallet address (folder name under harvest)")
    ap.add_argument("--year", type=int, required=True, help="Tax year")
    args = ap.parse_args()

    crit, warn, ok, verdict = run_checks(args.wallet, args.year)

    print("SYSTEM INTEGRITY REPORT")
    print(f"Wallet: {args.wallet.lower()}  Year: {args.year}")
    print()

    print("--- Critical Issues ---")
    if crit:
        for line in crit:
            print("  ", line)
    else:
        print("  (none)")
    print()

    print("--- Warnings ---")
    if warn:
        for line in warn:
            print("  ", line)
    else:
        print("  (none)")
    print()

    print("--- Passed Checks ---")
    if ok:
        for line in ok:
            print("  ", line)
    else:
        print("  (none)")
    print()

    print("FINAL:", verdict)
    sys.exit(2 if verdict == "NOT SAFE" else 1 if verdict == "PARTIAL" else 0)


if __name__ == "__main__":
    main()
