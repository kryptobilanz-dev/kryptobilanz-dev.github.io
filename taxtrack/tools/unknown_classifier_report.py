#!/usr/bin/env python3
"""
Analyze all transactions classified as "unknown" to understand which DeFi
interactions are not yet covered by the classification engine.

Reuses: discover_wallet_data(), load_transactions(), evaluate_batch().
Does NOT change classification logic; analysis only.

Usage:
  python -m taxtrack.tools.unknown_classifier_report --year 2025 [--out docs/UNKNOWN_CLASSIFICATION_REPORT.md]
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from taxtrack.tools.classification_report import discover_wallet_data, load_transactions
from taxtrack.rules.evaluate import evaluate_batch


def _item_to_row(ci) -> dict:
    """ClassifiedItem to dict for aggregation and samples."""
    return {
        "tx_hash": getattr(ci, "tx_hash", "") or "",
        "method": (getattr(ci, "method", "") or "").strip() or "(empty)",
        "token": (getattr(ci, "token", "") or "").strip().upper() or "",
        "amount": getattr(ci, "amount", 0) or 0,
        "from_addr": getattr(ci, "from_addr", "") or "",
        "to_addr": getattr(ci, "to_addr", "") or "",
        "contract": getattr(ci, "to_addr", "") or "",  # contract = to_addr for reporting
        "counterparty": getattr(ci, "counterparty", "") or "",
    }


def run_analysis(root: Path, tax_year: int) -> dict:
    """Load data, classify, filter unknown, return aggregates and samples."""
    wallet_data = discover_wallet_data(root, tax_year)
    if not wallet_data:
        return {"error": "No wallet data discovered", "wallet_data_count": 0, "tax_year": tax_year}

    primary_wallet = (wallet_data[0].get("wallet") or "").lower()
    filtered_dicts = load_transactions(wallet_data, tax_year)
    if not filtered_dicts:
        return {
            "error": "No transactions in tax year",
            "wallet_data_count": len(wallet_data),
            "tax_year": tax_year,
        }

    # Suppress evaluate_batch prints
    import io
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        classified, _ = evaluate_batch(filtered_dicts, primary_wallet)
    finally:
        sys.stdout = old_stdout

    unknown = [ci for ci in classified if (getattr(ci, "category", "") or "").strip().lower() == "unknown"]
    rows = [_item_to_row(ci) for ci in unknown]

    # A) Unknown by method signature
    by_method = defaultdict(int)
    for r in rows:
        by_method[r["method"]] += 1
    by_method = dict(sorted(by_method.items(), key=lambda x: -x[1]))

    # B) Unknown by contract address (to_addr)
    by_contract = defaultdict(int)
    for r in rows:
        addr = (r["contract"] or "").strip()
        if addr:
            by_contract[addr] += 1
    by_contract = dict(sorted(by_contract.items(), key=lambda x: -x[1]))

    # C) Unknown by token
    by_token = defaultdict(int)
    for r in rows:
        t = (r["token"] or "").strip()
        if t:
            by_token[t] += 1
    by_token = dict(sorted(by_token.items(), key=lambda x: -x[1]))

    # D) Unknown combinations (method + contract + token)
    combo_key = lambda r: (r["method"], (r["contract"] or "").strip(), (r["token"] or "").strip())
    by_combo = defaultdict(list)
    for r in rows:
        by_combo[combo_key(r)].append(r)
    combo_counts = [(k, len(v)) for k, v in by_combo.items()]
    combo_counts.sort(key=lambda x: -x[1])

    # E) Sample rows per cluster (each method+contract+token combo): up to 3 samples
    combo_samples = {}
    for (method, contract, token), group in by_combo.items():
        combo_samples[(method, contract, token)] = group[:3]

    return {
        "tax_year": tax_year,
        "wallet_data_count": len(wallet_data),
        "rows_loaded": len(filtered_dicts),
        "rows_classified": len(classified),
        "unknown_count": len(unknown),
        "by_method": by_method,
        "by_contract": by_contract,
        "by_token": by_token,
        "combo_counts": combo_counts,
        "combo_samples": combo_samples,
    }


def format_markdown(data: dict) -> str:
    """Generate markdown report."""
    if data.get("error"):
        return (
            "# Unknown Classification Report\n\n"
            f"**Error:** {data['error']}\n\n"
            f"Wallet data items: {data.get('wallet_data_count', 0)}\n"
        )

    lines = [
        "# Unknown Classification Report",
        "",
        f"**Tax year:** {data['tax_year']}",
        f"**Wallet data sources:** {data['wallet_data_count']}",
        f"**Rows loaded (year-filtered):** {data['rows_loaded']}",
        f"**Rows classified:** {data['rows_classified']}",
        f"**Rows classified as `unknown`:** {data['unknown_count']}",
        "",
        "---",
        "",
        "## A) Unknown by method signature",
        "",
        "| method_signature | count |",
        "|------------------|-------|",
    ]
    for method, count in data["by_method"].items():
        esc = (method or "(empty)").replace("|", "\\|")[:80]
        lines.append(f"| {esc} | {count} |")
    if not data["by_method"]:
        lines.append("| *(none)* | 0 |")

    lines.extend([
        "",
        "---",
        "",
        "## B) Unknown by contract address (counterparty)",
        "",
        "| contract_address | count |",
        "|------------------|-------|",
    ])
    for addr, count in data["by_contract"].items():
        lines.append(f"| {addr} | {count} |")
    if not data["by_contract"]:
        lines.append("| *(none)* | 0 |")

    lines.extend([
        "",
        "---",
        "",
        "## C) Unknown by token",
        "",
        "| token | count |",
        "|-------|-------|",
    ])
    for token, count in data["by_token"].items():
        lines.append(f"| {token} | {count} |")
    if not data["by_token"]:
        lines.append("| *(none)* | 0 |")

    lines.extend([
        "",
        "---",
        "",
        "## D) Unknown combinations (method + contract + token)",
        "",
        "| method | contract | token | count |",
        "|--------|----------|-------|-------|",
    ])
    for (method, contract, token), count in data["combo_counts"]:
        me = (method or "(empty)").replace("|", "\\|")[:40]
        co = (contract or "(empty)")[:42]
        to = (token or "(empty)").replace("|", "\\|")[:20]
        lines.append(f"| {me} | {co} | {to} | {count} |")
    if not data["combo_counts"]:
        lines.append("| *(none)* | | | 0 |")

    lines.extend([
        "",
        "---",
        "",
        "## E) Example transactions (up to 3 per cluster)",
        "",
    ])
    for (method, contract, token), _count in data["combo_counts"]:
        samples = data["combo_samples"].get((method, contract, token), [])
        header = f"### `{method or '(empty)'}` | `{(contract or '(empty)')[:42]}` | `{token or '(empty)'}`"
        lines.append(header)
        lines.append("")
        lines.append("| tx_hash | method | token | amount | from_addr | to_addr | contract |")
        lines.append("|---------|--------|-------|--------|-----------|---------|----------|")
        for r in samples:
            tx = (r.get("tx_hash") or "")[:20] + ("..." if len(r.get("tx_hash") or "") > 20 else "")
            m = (r.get("method") or "").replace("|", "\\|")[:25]
            t = (r.get("token") or "").replace("|", "\\|")[:12]
            amt = r.get("amount", 0)
            fa = (r.get("from_addr") or "")[:18] + (".." if len(r.get("from_addr") or "") > 18 else "")
            ta = (r.get("to_addr") or "")[:18] + (".." if len(r.get("to_addr") or "") > 18 else "")
            co = (r.get("contract") or "")[:18] + (".." if len(r.get("contract") or "") > 18 else "")
            lines.append(f"| {tx} | {m} | {t} | {amt} | {fa} | {ta} | {co} |")
        lines.append("")

    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(
        description="Analyze transactions classified as 'unknown' (DeFi not yet covered)."
    )
    ap.add_argument("--year", type=int, default=2025, help="Tax year")
    ap.add_argument("--out", default="", help="Output path (default: docs/UNKNOWN_CLASSIFICATION_REPORT.md)")
    ap.add_argument("--root", default=None, help="Project root (default: taxtrack package parent)")
    args = ap.parse_args()

    root = Path(args.root) if args.root else ROOT
    out_path = args.out or "docs/UNKNOWN_CLASSIFICATION_REPORT.md"

    data = run_analysis(root, args.year)
    report = format_markdown(data)

    path = Path(out_path)
    if not path.is_absolute():
        path = root / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report, encoding="utf-8")
    print(f"Report written to {path}", file=sys.stderr)
    if data.get("unknown_count", 0) > 0:
        print(f"Unknown count: {data['unknown_count']}", file=sys.stderr)


if __name__ == "__main__":
    main()
