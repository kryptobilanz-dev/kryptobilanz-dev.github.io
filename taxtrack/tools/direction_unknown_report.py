#!/usr/bin/env python3
"""
Analyze rows where derive_direction(wallet, from_addr, to_addr) returns "unknown".

Loads rows like raw_data_report, recomputes direction, keeps only "unknown",
then aggregates by method, contract (to_addr), token, and (method + contract + token).
Shows top 20 per category and 5 sample rows per cluster.

Usage:
  python -m taxtrack.tools.direction_unknown_report --year 2025
"""

from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from taxtrack.tools.raw_data_report import discover_wallet_data, load_transactions
from taxtrack.utils.direction import derive_direction


def _row_to_dict(r):
    if hasattr(r, "to_dict"):
        return r.to_dict()
    return r if isinstance(r, dict) else {}


def _get(r, key):
    if key in r:
        return r[key]
    if key == "from_addr" and "from" in r:
        return r["from"]
    if key == "to_addr" and "to" in r:
        return r["to"]
    return None


def run_report(root: Path, tax_year: int) -> dict:
    wallet_data = discover_wallet_data(root, tax_year)
    if not wallet_data:
        return {"error": "No wallet data discovered", "unknown_count": 0}

    primary_wallet = (wallet_data[0].get("wallet") or "").lower().strip()
    rows = load_transactions(wallet_data, tax_year)
    if not rows:
        return {"error": "No transactions in tax year", "unknown_count": 0}

    dicts = [_row_to_dict(r) for r in rows]
    unknown_rows = []
    for r in dicts:
        if not r:
            continue
        from_addr = (_get(r, "from_addr") or r.get("from") or "").strip()
        to_addr = (_get(r, "to_addr") or r.get("to") or "").strip()
        direction = derive_direction(primary_wallet, from_addr, to_addr)
        if direction == "unknown":
            unknown_rows.append(r)

    # A) by method
    by_method = defaultdict(int)
    for r in unknown_rows:
        m = (r.get("method") or "").strip() or "(empty)"
        by_method[m] += 1
    by_method = sorted(by_method.items(), key=lambda x: -x[1])[:20]

    # B) by contract (to_addr)
    by_contract = defaultdict(int)
    for r in unknown_rows:
        c = (_get(r, "to_addr") or r.get("to") or "").strip() or "(empty)"
        by_contract[c] += 1
    by_contract = sorted(by_contract.items(), key=lambda x: -x[1])[:20]

    # C) by token
    by_token = defaultdict(int)
    for r in unknown_rows:
        t = (r.get("token") or "").strip().upper() or "(empty)"
        by_token[t] += 1
    by_token = sorted(by_token.items(), key=lambda x: -x[1])[:20]

    # D) by (method + contract + token); collect rows per cluster for samples
    cluster_key = lambda r: (
        (r.get("method") or "").strip() or "(empty)",
        (_get(r, "to_addr") or r.get("to") or "").strip() or "(empty)",
        (r.get("token") or "").strip().upper() or "(empty)",
    )
    by_cluster = defaultdict(list)
    for r in unknown_rows:
        by_cluster[cluster_key(r)].append(r)
    cluster_counts = sorted(
        [(k, len(v)) for k, v in by_cluster.items()],
        key=lambda x: -x[1],
    )[:20]
    cluster_samples = {k: v[:5] for k, v in by_cluster.items()}

    return {
        "tax_year": tax_year,
        "wallet": primary_wallet,
        "total_rows": len(dicts),
        "unknown_count": len(unknown_rows),
        "by_method": by_method,
        "by_contract": by_contract,
        "by_token": by_token,
        "cluster_counts": cluster_counts,
        "cluster_samples": cluster_samples,
    }


def format_report(data: dict) -> str:
    if data.get("error"):
        return f"# Direction Unknown Report\n\n**Error:** {data['error']}\n\n"

    lines = [
        "# Direction Unknown Report",
        "",
        f"**Tax year:** {data['tax_year']}",
        f"**Wallet (primary):** {(data.get('wallet') or '')[:50]}{'...' if len(data.get('wallet') or '') > 50 else ''}",
        f"**Total rows loaded:** {data['total_rows']}",
        f"**Rows with direction = unknown:** {data['unknown_count']}",
        "",
        "---",
        "",
        "## A) By method (top 20)",
        "",
        "| method | count |",
        "|--------|-------|",
    ]
    for method, count in data["by_method"]:
        esc = (method or "(empty)").replace("|", "\\|")[:60]
        lines.append(f"| {esc} | {count} |")
    if not data["by_method"]:
        lines.append("| *(none)* | 0 |")

    lines.extend([
        "",
        "---",
        "",
        "## B) By contract / to_addr (top 20)",
        "",
        "| contract | count |",
        "|----------|-------|",
    ])
    for contract, count in data["by_contract"]:
        c = (contract or "(empty)")[:50]
        lines.append(f"| {c} | {count} |")
    if not data["by_contract"]:
        lines.append("| *(none)* | 0 |")

    lines.extend([
        "",
        "---",
        "",
        "## C) By token (top 20)",
        "",
        "| token | count |",
        "|-------|-------|",
    ])
    for token, count in data["by_token"]:
        lines.append(f"| {token} | {count} |")
    if not data["by_token"]:
        lines.append("| *(none)* | 0 |")

    lines.extend([
        "",
        "---",
        "",
        "## D) By cluster: method + contract + token (top 20)",
        "",
        "| method | contract | token | count |",
        "|--------|----------|-------|-------|",
    ])
    for (method, contract, token), count in data["cluster_counts"]:
        me = (method or "(empty)").replace("|", "\\|")[:35]
        co = (contract or "(empty)")[:40]
        to = (token or "(empty)").replace("|", "\\|")[:15]
        lines.append(f"| {me} | {co} | {to} | {count} |")
    if not data["cluster_counts"]:
        lines.append("| *(none)* | | | 0 |")

    lines.extend([
        "",
        "---",
        "",
        "## Sample rows (5 per cluster, top 20 clusters)",
        "",
    ])
    for (method, contract, token), count in data["cluster_counts"]:
        samples = data["cluster_samples"].get((method, contract, token), [])
        header = f"### `{method[:40]}` | `{contract[:36]}` | `{token}`"
        lines.append(header)
        lines.append("")
        lines.append("| tx_hash | method | token | from_addr | to_addr | amount | chain_id |")
        lines.append("|---------|--------|-------|-----------|---------|--------|----------|")
        for r in samples:
            tx = (r.get("tx_hash") or "")[:18] + (".." if len(r.get("tx_hash") or "") > 18 else "")
            m = (r.get("method") or "").replace("|", "\\|")[:20]
            t = (r.get("token") or "").replace("|", "\\|")[:10]
            fa = (_get(r, "from_addr") or r.get("from") or "")[:18] + (".." if len(_get(r, "from_addr") or r.get("from") or "") > 18 else "")
            ta = (_get(r, "to_addr") or r.get("to") or "")[:18] + (".." if len(_get(r, "to_addr") or r.get("to") or "") > 18 else "")
            amt = r.get("amount", "")
            ch = (r.get("chain_id") or "").strip()[:10]
            lines.append(f"| {tx} | {m} | {t} | {fa} | {ta} | {amt} | {ch} |")
        lines.append("")

    return "\n".join(lines)


def main():
    import argparse
    ap = argparse.ArgumentParser(description="Report rows where direction is 'unknown' (derive_direction).")
    ap.add_argument("--year", type=int, default=2025, help="Tax year")
    ap.add_argument("--out", default="", help="Write markdown to this path (default: stdout)")
    ap.add_argument("--root", default=None, help="Project root")
    args = ap.parse_args()

    root = Path(args.root) if args.root else ROOT
    data = run_report(root, args.year)
    report = format_report(data)

    if args.out:
        path = Path(args.out)
        if not path.is_absolute():
            path = root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(report, encoding="utf-8")
        print(f"Report written to {path}", file=sys.stderr)
    else:
        try:
            print(report)
        except UnicodeEncodeError:
            sys.stdout.buffer.write(report.encode("utf-8"))
            sys.stdout.buffer.write(b"\n")

    if data.get("unknown_count", 0) > 0:
        print(f"Unknown direction count: {data['unknown_count']}", file=sys.stderr)


if __name__ == "__main__":
    main()
