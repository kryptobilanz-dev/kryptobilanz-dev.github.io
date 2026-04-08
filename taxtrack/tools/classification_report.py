#!/usr/bin/env python3
"""
Scan all classification results in the project and generate a report.

Discovers wallet data from:
- data/test_runs/<run>/
- data/inbox/<wallet>/<chain>/
- customers/<name>/inbox/

Loads transactions for the given tax year, runs evaluate_batch (classification only),
then aggregates: categories, unknown, method signatures for unknown, tokens in unknown,
protocols (counterparty) detected.

Usage:
  python -m taxtrack.tools.classification_report --year 2025 [--out docs/CLASSIFICATION_REPORT.md]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime

# Add parent so we can import taxtrack
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from taxtrack.loaders.auto_detect import load_auto
from taxtrack.rules.evaluate import evaluate_batch


def _row_timestamp(r) -> int:
    if hasattr(r, "timestamp"):
        return int(getattr(r, "timestamp") or 0)
    if isinstance(r, dict):
        return int(r.get("timestamp", 0) or 0)
    return 0


def _row_to_dict(r):
    if hasattr(r, "to_dict"):
        return r.to_dict()
    if isinstance(r, dict):
        return r
    return {}


def discover_wallet_data(root: Path, tax_year: int):
    """Build wallet_data list from test_runs, inbox, and customers."""
    wallet_data = []

    # 1) data/test_runs/<run>/
    test_runs = root / "data" / "test_runs"
    if test_runs.exists():
        for run_dir in test_runs.iterdir():
            if not run_dir.is_dir():
                continue
            wallets_file = run_dir / "wallets.json"
            if not wallets_file.exists():
                wallets_file = run_dir / "wallet.json"
            if not wallets_file.exists():
                continue
            try:
                data = json.loads(wallets_file.read_text(encoding="utf-8"))
            except Exception:
                continue
            if isinstance(data, list):
                if not data:
                    continue
                data = data[0]
            wallet = (data.get("wallet_address") or data.get("wallet") or "").lower()
            if not wallet:
                continue
            chains = data.get("chains") or [data.get("chain", "eth")]
            if isinstance(chains, str):
                chains = [chains]
            for ch in chains:
                ch = ch.strip().lower().replace("ethereum", "eth")
                chain_dir = run_dir / ch
                if not chain_dir.is_dir():
                    continue
                if not any((chain_dir / f).exists() for f in ("normal.csv", "erc20.csv", "internal.csv")):
                    continue
                wallet_data.append({
                    "wallet": wallet,
                    "chain_id": ch,
                    "base_dir": chain_dir,
                })

    # 2) data/inbox/<wallet>/<chain>/
    inbox = root / "data" / "inbox"
    if inbox.exists():
        for wallet_dir in inbox.iterdir():
            if not wallet_dir.is_dir():
                continue
            wallet = wallet_dir.name.lower()
            for chain_dir in wallet_dir.iterdir():
                if not chain_dir.is_dir():
                    continue
                chain_id = chain_dir.name.lower().replace("ethereum", "eth")
                files = [p for p in chain_dir.iterdir() if p.is_file() and p.suffix.lower() in (".csv", ".txt")]
                if not files:
                    continue
                wallet_data.append({
                    "wallet": wallet,
                    "chain_id": chain_id,
                    "files": [str(p) for p in files],
                })

    # 3) customers/<name>/inbox/
    customers_dir = root / "customers"
    if customers_dir.exists():
        for cust_dir in customers_dir.iterdir():
            if not cust_dir.is_dir():
                continue
            cfg_path = cust_dir / "config.json"
            if not cfg_path.exists():
                continue
            try:
                cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(cfg, dict) or "wallets" not in cfg or not isinstance(cfg["wallets"], list):
                continue
            for w in cfg["wallets"]:
                address = (w.get("address") or "").lower()
                label = w.get("label") or address or "wallet"
                if not address:
                    continue
                for inbox_name in (label, address):
                    wallet_inbox = cust_dir / "inbox" / inbox_name
                    if not wallet_inbox.exists() or not wallet_inbox.is_dir():
                        continue
                    for chain_dir in wallet_inbox.iterdir():
                        if not chain_dir.is_dir():
                            continue
                        chain_id = chain_dir.name.lower()
                        if chain_id.startswith(("eth", "mainnet")):
                            chain_id = "eth"
                        elif chain_id.startswith(("arb", "arbitrum")):
                            chain_id = "arb"
                        elif chain_id.startswith(("avax", "avalanche")):
                            chain_id = "avax"
                        elif chain_id.startswith(("op", "optimism")):
                            chain_id = "op"
                        elif chain_id.startswith(("base",)):
                            chain_id = "base"
                        elif chain_id.startswith(("bnb", "bsc", "binance")):
                            chain_id = "bnb"
                        elif chain_id.startswith(("matic", "polygon")):
                            chain_id = "matic"
                        files = [p for p in chain_dir.iterdir() if p.is_file() and p.suffix.lower() in (".csv", ".txt")]
                        if not files:
                            continue
                        wallet_data.append({
                            "wallet": address,
                            "chain_id": chain_id,
                            "files": [str(p) for p in files],
                        })
                    break

    return wallet_data


def load_transactions(wallet_data: list, tax_year: int):
    """Load and year-filter; return filtered_dicts."""
    raw_rows = []
    for item in wallet_data:
        wallet = item.get("wallet") or ""
        chain_id = item.get("chain_id") or "eth"
        files = item.get("files")
        if files is not None:
            for path in files:
                path = Path(path)
                if not path.exists():
                    continue
                try:
                    rows = load_auto(path, wallet=wallet, chain_id=chain_id)
                    raw_rows.extend(rows)
                except Exception as e:
                    print(f"[WARN] Load failed {path}: {e}", file=sys.stderr)
        else:
            base_dir = item.get("base_dir")
            if not base_dir:
                continue
            base_dir = Path(base_dir)
            for name in ("normal.csv", "erc20.csv", "internal.csv"):
                path = base_dir / name
                if not path.exists():
                    continue
                try:
                    rows = load_auto(path, wallet=wallet, chain_id=chain_id)
                    raw_rows.extend(rows)
                except Exception as e:
                    print(f"[WARN] Load failed {path}: {e}", file=sys.stderr)

    ts_from = int(datetime(tax_year, 1, 1).timestamp())
    ts_to = int(datetime(tax_year + 1, 1, 1).timestamp())
    filtered = [r for r in raw_rows if ts_from <= _row_timestamp(r) < ts_to]
    filtered_dicts = [_row_to_dict(r) for r in filtered]
    for r in filtered_dicts:
        if not r.get("chain_id") and isinstance(r.get("meta"), dict):
            r["chain_id"] = r["meta"].get("chain_id", "") or ""
        r.setdefault("chain_id", "")
    return filtered_dicts


def run_report(root: Path, tax_year: int) -> dict:
    """Discover data, load, classify, return aggregates."""
    wallet_data = discover_wallet_data(root, tax_year)
    if not wallet_data:
        return {"error": "No wallet data discovered", "wallet_data_count": 0}

    primary_wallet = (wallet_data[0].get("wallet") or "").lower()
    filtered_dicts = load_transactions(wallet_data, tax_year)
    if not filtered_dicts:
        return {
            "error": "No transactions in tax year",
            "wallet_data_count": len(wallet_data),
            "tax_year": tax_year,
        }

    # Suppress noisy evaluate_batch prints
    import io
    old_stdout = sys.stdout
    sys.stdout = io.StringIO()
    try:
        classified, _ = evaluate_batch(filtered_dicts, primary_wallet)
    finally:
        sys.stdout = old_stdout

    # Aggregate
    categories = defaultdict(int)
    unknown_rows = []
    method_for_unknown = defaultdict(int)
    tokens_in_unknown = defaultdict(int)
    protocols_detected = defaultdict(int)  # counterparty as protocol/label

    for it in classified:
        cat = (getattr(it, "category", "") or "").strip().lower()
        method = (getattr(it, "method", "") or "").strip() or "(empty)"
        token = (getattr(it, "token", "") or "").upper()
        counterparty = (getattr(it, "counterparty", "") or "").strip()

        categories[cat] += 1
        if counterparty:
            protocols_detected[counterparty] += 1

        if cat == "unknown":
            unknown_rows.append({
                "tx_hash": getattr(it, "tx_hash", ""),
                "method": method,
                "token": token,
                "direction": getattr(it, "direction", ""),
                "counterparty": counterparty,
            })
            method_for_unknown[method] += 1
            if token:
                tokens_in_unknown[token] += 1

    return {
        "tax_year": tax_year,
        "wallet_data_count": len(wallet_data),
        "rows_loaded": len(filtered_dicts),
        "rows_classified": len(classified),
        "categories": dict(sorted(categories.items(), key=lambda x: -x[1])),
        "unknown_count": len(unknown_rows),
        "unknown_rows_sample": unknown_rows[:100],
        "method_signatures_producing_unknown": dict(sorted(method_for_unknown.items(), key=lambda x: -x[1])),
        "tokens_in_unknown_transactions": dict(sorted(tokens_in_unknown.items(), key=lambda x: -x[1])),
        "protocols_detected": dict(sorted(protocols_detected.items(), key=lambda x: -x[1])),
    }


def format_report(data: dict) -> str:
    """Turn aggregate data into markdown report."""
    if data.get("error"):
        return f"# Classification Report\n\n**Error:** {data['error']}\n\nWallet data items: {data.get('wallet_data_count', 0)}\n"

    lines = [
        "# Classification Report",
        "",
        f"**Tax year:** {data['tax_year']}",
        f"**Wallet data sources:** {data['wallet_data_count']}",
        f"**Rows loaded (year-filtered):** {data['rows_loaded']}",
        f"**Rows classified:** {data['rows_classified']}",
        "",
        "---",
        "",
        "## 1. Categories that appear",
        "",
        "| Category | Count |",
        "|----------|-------|",
    ]
    for cat, count in data["categories"].items():
        lines.append(f"| {cat} | {count} |")
    lines.extend([
        "",
        "---",
        "",
        "## 2. Unknown categories",
        "",
        f"**Total rows classified as `unknown`:** {data['unknown_count']}",
        "",
    ])
    if data["unknown_count"] > 0:
        lines.append("These rows may represent DeFi behaviors not yet mapped to a specific category (e.g. swap, reward, lp_add).")
    lines.extend([
        "",
        "---",
        "",
        "## 3. Method signatures producing unknown classifications",
        "",
        "| Method | Count |",
        "|--------|-------|",
    ])
    for method, count in data["method_signatures_producing_unknown"].items():
        esc = method.replace("|", "\\|")
        lines.append(f"| {esc} | {count} |")
    if not data["method_signatures_producing_unknown"]:
        lines.append("| *(none)* | 0 |")
    lines.extend([
        "",
        "---",
        "",
        "## 4. Tokens involved in unknown transactions",
        "",
        "| Token | Count |",
        "|-------|-------|",
    ])
    for token, count in data["tokens_in_unknown_transactions"].items():
        lines.append(f"| {token} | {count} |")
    if not data["tokens_in_unknown_transactions"]:
        lines.append("| *(none)* | 0 |")
    lines.extend([
        "",
        "---",
        "",
        "## 5. Protocols detected",
        "",
        "Counterparty labels from contract_labeler (protocol/router/label) seen in classified rows:",
        "",
        "| Protocol / Label | Count |",
        "|------------------|-------|",
    ])
    for proto, count in data["protocols_detected"].items():
        esc = (proto or "(empty)").replace("|", "\\|")[:80]
        lines.append(f"| {esc} | {count} |")
    if not data["protocols_detected"]:
        lines.append("| *(none)* | 0 |")
    lines.extend([
        "",
        "---",
        "",
        "## Summary: DeFi behaviors not yet covered",
        "",
    ])
    # Brief interpretation
    unknown = data["unknown_count"]
    methods_unknown = list(data["method_signatures_producing_unknown"].keys())
    if unknown == 0:
        lines.append("- No rows were classified as `unknown` in the scanned data. Gray-zone methods (transfer, withdraw, etc.) were likely overridden by swap/LP/Pendle postprocessors or refined by counterparty.")
    else:
        lines.append("- **Unknown classifications** indicate method + counterparty did not match any refined category; they fell back to `unknown` or `transfer`.")
        lines.append("- **Method signatures producing unknown** should be reviewed: add contract labels or adjust _basic_category/_refine_category to map them to swap, reward, lp_add, etc.")
        lines.append("- **Tokens in unknown** often appear in transfers or unrecognized DEX/protocol flows; adding token or contract mapping can help.")
    lines.append("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(description="Generate classification report from all project data")
    ap.add_argument("--year", type=int, default=2025, help="Tax year to filter transactions")
    ap.add_argument("--out", default="", help="Write report to this path (default: stdout)")
    ap.add_argument("--root", default=None, help="Project root (default: taxtrack package parent)")
    args = ap.parse_args()
    root = Path(args.root) if args.root else ROOT

    data = run_report(root, args.year)
    report = format_report(data)

    if args.out:
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = root / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(report, encoding="utf-8")
        print(f"Report written to {out_path}", file=sys.stderr)
    else:
        print(report)


if __name__ == "__main__":
    main()
