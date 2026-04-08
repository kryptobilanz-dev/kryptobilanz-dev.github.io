#!/usr/bin/env python3
"""
Raw data debugging tool: analyze RawRow data BEFORE classification.

Reports:
  - Unique tokens, method signatures, contracts (to_addr), chains
  - Direction distribution
  - Missing fields statistics
  - Rows with missing required fields, zero amount, empty token

Usage:
  python -m taxtrack.tools.raw_data_report --year 2025 [--out report.md] [--root /path]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

# Project root for imports
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from taxtrack.validation.raw_row import REQUIRED_FIELDS
from taxtrack.utils.direction import derive_direction


def _row_to_dict(r: Any) -> Dict[str, Any]:
    if hasattr(r, "to_dict"):
        return r.to_dict()
    if isinstance(r, dict):
        return r
    return {}


def _get(r: Dict[str, Any], key: str) -> Any:
    if key in r:
        return r[key]
    if key == "from_addr" and "from" in r:
        return r["from"]
    if key == "to_addr" and "to" in r:
        return r["to"]
    return None


def _is_empty_value(val: Any, key: str) -> bool:
    if val is None:
        return True
    if key in ("timestamp",):
        try:
            return int(val) < 0
        except (TypeError, ValueError):
            return True
    if key in ("amount", "fee_amount", "eur_value"):
        try:
            return float(val) == 0
        except (TypeError, ValueError):
            return True
    if isinstance(val, str):
        return not val.strip()
    return False


def generate_raw_data_report(
    rows: List[Union[Any, Dict[str, Any]]],
    *,
    wallet: str = "",
    required_fields: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Analyze a list of RawRow objects or dict rows and produce a report.

    Returns a dict with:
      - total_rows
      - unique_tokens: sorted list
      - unique_methods: sorted list
      - unique_contracts: sorted list (to_addr)
      - unique_chains: sorted list
      - direction_distribution: { direction: count }
      - missing_fields_stats: { field: count } of rows where field is missing/empty
      - rows_missing_required_count, rows_missing_required_sample_indices
      - rows_zero_amount_count
      - rows_empty_token_count
    """
    required = required_fields or REQUIRED_FIELDS
    dicts = [_row_to_dict(r) for r in rows]

    tokens = set()
    methods = set()
    contracts = set()
    chains = set()
    direction_dist = defaultdict(int)
    missing_stats = defaultdict(int)
    rows_missing_required_indices = []
    zero_amount_count = 0
    empty_token_count = 0

    for i, r in enumerate(dicts):
        if not r:
            continue

        # Tokens
        tok = (r.get("token") or "").strip().upper()
        if tok:
            tokens.add(tok)
        else:
            empty_token_count += 1

        # Method
        meth = (r.get("method") or "").strip()
        if meth:
            methods.add(meth)
        else:
            missing_stats["method"] += 1

        # Contract (to_addr)
        to_a = (_get(r, "to_addr") or "").strip()
        if to_a:
            contracts.add(to_a)

        # Chain
        ch = (r.get("chain_id") or "").strip().lower()
        if ch:
            chains.add(ch)
        else:
            missing_stats["chain_id"] += 1

        # Direction: recompute from wallet/from_addr/to_addr (do not use stored row["direction"])
        from_addr = (_get(r, "from_addr") or r.get("from") or "").strip()
        to_addr = (_get(r, "to_addr") or r.get("to") or "").strip()
        d = derive_direction(wallet, from_addr, to_addr)
        direction_dist[d] += 1
        if not d:
            missing_stats["direction"] += 1

        # Amount
        try:
            amt = float(r.get("amount") or 0)
        except (TypeError, ValueError):
            amt = 0
        if amt == 0:
            zero_amount_count += 1

        # Missing required fields
        missing_here = []
        for f in required:
            val = _get(r, f) if f in ("from_addr", "to_addr") else r.get(f)
            if _is_empty_value(val, f):
                missing_stats[f] += 1
                missing_here.append(f)
        if missing_here:
            rows_missing_required_indices.append(i)

    return {
        "total_rows": len(dicts),
        "unique_tokens": sorted(tokens),
        "unique_methods": sorted(methods),
        "unique_contracts": sorted(contracts),
        "unique_chains": sorted(chains),
        "direction_distribution": dict(sorted(direction_dist.items(), key=lambda x: -x[1])),
        "missing_fields_stats": dict(missing_stats),
        "rows_missing_required_count": len(rows_missing_required_indices),
        "rows_missing_required_sample_indices": rows_missing_required_indices[:100],
        "rows_zero_amount_count": zero_amount_count,
        "rows_empty_token_count": empty_token_count,
    }


def format_report_text(data: Dict[str, Any]) -> str:
    """Plain-text report (counts and sorted lists)."""
    lines = [
        "Raw Data Report",
        "===============",
        "",
        f"Total rows: {data['total_rows']}",
        "",
        "---",
        "Unique tokens:",
    ]
    for t in data["unique_tokens"]:
        lines.append(f"  {t}")
    if not data["unique_tokens"]:
        lines.append("  (none)")
    lines.extend([
        "",
        "---",
        "Method signatures:",
    ])
    for m in data["unique_methods"]:
        lines.append(f"  {m}")
    if not data["unique_methods"]:
        lines.append("  (none)")
    lines.extend([
        "",
        "---",
        "Contracts (to_addr):",
    ])
    for c in data["unique_contracts"]:
        lines.append(f"  {c}")
    if not data["unique_contracts"]:
        lines.append("  (none)")
    lines.extend([
        "",
        "---",
        "Chains:",
    ])
    for ch in data["unique_chains"]:
        lines.append(f"  {ch}")
    if not data["unique_chains"]:
        lines.append("  (none)")
    lines.extend([
        "",
        "---",
        "Direction distribution:",
    ])
    for d, cnt in data["direction_distribution"].items():
        lines.append(f"  {d}: {cnt}")
    lines.extend([
        "",
        "---",
        "Missing fields (count of rows with missing/empty field):",
    ])
    for f, cnt in sorted(data["missing_fields_stats"].items(), key=lambda x: -x[1]):
        lines.append(f"  {f}: {cnt}")
    if not data["missing_fields_stats"]:
        lines.append("  (none)")
    lines.extend([
        "",
        "---",
        "Data quality:",
        f"  Rows with at least one missing required field: {data['rows_missing_required_count']}",
        f"  Rows with zero amount: {data['rows_zero_amount_count']}",
        f"  Rows with empty token: {data['rows_empty_token_count']}",
    ])
    if data["rows_missing_required_sample_indices"]:
        lines.append(f"  Sample row indices (missing required): {data['rows_missing_required_sample_indices'][:20]}")
    lines.append("")
    return "\n".join(lines)


def format_report_markdown(data: Dict[str, Any], title: str = "Raw Data Report") -> str:
    """Markdown report."""
    lines = [
        f"# {title}",
        "",
        f"**Total rows:** {data['total_rows']}",
        "",
        "---",
        "",
        "## 1. Unique tokens",
        "",
        "| Token |",
        "|-------|",
    ]
    for t in data["unique_tokens"]:
        esc = (t or "").replace("|", "\\|")
        lines.append(f"| {esc} |")
    if not data["unique_tokens"]:
        lines.append("| *(none)* |")
    lines.extend([
        "",
        "---",
        "",
        "## 2. Method signatures",
        "",
        "| Method |",
        "|--------|",
    ])
    for m in data["unique_methods"]:
        esc = (m or "").replace("|", "\\|")
        lines.append(f"| {esc} |")
    if not data["unique_methods"]:
        lines.append("| *(none)* |")
    lines.extend([
        "",
        "---",
        "",
        "## 3. Contracts (to_addr)",
        "",
        "| Address |",
        "|---------|",
    ])
    for c in data["unique_contracts"]:
        esc = (c or "").replace("|", "\\|")[:80]
        lines.append(f"| {esc} |")
    if not data["unique_contracts"]:
        lines.append("| *(none)* |")
    lines.extend([
        "",
        "---",
        "",
        "## 4. Chains",
        "",
        "| Chain |",
        "|-------|",
    ])
    for ch in data["unique_chains"]:
        lines.append(f"| {ch} |")
    if not data["unique_chains"]:
        lines.append("| *(none)* |")
    lines.extend([
        "",
        "---",
        "",
        "## 5. Direction distribution",
        "",
        "| Direction | Count |",
        "|-----------|-------|",
    ])
    for d, cnt in data["direction_distribution"].items():
        lines.append(f"| {d} | {cnt} |")
    lines.extend([
        "",
        "---",
        "",
        "## 6. Missing fields statistics",
        "",
        "Count of rows where the field is missing or empty.",
        "",
        "| Field | Count |",
        "|-------|-------|",
    ])
    for f, cnt in sorted(data["missing_fields_stats"].items(), key=lambda x: -x[1]):
        lines.append(f"| {f} | {cnt} |")
    if not data["missing_fields_stats"]:
        lines.append("| *(none)* | 0 |")
    lines.extend([
        "",
        "---",
        "",
        "## 7. Data quality",
        "",
        "| Issue | Count |",
        "|-------|-------|",
        f"| Rows with at least one missing required field | {data['rows_missing_required_count']} |",
        f"| Rows with zero amount | {data['rows_zero_amount_count']} |",
        f"| Rows with empty token | {data['rows_empty_token_count']} |",
        "",
    ])
    if data["rows_missing_required_sample_indices"]:
        sample = data["rows_missing_required_sample_indices"][:30]
        lines.append("Sample row indices (missing required fields): " + ", ".join(str(x) for x in sample))
        lines.append("")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI: discover wallet data and load transactions (same as classification_report)
# ---------------------------------------------------------------------------

def _row_timestamp(r) -> int:
    if hasattr(r, "timestamp"):
        return int(getattr(r, "timestamp") or 0)
    if isinstance(r, dict):
        return int(r.get("timestamp", 0) or 0)
    return 0


def discover_wallet_data(root: Path, tax_year: int) -> List[Dict[str, Any]]:
    """Build wallet_data list from test_runs, inbox, and customers."""
    wallet_data = []

    # 1) data/test_runs/<run>/
    test_runs = root / "data" / "test_runs"
    if test_runs.exists():
        for run_dir in test_runs.iterdir():
            if not run_dir.is_dir():
                continue
            for name in ("wallets.json", "wallet.json"):
                wallets_file = run_dir / name
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
                break

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
                files = [p for p in chain_dir.iterdir() if p.is_file() and p.suffix.lower() in (".csv", ".txt", ".pdf")]
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
                        files = [p for p in chain_dir.iterdir() if p.is_file() and p.suffix.lower() in (".csv", ".txt", ".pdf")]
                        if not files:
                            continue
                        wallet_data.append({
                            "wallet": address,
                            "chain_id": chain_id,
                            "files": [str(p) for p in files],
                        })
                    break

    return wallet_data


def load_transactions(wallet_data: List[Dict[str, Any]], tax_year: int) -> List[Dict[str, Any]]:
    """Load and year-filter; return list of row dicts."""
    from taxtrack.loaders.auto_detect import load_auto

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


def main():
    ap = argparse.ArgumentParser(
        description="Raw data report: analyze RawRow data before classification (tokens, methods, contracts, chains, missing fields)."
    )
    ap.add_argument("--year", type=int, default=2025, help="Tax year to filter transactions")
    ap.add_argument("--out", default="", help="Write report to this path (default: stdout)")
    ap.add_argument("--root", default=None, help="Project root (default: taxtrack package parent)")
    ap.add_argument("--markdown", action="store_true", help="Output markdown (default: plain text)")
    ap.add_argument("--json", action="store_true", help="Output raw report dict as JSON (no text report)")
    args = ap.parse_args()

    root = Path(args.root) if args.root else ROOT

    wallet_data = discover_wallet_data(root, args.year)
    if not wallet_data:
        print("No wallet data discovered.", file=sys.stderr)
        sys.exit(1)

    rows = load_transactions(wallet_data, args.year)
    if not rows:
        print(f"No transactions in tax year {args.year}.", file=sys.stderr)
        sys.exit(1)

    primary_wallet = (wallet_data[0].get("wallet") or "").lower().strip()
    data = generate_raw_data_report(rows, wallet=primary_wallet)

    if args.json:
        out = json.dumps(data, indent=2)
    elif args.markdown:
        data["tax_year"] = args.year
        out = format_report_markdown(data, title=f"Raw Data Report ({args.year})")
    else:
        out = format_report_text(data)

    if args.out:
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = root / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(out, encoding="utf-8")
        print(f"Report written to {out_path}", file=sys.stderr)
    else:
        # Avoid UnicodeEncodeError on Windows when stdout is cp1252
        try:
            print(out)
        except UnicodeEncodeError:
            sys.stdout.buffer.write(out.encode("utf-8"))
            sys.stdout.buffer.write(b"\n")


if __name__ == "__main__":
    main()
