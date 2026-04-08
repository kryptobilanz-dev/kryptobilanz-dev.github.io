from __future__ import annotations

"""
Preflight wallet scan (safe):
- Reads customer inbox CSVs
- Runs pipeline WITHOUT generating PDF/CSV output
- Writes a scan report JSON under customer/reports/

Goal: surface unknown tokens/contracts, valuation_missing, protocol gaps for mapping review.
Does NOT modify any mapping/config files.
"""

import argparse
import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from taxtrack.root.pipeline import run_pipeline


def _normalize_customer_slug(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_") or "customer"


def _resolve_customer_dir(customers_root: Path, name: str) -> Path:
    if not customers_root.is_dir():
        raise FileNotFoundError(f"Customers directory not found: {customers_root}")
    direct = customers_root / name
    if direct.is_dir():
        return direct
    slug = _normalize_customer_slug(name)
    cand = customers_root / slug
    if cand.is_dir():
        return cand
    for d in customers_root.iterdir():
        if d.is_dir() and _normalize_customer_slug(d.name) == slug:
            return d
    raise FileNotFoundError(
        f"No customer folder for {name!r} under {customers_root} (tried {slug!r})."
    )


def _load_customer_bundle(customer_dir: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Prefer wallets.json + info.json (create_customer.py layout).
    Fall back to legacy config.json.
    """
    info: Dict[str, Any] = {}
    info_path = customer_dir / "info.json"
    if info_path.exists():
        loaded = json.loads(info_path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            info = loaded

    wallets: List[Dict[str, Any]] = []
    wp = customer_dir / "wallets.json"
    if wp.exists():
        data = json.loads(wp.read_text(encoding="utf-8"))
        if isinstance(data, dict) and isinstance(data.get("wallets"), list):
            wallets = list(data["wallets"])

    if wallets:
        return info, wallets

    cfg_path = customer_dir / "config.json"
    if not cfg_path.exists():
        raise FileNotFoundError(f"Neither wallets.json nor config.json found in {customer_dir}")
    data = json.loads(cfg_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or not isinstance(data.get("wallets"), list):
        raise ValueError("Customer config must contain a 'wallets' list")
    return info, list(data["wallets"])


def _normalize_chain_id(name: str) -> str | None:
    n = (name or "").lower()
    if n.startswith(("eth", "mainnet")):
        return "eth"
    if n.startswith(("arb", "arbitrum")):
        return "arb"
    if n.startswith(("op", "optimism")):
        return "op"
    if n.startswith(("base",)):
        return "base"
    if n.startswith(("matic", "polygon", "pol")):
        return "matic"
    if n.startswith(("bnb", "bsc", "binance", "bnbchain")):
        return "bnb"
    if n.startswith(("avax", "avalanche")):
        return "avax"
    if n.startswith(("ftm", "fantom")):
        return "ftm"
    return None


def _as_float(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _is_unknown_token(tok: str) -> bool:
    t = (tok or "").strip().upper()
    return (not t) or t == "UNKNOWN" or t.startswith("UNKNOWN") or t.startswith("ERC20")


def build_scan_report(classified_dicts: List[Dict[str, Any]], debug_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pure aggregation: produce a safe review report.
    """
    tx_with_vm: set[str] = set()
    cp_protocol_hist = Counter()
    unknown_tokens = Counter()
    unknown_contracts = Counter()
    missing_price_tokens = Counter()

    for r in classified_dicts or []:
        txh = str(r.get("tx_hash") or "").strip()
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        cp_proto = str(meta.get("cp_protocol") or "").strip().lower() or "unknown"
        cp_protocol_hist[cp_proto] += 1

        tok = str(r.get("token") or "").strip().upper()
        eff = str(meta.get("effective_token") or "").strip().upper()
        token_contract = str(meta.get("token_contract") or "").strip().lower()

        if meta.get("valuation_missing") is True:
            if txh:
                tx_with_vm.add(txh)

        # Unknown token identity candidates (symbol or effective token)
        if _is_unknown_token(tok) or _is_unknown_token(eff):
            k = tok or eff or "UNKNOWN"
            unknown_tokens[k] += 1
            if token_contract:
                unknown_contracts[token_contract] += 1

        # Missing price risk: eur_value <=0 on non-zero amount
        amt = _as_float(r.get("amount"))
        eur = _as_float(r.get("eur_value"))
        if abs(amt) > 0 and eur <= 0:
            if tok:
                missing_price_tokens[tok] += 1

    top_unknown_tokens = [{"token": k, "count": int(v)} for k, v in unknown_tokens.most_common(50)]
    top_unknown_contracts = [{"contract": k, "count": int(v)} for k, v in unknown_contracts.most_common(50)]
    top_missing_price = [{"token": k, "count": int(v)} for k, v in missing_price_tokens.most_common(50)]

    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "wallet": debug_info.get("wallet") if isinstance(debug_info, dict) else None,
        "tax_year": debug_info.get("tax_year") if isinstance(debug_info, dict) else None,
        "tx_count": len({str(r.get("tx_hash") or "") for r in classified_dicts if r.get("tx_hash")}),
        "valuation_missing_txs": sorted(tx_with_vm)[:2000],
        "valuation_missing_count": len(tx_with_vm),
        "cp_protocol_histogram": dict(cp_protocol_hist.most_common(50)),
        "unknown_tokens_top": top_unknown_tokens,
        "unknown_contracts_top": top_unknown_contracts,
        "missing_price_tokens_top": top_missing_price,
        # pipeline already computes additional audit signals; pass through if present
        "pipeline_audit_report": (debug_info or {}).get("audit_report") if isinstance(debug_info, dict) else None,
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser("KryptoBilanz Preflight Scan")
    p.add_argument("--customer", required=True, help="Customer folder name/slug (under customers root)")
    p.add_argument("--workspace", default=None, help="Workspace root that contains a 'customers' folder")
    p.add_argument("--customers-root", default=None, help="Explicit customers root directory")
    p.add_argument("--customer-dir", default=None, help="Explicit customer directory path")
    p.add_argument("--year", type=int, required=True, help="Tax year")
    p.add_argument("--out", default=None, help="Optional output path for scan JSON")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    ROOT = Path(__file__).resolve().parents[1]
    if args.customer_dir:
        customer_dir = Path(args.customer_dir)
        if not customer_dir.is_dir():
            raise FileNotFoundError(f"customer_dir not found: {customer_dir}")
    else:
        if args.customers_root:
            customers_root = Path(args.customers_root)
        elif args.workspace:
            customers_root = Path(args.workspace) / "customers"
        else:
            customers_root = ROOT / "customers"
        customer_dir = _resolve_customer_dir(customers_root, args.customer)

    info, wallets = _load_customer_bundle(customer_dir)
    if not wallets:
        raise ValueError("Customer has no wallets configured")

    # Build wallet_data from inbox
    wallet_data: List[Dict[str, Any]] = []
    for w in wallets:
        address = (w.get("address") or "").lower()
        label = w.get("label") or address or "wallet"
        if not address:
            continue
        inbox_label = customer_dir / "inbox" / label
        inbox_addr = customer_dir / "inbox" / address
        wallet_inbox = inbox_label if inbox_label.exists() else inbox_addr if inbox_addr.exists() else None
        if wallet_inbox is None:
            continue
        for chain_dir in wallet_inbox.iterdir():
            if not chain_dir.is_dir():
                continue
            chain_id = _normalize_chain_id(chain_dir.name)
            if not chain_id:
                continue
            files = [f for f in chain_dir.iterdir() if f.is_file() and f.suffix.lower() in (".csv", ".txt")]
            if not files:
                continue
            wallet_data.append({"wallet": address, "chain_id": chain_id, "files": [str(p) for p in files]})

    if not wallet_data:
        raise ValueError("No inbox data found (no wallet/chain CSV files).")

    # SAFE run: no PDF/CSV outputs (output_dir omitted). Pipeline may still write harvest/audit JSON under repo.
    config = {
        "primary_wallet": wallets[0].get("address", "").lower() if wallets else "",
        "debug_info": {
            "customer": (info.get("name") or "").strip() if isinstance(info, dict) else "",
            "customer_name": (info.get("name") or "").strip() if isinstance(info, dict) else "",
            "customer_address": (info.get("address") or "") if isinstance(info, dict) else "",
            "year": int(args.year),
            "wallet_count": len(wallets),
            "mode": "preflight_scan",
        },
        "skip_pipeline_consistency_check": False,
    }

    res = run_pipeline(wallet_data, int(args.year), config)
    classified_dicts = res.get("classified_dicts") or []
    debug_info = res.get("debug_info") or {}
    report = build_scan_report(classified_dicts, debug_info)

    reports_dir = customer_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    out_path = Path(args.out) if args.out else (reports_dir / f"preflight_scan_{int(args.year)}.json")
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[PREFLIGHT] wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

