from __future__ import annotations

"""
Customer runner for ZenTaxCore.

Goal:
- Allow processing multiple wallets that belong to a single customer.
- Reuse the unified pipeline (run_pipeline).

Directory structure (per customer):

    taxtrack/customers/<customer_slug>/
        info.json          # optional: name, address, year (create_customer.py)
        wallets.json       # optional: { "wallets": [...] }
        config.json        # legacy: name, tax_year, wallets
        inbox/
            <wallet_label_or_address>/
                <chain_folder>/   # e.g. eth, arb, bnb, op, base, polygon
                    *.csv / *.txt (Etherscan-style exports)
        reports/
            tax_report_<year>.pdf
            tax_audit_<year>.csv
"""

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Dict, Any, List, Tuple

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
        f"No customer folder for {name!r} under {customers_root} "
        f"(tried {slug!r})."
    )


def _load_customer_bundle(customer_dir: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """
    Prefer wallets.json + info.json (create_customer.py layout).
    Fall back to legacy config.json.
    """
    info: Dict[str, Any] = {}
    info_path = customer_dir / "info.json"
    if info_path.exists():
        with info_path.open("r", encoding="utf-8") as f:
            loaded = json.load(f)
        if isinstance(loaded, dict):
            info = loaded

    wallets: List[Dict[str, Any]] = []
    wp = customer_dir / "wallets.json"
    if wp.exists():
        with wp.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict) and isinstance(data.get("wallets"), list):
            wallets = list(data["wallets"])

    if wallets:
        return info, wallets

    cfg_path = customer_dir / "config.json"
    if not cfg_path.exists():
        raise FileNotFoundError(
            f"Neither wallets.json nor config.json found in {customer_dir}"
        )
    with cfg_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid customer config structure in {cfg_path}")
    if "wallets" not in data or not isinstance(data["wallets"], list):
        raise ValueError("Customer config must contain a 'wallets' list")
    return info, list(data["wallets"])


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser("ZenTaxCore Customer Runner")
    p.add_argument(
        "--customer",
        required=True,
        help="Customer folder name/slug (under customers root)",
    )
    p.add_argument(
        "--workspace",
        default=None,
        help=r"Workspace root that contains a 'customers' folder (e.g. C:\...\taxtrack_loop).",
    )
    p.add_argument(
        "--customers-root",
        default=None,
        help="Explicit customers root directory (overrides --workspace).",
    )
    p.add_argument(
        "--customer-dir",
        default=None,
        help="Explicit customer directory path (overrides --customers-root/--workspace/--customer).",
    )
    p.add_argument(
        "--year",
        type=int,
        help="Tax year (overrides config.json tax_year if provided)",
    )
    return p.parse_args(argv)


def _normalize_chain_id(name: str) -> str | None:
    """
    Map chain-folder names to canonical internal chain IDs.
    """
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

    return None


def main(argv: list[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    args = parse_args(argv)

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

    customer_name = (info.get("name") or "").strip() or args.customer
    tax_year = args.year
    if not tax_year:
        y = info.get("year")
        if y is not None:
            try:
                tax_year = int(y)
            except (TypeError, ValueError):
                tax_year = None
    if not tax_year:
        cfg_path = customer_dir / "config.json"
        if cfg_path.exists():
            with cfg_path.open("r", encoding="utf-8") as f:
                legacy = json.load(f)
            if isinstance(legacy, dict):
                ty = legacy.get("tax_year")
                if ty is not None:
                    try:
                        tax_year = int(ty)
                    except (TypeError, ValueError):
                        pass

    if not tax_year:
        raise ValueError("Tax year must be provided via --year, info.json, or config.json")

    if not wallets:
        raise ValueError("Customer config contains no wallets")

    jurisdiction = "DE"
    if isinstance(info, dict) and info.get("jurisdiction"):
        jurisdiction = str(info["jurisdiction"]).strip().upper()
    _cfg_for_j = customer_dir / "config.json"
    if _cfg_for_j.exists():
        with _cfg_for_j.open("r", encoding="utf-8") as f:
            _legacy_j = json.load(f)
        if isinstance(_legacy_j, dict) and _legacy_j.get("jurisdiction"):
            jurisdiction = str(_legacy_j["jurisdiction"]).strip().upper()

    print(f"[CUSTOMER] Name: {customer_name}")
    print(f"[CUSTOMER] Tax year: {tax_year}")
    print(f"[CUSTOMER] Jurisdiction: {jurisdiction}")
    print(f"[CUSTOMER] Wallets: {len(wallets)}")

    # Build wallet_data: one item per (wallet, chain) with "files" = list of CSV paths
    wallet_data: List[Dict[str, Any]] = []

    for w in wallets:
        address = (w.get("address") or "").lower()
        label = w.get("label") or address or "wallet"

        if not address:
            print(f"[WARN] Wallet missing address in config: {w}")
            continue

        wallet_inbox_label = customer_dir / "inbox" / label
        wallet_inbox_addr = customer_dir / "inbox" / address

        if wallet_inbox_label.exists():
            wallet_inbox = wallet_inbox_label
        elif wallet_inbox_addr.exists():
            wallet_inbox = wallet_inbox_addr
        else:
            print(
                f"[WARN] No inbox folder found for wallet {label} "
                f"(tried {wallet_inbox_label} and {wallet_inbox_addr})"
            )
            continue

        print(f"[INBOX] Wallet '{label}' at {wallet_inbox}")

        for chain_dir in wallet_inbox.iterdir():
            if not chain_dir.is_dir():
                continue

            chain_id = _normalize_chain_id(chain_dir.name)
            if not chain_id:
                print(f"[SKIP] Unknown chain folder '{chain_dir.name}'")
                continue

            print(f"[CHAIN] Wallet '{label}' chain '{chain_dir.name}' -> {chain_id}")

            files = [
                f for f in chain_dir.iterdir()
                if f.is_file() and f.suffix.lower() in (".csv", ".txt")
            ]
            if not files:
                continue

            wallet_data.append({
                "wallet": address,
                "chain_id": chain_id,
                "files": [str(p) for p in files],
            })

    if not wallet_data:
        print("[WARN] No wallet/chain data found. No reports generated.")
        return

    reports_dir = customer_dir / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "output_dir": reports_dir,
        "primary_wallet": wallets[0].get("address", "").lower() if wallets else "",
        "jurisdiction": jurisdiction,
        "debug_info": {
            "customer": customer_name,
            "customer_name": customer_name,
            "customer_address": (info.get("address") or "") if isinstance(info, dict) else "",
            "year": tax_year,
            "wallet_count": len(wallets),
            "jurisdiction": jurisdiction,
        },
        "pdf_filename": f"tax_report_{tax_year}.pdf",
        "audit_filename": f"tax_audit_{tax_year}.csv",
    }

    run_pipeline(wallet_data, tax_year, config)
    print("[CUSTOMER DONE] OK")


if __name__ == "__main__":
    main()
