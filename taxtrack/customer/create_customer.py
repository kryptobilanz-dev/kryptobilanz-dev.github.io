# taxtrack/customer/create_customer.py
"""
Interactive CLI: create customers/<normalized_name>/ with info.json, wallets.json, inbox/, reports/.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_CHAINS = ["eth", "arb", "op", "base", "avax", "matic"]


def normalize_customer_folder(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = s.strip("_")
    return s or "customer"


def _read_multiline_address(prompt_lines: str = "Adresse (leere Zeile = fertig):\n") -> str:
    print(prompt_lines, end="")
    lines: list[str] = []
    while True:
        try:
            line = input()
        except EOFError:
            break
        if line == "":
            break
        lines.append(line)
    return "\n".join(lines).strip()


def _read_wallets() -> list[dict]:
    wallets: list[dict] = []
    print("\nWallet (leer = fertig):")
    while True:
        try:
            raw = input("Wallet: ").strip()
        except EOFError:
            break
        if not raw:
            break
        addr = raw.lower()
        if not addr.startswith("0x") or len(addr) < 10:
            print("  [WARN] Ungültige Adresse, übersprungen.")
            continue
        wallets.append({"address": addr, "chains": list(DEFAULT_CHAINS)})
        try:
            more = input("Weitere Wallet? (Enter = nein): ").strip()
        except EOFError:
            more = ""
        if not more:
            break
        # treat non-empty as "yes" and loop for next address
    return wallets


def create_customer_files(
    root: Path,
    display_name: str,
    address: str,
    year: int,
    wallets: list[dict],
    *,
    customers_root: Path | None = None,
    company: str | None = None,
) -> Path:
    folder = normalize_customer_folder(display_name)
    base = Path(customers_root) if customers_root is not None else (root / "customers")
    customer_dir = base / folder
    customer_dir.mkdir(parents=True, exist_ok=True)
    (customer_dir / "inbox").mkdir(exist_ok=True)
    (customer_dir / "reports").mkdir(exist_ok=True)

    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    info = {
        "name": display_name.strip(),
        "address": address,
        "company": (company or "").strip() or None,
        "created_at": created_at,
        "year": int(year),
    }
    (customer_dir / "info.json").write_text(
        json.dumps(info, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    wallet_payload = {"wallets": wallets}
    (customer_dir / "wallets.json").write_text(
        json.dumps(wallet_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return customer_dir


def main() -> None:
    print("=== ZenTaxCore – Kunde anlegen ===\n")
    try:
        name = input("Name: ").strip()
    except EOFError:
        name = ""
    if not name:
        print("[ERROR] Name erforderlich.")
        sys.exit(1)

    address = _read_multiline_address()

    try:
        year_raw = input("\nJahr (z. B. 2025): ").strip()
    except EOFError:
        year_raw = ""
    try:
        year = int(year_raw)
    except ValueError:
        print("[ERROR] Ungültiges Jahr.")
        sys.exit(1)

    wallets = _read_wallets()
    if not wallets:
        print("[ERROR] Mindestens eine Wallet-Adresse erforderlich.")
        sys.exit(1)

    root = Path(__file__).resolve().parents[1]
    out = create_customer_files(root, name, address, year, wallets)
    print(f"\n[OK] Kunde angelegt unter: {out}")
    print(f"     Ordner-Slug: {normalize_customer_folder(name)}")
    print("     Dateien: info.json, wallets.json")
    print("     Nächster Schritt: Daten nach inbox/<wallet>/<chain>/ legen, dann:")
    print(f"     python -m taxtrack customer --customer {normalize_customer_folder(name)} --year {year}")


if __name__ == "__main__":
    main()
