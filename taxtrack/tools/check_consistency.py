"""
Harvest pipeline consistency: classified.json vs gains.json vs economic_gains_tax_ready.json.

Run: python -m taxtrack.tools.check_consistency
Exit code 0 if no issues, 1 otherwise.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from taxtrack.validation.harvest_consistency import validate_consistency_lists


def _harvest_root() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "harvest"


def _load_json(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


def discover_wallet_year_dirs(harvest_root: Path, year: str = "2025") -> list[tuple[Path, str]]:
    """Return (wallet_dir, wallet_id) for each wallet that has a year subfolder."""
    out: list[tuple[Path, str]] = []
    if not harvest_root.is_dir():
        return out
    for wallet_dir in sorted(harvest_root.iterdir()):
        if not wallet_dir.is_dir():
            continue
        ydir = wallet_dir / year
        if ydir.is_dir():
            out.append((wallet_dir, wallet_dir.name))
    return out


def check_wallet_year(
    wallet_dir: Path,
    wallet_id: str,
    year: str = "2025",
) -> list[str]:
    ydir = wallet_dir / year
    p_classified = ydir / "classified.json"
    p_gains = ydir / "gains.json"
    p_tax = ydir / "economic_gains_tax_ready.json"

    if not (p_classified.is_file() and p_gains.is_file() and p_tax.is_file()):
        return [
            f"[SKIP] wallet={wallet_id} year={year} missing one of classified/gains/economic_gains_tax_ready"
        ]

    classified = _load_json(p_classified)
    gains_rows = _load_json(p_gains)
    tax_rows = _load_json(p_tax)
    return validate_consistency_lists(classified, gains_rows, tax_rows, wallet_id=wallet_id, year=year)


def run_all_checks(harvest_root: Path | None = None, year: str = "2025") -> list[str]:
    root = harvest_root or _harvest_root()
    all_errs: list[str] = []
    for wallet_dir, wallet_id in discover_wallet_year_dirs(root, year):
        all_errs.extend(check_wallet_year(wallet_dir, wallet_id, year))
    return all_errs


def main() -> int:
    year = "2025"
    if len(sys.argv) > 1 and sys.argv[1].strip():
        year = sys.argv[1].strip()

    root = _harvest_root()
    if not root.is_dir():
        print(f"[FAIL] harvest root missing: {root}", file=sys.stderr)
        return 1

    errors = run_all_checks(root, year)
    skips = [e for e in errors if e.startswith("[SKIP]")]
    fails = [e for e in errors if e.startswith("[FAIL]")]

    for line in skips:
        print(line)
    for line in fails:
        print(line)

    if fails:
        print(f"\nSummary: {len(fails)} failure(s), {len(skips)} skip(s).", file=sys.stderr)
        return 1
    if not errors:
        print(f"OK: no issues under {root} for year {year}.")
    else:
        print(f"OK: only skips ({len(skips)}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
