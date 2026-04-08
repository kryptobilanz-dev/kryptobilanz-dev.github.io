# taxtrack/tests/test_pipeline_consistency.py
"""
Harvest JSON consistency: classified.json, gains.json, economic_gains_tax_ready.json
under taxtrack/data/harvest/*/2025/.

Logic lives in taxtrack.tools.check_consistency (also runnable as CLI).
"""

import pytest

from taxtrack.tools.check_consistency import _harvest_root, run_all_checks

YEAR = "2025"


def test_harvest_pipeline_consistency():
    """
    For each wallet with all three JSONs under harvest/<wallet>/2025/:
    gain vs economic row, swap alignment, valuation_missing, orphans, duplicates.
    """
    root = _harvest_root()
    if not root.is_dir():
        pytest.skip(f"Harvest directory not present: {root}")

    errors = run_all_checks(root, YEAR)
    fails = [e for e in errors if e.startswith("[FAIL]")]
    skips = [e for e in errors if e.startswith("[SKIP]")]

    for line in skips:
        print(line)

    if fails:
        for line in fails:
            print(line)
        pytest.fail(
            f"Harvest pipeline consistency: {len(fails)} failure(s). "
            f"({len(skips)} wallet-year skip(s).)\n" + "\n".join(fails)
        )
