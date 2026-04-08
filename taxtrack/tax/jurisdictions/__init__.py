"""
Pluggable tax jurisdictions.

The unified pipeline dispatches into a jurisdiction module here.
"""

from __future__ import annotations

from typing import Dict

from taxtrack.tax.jurisdictions.base import TaxJurisdiction


def get_jurisdiction(code: str | None) -> TaxJurisdiction:
    """
    Resolve a jurisdiction implementation by code.

    Defaults to DE.
    """
    c = (code or "DE").strip().upper()
    if c == "US":
        from taxtrack.tax.jurisdictions.us import get_jurisdiction as _get

        return _get()

    # Default: DE
    from taxtrack.tax.jurisdictions.de import get_jurisdiction as _get

    return _get()


def supported_jurisdictions() -> Dict[str, str]:
    return {
        "DE": "Germany",
        "US": "United States",
    }

