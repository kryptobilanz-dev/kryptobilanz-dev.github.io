from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class ContinuityReport:
    broken_links: List[Dict[str, Any]]
    suspected_missing_wallets: List[Dict[str, Any]]
    orphan_assets: List[Dict[str, Any]]
    meta: Dict[str, Any] | None = None


def detect_broken_chains(classified_items: List[Dict[str, Any]]) -> ContinuityReport:
    """
    Detect broken cost-basis continuity chains.

    Placeholder stub. Intended signals:
    - inflows without plausible origin lots
    - unmatched transfers across wallets/platforms
    - missing wallet links (owner_wallet gaps)
    """
    _ = classified_items
    return ContinuityReport(broken_links=[], suspected_missing_wallets=[], orphan_assets=[], meta={})

