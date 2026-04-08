from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional


Confidence = Literal["high", "medium", "low"]


@dataclass
class FixSuggestion:
    issue_type: str
    explanation: str
    suggested_action: Dict[str, Any]
    confidence: Confidence
    tx_hash: Optional[str] = None
    context: Optional[Dict[str, Any]] = None


def suggest_fixes(*, classified_dicts: List[Dict[str, Any]], continuity_report: Dict[str, Any] | None = None) -> List[FixSuggestion]:
    """
    Produce user-guided fix suggestions instead of silent failures.

    Placeholder stub (will be wired to:
    - classification confidence low flags
    - valuation_missing signals
    - continuity_report broken links)
    """
    _ = classified_dicts
    _ = continuity_report
    return []

