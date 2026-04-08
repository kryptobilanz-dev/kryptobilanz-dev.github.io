from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Protocol


@dataclass
class TaxResult:
    """
    Jurisdiction output container.

    This intentionally stays generic: the pipeline produces jurisdiction-neutral
    economic events; jurisdictions project those into tax-ready rows + summaries.
    """

    tax_ready_events: List[Dict[str, Any]]
    tax_summary: Dict[str, Any]
    meta: Dict[str, Any] | None = None


class TaxJurisdiction(Protocol):
    """
    Pluggable tax jurisdiction interface.
    """

    code: str  # e.g. "DE", "US"

    def process(self, economic_events: List[Dict[str, Any]], *, context: Dict[str, Any] | None = None) -> TaxResult:
        """
        Convert jurisdiction-neutral economic events into jurisdiction-specific tax-ready results.
        """

