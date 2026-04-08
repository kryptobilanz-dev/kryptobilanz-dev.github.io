from __future__ import annotations

from typing import Any, Dict, List

from taxtrack.analyze.tax_interpreter_us import build_reward_income_us, build_tax_ready_economic_gains_us
from taxtrack.pdf.audit_enrichment import enrich_tax_ready_rows
from taxtrack.tax.jurisdictions.base import TaxJurisdiction, TaxResult


class USJurisdiction:
    """
    United States (US) tax projection.

    - English-oriented summaries; amounts remain EUR (pipeline unit) unless converted elsewhere.
    - Short-term vs long-term capital (holding > 365 days = long-term bucket).
    - No DE-style non-realizing swap neutralization (treat as realization for US projection).
    """

    code = "US"

    def process(self, economic_events: List[Dict[str, Any]], *, context: Dict[str, Any] | None = None) -> TaxResult:
        ctx = dict(context or {})
        fifo_gain_rows = ctx.get("fifo_gain_rows") or []
        classified_dicts = ctx.get("classified_dicts") or []

        economic_gains_tax_ready, tax_summary = build_tax_ready_economic_gains_us(
            list(economic_events or []),
            fifo_gain_rows=list(fifo_gain_rows or []),
            classified_dicts=list(classified_dicts or []),
        )

        fifo_gain_dicts = list(fifo_gain_rows or [])
        economic_gains_tax_ready = enrich_tax_ready_rows(
            economic_gains_tax_ready,
            classified_dicts,
            fifo_gain_rows=fifo_gain_dicts,
        )

        reward_income, reward_income_summary = build_reward_income_us(classified_dicts)

        meta = {
            **ctx,
            "reward_income": reward_income,
            "reward_income_summary": reward_income_summary,
        }
        return TaxResult(tax_ready_events=economic_gains_tax_ready, tax_summary=tax_summary, meta=meta)


def get_jurisdiction() -> TaxJurisdiction:
    return USJurisdiction()
