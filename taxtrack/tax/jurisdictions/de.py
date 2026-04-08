from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Tuple

from taxtrack.analyze.tax_interpreter_de import build_reward_income_de, build_tax_ready_economic_gains_de
from taxtrack.pdf.audit_enrichment import enrich_tax_ready_rows
from taxtrack.tax.jurisdictions.base import TaxJurisdiction, TaxResult


# ---------------------------------------------------------------------------
# DE-specific post-processing moved out of pipeline.py
# ---------------------------------------------------------------------------

_ASSET_TYPE_DERIVATIVE_SUBSTRINGS = (
    "STETH",
    "WSTETH",
    "EETH",
    "EZETH",
    "RSETH",
    "RETH",
    "CBETH",
)
_ASSET_TYPE_POSITION_SUBSTRINGS = (
    "LP",
    "LPT",
    "VAULT",
    "PENDLE",
    "MOO",
    "BEEFY",
)
_ASSET_TYPE_STABLES = frozenset({"USDC", "USDT", "DAI", "EUR", "USD"})
_ASSET_TYPE_BASE_ASSETS = frozenset({"ETH", "BTC"})


def _classify_asset_type(token: str) -> str:
    t = (token or "").strip().upper()
    if not t or t == "UNKNOWN" or t.startswith("UNKNOWN") or t.startswith("ERC20"):
        return "unknown"

    if t in _ASSET_TYPE_STABLES:
        return "stable"
    if t in _ASSET_TYPE_BASE_ASSETS:
        return "base_asset"
    if t.startswith("LRT_"):
        return "derivative"
    if any(s in t for s in _ASSET_TYPE_POSITION_SUBSTRINGS):
        return "position_token"
    if any(s in t for s in _ASSET_TYPE_DERIVATIVE_SUBSTRINGS):
        return "derivative"
    return "unknown"


def _is_non_realizing_swap(tx: Dict[str, Any]) -> bool:
    """
    DE detection layer: derived-price swaps that look like position transformations (no intended realization).

    Mirrors the historical pipeline behavior to preserve output.
    """
    try:
        if str(tx.get("category") or "").strip().lower() != "swap":
            return False
        if str(tx.get("price_source") or "").strip().lower() != "derived":
            return False

        toks_in: list[str] = []
        toks_out: list[str] = []
        for leg in (tx.get("tokens_in") or []):
            if isinstance(leg, dict):
                toks_in.append(str(leg.get("token") or ""))
        for leg in (tx.get("tokens_out") or []):
            if isinstance(leg, dict):
                toks_out.append(str(leg.get("token") or ""))

        has_derivative_out = any(_classify_asset_type(t) == "derivative" for t in toks_out if t is not None)
        has_position_in = any(_classify_asset_type(t) == "position_token" for t in toks_in if t is not None)
        if not (has_derivative_out and has_position_in):
            return False

        tin = tx.get("_classified_total_in_value_eur")
        tout = tx.get("_classified_total_out_value_eur")
        try:
            tin_f = float(tin) if tin is not None else None
        except Exception:
            tin_f = None
        try:
            tout_f = float(tout) if tout is not None else None
        except Exception:
            tout_f = None
        if tin_f is None or tout_f is None:
            return False
        if abs(tin_f - tout_f) > 0.02:
            return False

        cp_protocols = tx.get("_classified_cp_protocols") or []
        cp_set = {str(p or "").strip().lower() for p in (cp_protocols if isinstance(cp_protocols, list) else [])}
        toks_all = toks_in + toks_out
        no_stables = not any(_classify_asset_type(t) == "stable" for t in toks_all if t is not None)
        proto_trigger = bool(cp_set.intersection({"restake", "dex"})) and no_stables
        return bool(proto_trigger)
    except Exception:
        return False


def _apply_non_realizing_position_transformations(
    *,
    economic_gains: List[Dict[str, Any]],
    tax_ready: List[Dict[str, Any]],
    tax_summary: Dict[str, Any],
    classified_dicts: List[Dict[str, Any]],
) -> None:
    if not tax_ready:
        return

    def _norm_tx(txh: Any) -> str:
        return str(txh or "").strip().lower()

    by_tx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in classified_dicts or []:
        k = _norm_tx(r.get("tx_hash"))
        if k:
            by_tx[k].append(r)

    def _tx_cp_protocols(rows: List[Dict[str, Any]]) -> List[str]:
        out: set[str] = set()
        for r in rows:
            m = r.get("meta") if isinstance(r.get("meta"), dict) else {}
            p = (m.get("cp_protocol") or "").strip().lower()
            if p:
                out.add(p)
        return sorted(out)

    def _tx_swap_totals(rows: List[Dict[str, Any]]) -> Tuple[float | None, float | None]:
        for r in rows:
            if str(r.get("category") or "").strip().lower() != "swap":
                continue
            m = r.get("meta") if isinstance(r.get("meta"), dict) else {}
            if not isinstance(m, dict):
                continue
            try:
                tin = float(m.get("total_in_value_eur")) if m.get("total_in_value_eur") is not None else None
            except Exception:
                tin = None
            try:
                tout = float(m.get("total_out_value_eur")) if m.get("total_out_value_eur") is not None else None
            except Exception:
                tout = None
            return tin, tout
        return None, None

    econ_by_key: Dict[tuple[str, str], Dict[str, Any]] = {}
    for e in economic_gains or []:
        txk = _norm_tx(e.get("tx_hash"))
        cat = str(e.get("category") or "").strip().lower()
        if txk and cat:
            econ_by_key[(txk, cat)] = e

    for row in tax_ready:
        txh = str(row.get("tx_hash") or "").strip()
        txk = txh.lower()
        src_rows = by_tx.get(txk, [])

        tin, tout = _tx_swap_totals(src_rows)
        row["_classified_total_in_value_eur"] = tin
        row["_classified_total_out_value_eur"] = tout
        row["_classified_cp_protocols"] = _tx_cp_protocols(src_rows)

        if not _is_non_realizing_swap(row):
            continue

        old_gain = float(row.get("gain") or 0.0)
        old_spec = float(row.get("speculative_bucket_net_eur") or 0.0)
        old_lt = float(row.get("long_term_bucket_net_eur") or 0.0)
        old_included = bool(row.get("included_in_annual_totals"))

        row["non_realizing"] = True
        row["gain"] = 0.0
        row["included_in_annual_totals"] = False
        row["excluded_from_totals_reason"] = "position_transformation"
        row["excluded_reason"] = "position_transformation"

        try:
            tax_summary["sum_row_gains_all_eur"] = round(
                float(tax_summary.get("sum_row_gains_all_eur") or 0.0) - old_gain, 2
            )
            if old_included:
                tax_summary["total_gains_net_eur"] = round(
                    float(tax_summary.get("total_gains_net_eur") or 0.0) - old_gain, 2
                )
                tax_summary["taxable_gains_net_eur"] = round(
                    float(tax_summary.get("taxable_gains_net_eur") or 0.0) - old_spec, 2
                )
                tax_summary["taxfree_gains_net_eur"] = round(
                    float(tax_summary.get("taxfree_gains_net_eur") or 0.0) - old_lt, 2
                )
                if old_lt > 0:
                    tax_summary["taxfree_over_1y_net_eur"] = round(
                        float(tax_summary.get("taxfree_over_1y_net_eur") or 0.0) - old_lt, 2
                    )
                tax_summary["excluded_from_totals_count"] = int(tax_summary.get("excluded_from_totals_count") or 0) + 1
                tax_summary["excluded_from_totals_net_eur"] = round(
                    float(tax_summary.get("excluded_from_totals_net_eur") or 0.0) + old_gain, 2
                )
        except Exception:
            pass

        eg = econ_by_key.get((txk, "swap"))
        if isinstance(eg, dict):
            eg["non_realizing"] = True
            eg["net_pnl_eur"] = 0.0
            eg["pnl_eur"] = 0.0

        print(f"[FIX_APPLIED] reason=non_realizing_restake tx={txh}")


class DEJurisdiction:
    code = "DE"

    def process(self, economic_events: List[Dict[str, Any]], *, context: Dict[str, Any] | None = None) -> TaxResult:
        ctx = dict(context or {})
        fifo_gain_rows = ctx.get("fifo_gain_rows") or []
        classified_dicts = ctx.get("classified_dicts") or []

        economic_gains_tax_ready, tax_summary = build_tax_ready_economic_gains_de(
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

        _apply_non_realizing_position_transformations(
            economic_gains=list(economic_events or []),
            tax_ready=economic_gains_tax_ready,
            tax_summary=tax_summary,
            classified_dicts=classified_dicts,
        )

        reward_income, reward_income_summary = build_reward_income_de(classified_dicts)

        meta = {
            **ctx,
            "reward_income": reward_income,
            "reward_income_summary": reward_income_summary,
        }
        return TaxResult(tax_ready_events=economic_gains_tax_ready, tax_summary=tax_summary, meta=meta)


def get_jurisdiction() -> TaxJurisdiction:
    return DEJurisdiction()

