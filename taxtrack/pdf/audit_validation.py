# taxtrack/pdf/audit_validation.py
"""
Consistency checks for tax-ready rows vs tax_summary and classified inputs.
Logs [AUDIT ERROR] on mismatch (reporting-only; does not change numbers).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List

# Must match tax_interpreter_de.PVG_CATEGORIES for bucket sums
PVG_CATEGORIES = {
    "sell",
    "swap",
    "lp_remove",
    "pendle_redeem",
    "vault_exit",
    "position_exit",
    "restake_out",
    "withdraw",
    "trade",
    "stable_swap",
}


def _as_float(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def validate_tax_ready_audit(
    tax_ready: List[Dict[str, Any]],
    tax_summary: Dict[str, Any],
    classified_dicts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    errors: List[str] = []
    warnings: List[str] = []

    rows = list(tax_ready or [])
    ts = tax_summary or {}

    def _counts_toward_totals(r: Dict[str, Any]) -> bool:
        return r.get("included_in_annual_totals", True) is not False

    sum_gain = round(
        sum(_as_float(r.get("gain")) for r in rows if _counts_toward_totals(r)),
        2,
    )
    total_ref = _as_float(ts.get("total_gains_net_eur"))
    if abs(sum_gain - total_ref) > 0.05:
        msg = f"sum(gain)={sum_gain} != tax_summary.total_gains_net_eur={total_ref}"
        errors.append(msg)
        print(f"[AUDIT ERROR] {msg}")

    sum_spec_pvg = round(
        sum(
            _as_float(r.get("speculative_bucket_net_eur"))
            for r in rows
            if (r.get("category") or "").lower() in PVG_CATEGORIES
            and _counts_toward_totals(r)
        ),
        2,
    )
    tax_spec_ref = _as_float(ts.get("taxable_gains_net_eur"))
    if abs(sum_spec_pvg - tax_spec_ref) > 0.05:
        msg = (
            f"sum(speculative_bucket PVG)={sum_spec_pvg} != "
            f"tax_summary.taxable_gains_net_eur={tax_spec_ref}"
        )
        errors.append(msg)
        print(f"[AUDIT ERROR] {msg}")

    sum_lt_pvg = round(
        sum(
            _as_float(r.get("long_term_bucket_net_eur"))
            for r in rows
            if (r.get("category") or "").lower() in PVG_CATEGORIES
            and _counts_toward_totals(r)
        ),
        2,
    )
    tax_lt_ref = _as_float(ts.get("taxfree_gains_net_eur"))
    if abs(sum_lt_pvg - tax_lt_ref) > 0.05:
        msg = (
            f"sum(long_term_bucket PVG)={sum_lt_pvg} != "
            f"tax_summary.taxfree_gains_net_eur={tax_lt_ref}"
        )
        errors.append(msg)
        print(f"[AUDIT ERROR] {msg}")

    # Duplicate tx_hash realizations (informational)
    by_tx: Dict[str, int] = defaultdict(int)
    for r in rows:
        txh = str(r.get("tx_hash") or "").strip().lower()
        if txh:
            by_tx[txh] += 1
    duplicate_tx = sorted([tx for tx, c in by_tx.items() if c > 1])

    # Fees: classified aggregate per tx vs each row's fees_eur (should match pipeline fee for that tx)
    fees_classified: Dict[str, float] = defaultdict(float)
    for r in classified_dicts or []:
        txh = str(r.get("tx_hash") or "").strip().lower()
        if not txh:
            continue
        fees_classified[txh] += _as_float(r.get("fee_eur"))

    for txh, n in by_tx.items():
        if n <= 1:
            continue
        fee_vals = {
            round(_as_float(r.get("fees_eur")), 4)
            for r in rows
            if str(r.get("tx_hash") or "").strip().lower() == txh
        }
        if len(fee_vals) > 1:
            msg = f"inconsistent fees_eur across rows for tx {txh[:12]}…: {fee_vals}"
            errors.append(msg)
            print(f"[AUDIT ERROR] {msg}")

    ref_fee = {txh: round(v, 2) for txh, v in fees_classified.items()}
    for r in rows:
        txh = str(r.get("tx_hash") or "").strip().lower()
        if not txh:
            continue
        row_fee = round(_as_float(r.get("fees_eur")), 2)
        exp = ref_fee.get(txh, 0.0)
        # Only enforce when classified rows recorded a non-zero aggregate fee for this tx
        if exp > 1e-6 and abs(row_fee - exp) > 0.05:
            msg = f"fees_eur row {row_fee} != classified aggregate {exp} for tx {txh[:12]}…"
            errors.append(msg)
            print(f"[AUDIT ERROR] {msg}")

    vm_classified = sum(
        1
        for r in classified_dicts or []
        if isinstance(r.get("meta"), dict) and bool((r.get("meta") or {}).get("valuation_missing"))
    )

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "duplicate_tx_hashes": duplicate_tx,
        "valuation_missing_rows_classified": int(vm_classified),
    }


def confidence_distribution(tax_ready: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts = {"high": 0, "medium": 0, "low": 0, "other": 0}
    for r in tax_ready or []:
        c = (r.get("price_confidence") or "").lower()
        if c in counts:
            counts[c] += 1
        else:
            counts["other"] += 1
    n = max(1, len(tax_ready or []))
    return {
        "counts": {k: counts[k] for k in ("high", "medium", "low")},
        "pct_high": round(100.0 * counts["high"] / n, 2),
        "pct_medium": round(100.0 * counts["medium"] / n, 2),
        "pct_low": round(100.0 * counts["low"] / n, 2),
    }


def top_problem_tokens(classified_dicts: List[Dict[str, Any]], limit: int = 15) -> List[Dict[str, Any]]:
    """Tokens with low confidence or valuation_missing on classified rows."""
    from collections import Counter

    c = Counter()
    for r in classified_dicts or []:
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        conf = (meta.get("price_confidence") or "").lower()
        vm = bool(meta.get("valuation_missing"))
        if conf == "low" or vm:
            t = (r.get("token") or "").upper() or "?"
            c[t] += 1
    return [{"token": k, "count": v} for k, v in c.most_common(limit)]


def unresolved_tx_hashes(classified_dicts: List[Dict[str, Any]], limit: int = 50) -> List[str]:
    out: List[str] = []
    seen = set()
    for r in classified_dicts or []:
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        txh = str(r.get("tx_hash") or "").strip().lower()
        if not txh or txh in seen:
            continue
        conf = (meta.get("price_confidence") or "").lower()
        vm = bool(meta.get("valuation_missing"))
        try:
            amt = abs(float(r.get("amount") or 0.0))
        except Exception:
            amt = 0.0
        try:
            eur = float(r.get("eur_value") or 0.0)
        except Exception:
            eur = 0.0
        if vm or conf == "low" or (amt > 0 and eur <= 0):
            seen.add(txh)
            out.append(txh)
        if len(out) >= limit:
            break
    return out
