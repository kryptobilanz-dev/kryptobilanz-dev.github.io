from __future__ import annotations

from typing import Any, Dict, List, Tuple

# §23 EStG: private disposal gains are tax-free only after more than one year.
# Aligned with tax_rules.taxable_status: hold_days <= 365 → speculative (taxable gain),
# hold_days > 365 → tax-free gain.
HOLDING_SPECULATION_THRESHOLD_DAYS = 365

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

INCOME_CATEGORIES = {
    "reward",
    "staking_reward",
    "vault_reward",
    "pendle_reward",
    "restake_reward",
    "airdrop",
    "learning_reward",
    "earn_reward",
}


def _as_float(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _as_int(x: Any) -> int:
    try:
        return int(x or 0)
    except Exception:
        return 0


# Proceeds ~0 but large |net| = typical broken swap-out pricing (FIFO cost only).
_MATERIAL_NET_ABS_EUR = 50.0
_NEAR_ZERO_PROCEEDS_EUR = 0.02


def _row_matches_event_category(r: Dict[str, Any], cat_l: str) -> bool:
    r_cat = (r.get("category") or "").lower()
    if r_cat == cat_l:
        return True
    if cat_l == "swap" and r_cat == "swap":
        return True
    return False


def _fifo_leg_valuation_missing(fifo_rows: List[Dict[str, Any]]) -> bool:
    for r in fifo_rows:
        m = r.get("meta")
        if isinstance(m, dict) and m.get("valuation_missing"):
            return True
    return False


def _classified_unreliable_for_totals(
    txh: str,
    cat_l: str,
    classified_dicts: List[Dict[str, Any]] | None,
) -> tuple[bool, str]:
    """
    If classified pricing for this tx/category is missing or explicitly low-confidence
    on disposal legs, do not roll the economic PnL into annual headline totals.
    """
    if not classified_dicts:
        return False, ""
    txk = (txh or "").strip().lower()
    if not txk:
        return False, ""
    rows = [
        r
        for r in classified_dicts
        if str(r.get("tx_hash") or "").strip().lower() == txk
    ]
    for r in rows:
        if not _row_matches_event_category(r, cat_l):
            continue
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        if bool(meta.get("valuation_missing")):
            return True, "classified_valuation_missing"

        if cat_l == "swap":
            for leg in meta.get("tokens_out") or []:
                if not isinstance(leg, dict):
                    continue
                raw_eur = leg.get("eur_value")
                try:
                    ev = float(raw_eur) if raw_eur is not None else 0.0
                except (TypeError, ValueError):
                    ev = 0.0
                if raw_eur is None or ev <= 0:
                    return True, "classified_swap_out_unpriced"
                pc = (leg.get("price_confidence") or "").lower()
                if pc == "low":
                    return True, "classified_swap_out_low_confidence"
        else:
            try:
                evr = float(r.get("eur_value") or 0.0)
            except (TypeError, ValueError):
                evr = 0.0
            pc = (meta.get("price_confidence") or "").lower()
            if evr <= 0:
                return True, "classified_disposal_unpriced"
            if pc == "low":
                return True, "classified_disposal_low_confidence"
    return False, ""


def _exclude_from_annual_totals(
    e: Dict[str, Any],
    fifo_rows: List[Dict[str, Any]],
    cat_l: str,
    classified_dicts: List[Dict[str, Any]] | None,
) -> tuple[bool, str]:
    if bool(e.get("valuation_missing")):
        return True, "economic_valuation_missing"
    if _fifo_leg_valuation_missing(fifo_rows):
        return True, "fifo_leg_valuation_missing"
    proceeds = _as_float(e.get("proceeds_eur"))
    pnl = _as_float(e.get("pnl_eur"))
    net = _as_float(e.get("net_pnl_eur", pnl))
    if proceeds <= _NEAR_ZERO_PROCEEDS_EUR and abs(net) >= _MATERIAL_NET_ABS_EUR:
        return True, "near_zero_proceeds_material_net"
    ex, reason = _classified_unreliable_for_totals(e.get("tx_hash") or "", cat_l, classified_dicts)
    if ex:
        return True, reason
    return False, ""


def _fifo_category_for_event(e: Dict[str, Any]) -> str:
    """FIFO leg method to match (after false-swap reconcile, _fifo_category may still be 'swap')."""
    raw = (e.get("_fifo_category") or e.get("category") or "").strip().lower()
    return raw


def _classified_tx_valuation_missing(txh: str, classified_dicts: List[Dict[str, Any]] | None) -> bool:
    k = (txh or "").strip().lower()
    if not k:
        return False
    for r in classified_dicts or []:
        if str(r.get("tx_hash") or "").strip().lower() != k:
            continue
        m = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        if m.get("valuation_missing") is True:
            return True
    return False


def _fifo_rows_for_event(e: Dict[str, Any], fifo_gain_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Match FIFO gain rows to one economic event (same tx + disposal method on gain row)."""
    if not fifo_gain_rows:
        return []
    txh = (e.get("tx_hash") or "").strip()
    cat = _fifo_category_for_event(e)
    if not txh or not cat:
        return []
    out: List[Dict[str, Any]] = []
    for g in fifo_gain_rows:
        if (g.get("tx_hash") or "").strip() != txh:
            continue
        m = (g.get("method") or "").lower()
        if m == cat:
            out.append(g)
    return out


def _allocate_fees_proportional(
    fifo_rows: List[Dict[str, Any]], fee_eur: float
) -> List[float]:
    """Split tx fee across FIFO legs by proceeds share (deterministic)."""
    n = len(fifo_rows)
    if n == 0:
        return []
    fee_eur = max(0.0, float(fee_eur or 0.0))
    proceeds = [abs(_as_float(r.get("proceeds_eur"))) for r in fifo_rows]
    total = sum(proceeds)
    if total <= 1e-18:
        base = fee_eur / n
        alloc = [base] * n
    else:
        alloc = [fee_eur * (p / total) for p in proceeds]
    # Fix rounding drift on last leg
    drift = fee_eur - sum(alloc)
    if n > 0 and abs(drift) > 1e-9:
        alloc[-1] += drift
    return alloc


def _split_pvg_net_by_holding(
    e: Dict[str, Any],
    fifo_rows: List[Dict[str, Any]],
    fee_eur: float,
) -> Tuple[float, float, int, int, List[Dict[str, Any]]]:
    """
    Split net PnL after fees into:
      - speculative_net: disposal lots held <= HOLDING_SPECULATION_THRESHOLD_DAYS
      - long_term_net: held > threshold (§23 tax-free for gains)

    Returns (speculative_net, long_term_net, hold_days_min, hold_days_max, leg_debug).
    """
    th = HOLDING_SPECULATION_THRESHOLD_DAYS
    fee_eur = _as_float(fee_eur)

    if not fifo_rows:
        hd = _as_int(e.get("hold_days"))
        net = _as_float(e.get("net_pnl_eur"))
        if net == 0.0:
            net = _as_float(e.get("pnl_eur")) - fee_eur
        if hd > th:
            return 0.0, net, hd, hd, []
        return net, 0.0, hd, hd, []

    alloc = _allocate_fees_proportional(fifo_rows, fee_eur)
    speculative_net = 0.0
    long_term_net = 0.0
    holds: List[int] = []
    leg_debug: List[Dict[str, Any]] = []

    for i, r in enumerate(fifo_rows):
        pnl = _as_float(r.get("pnl_eur"))
        net_i = pnl - alloc[i] if i < len(alloc) else pnl
        hd = _as_int(r.get("hold_days"))
        holds.append(hd)
        leg_debug.append(
            {
                "token": r.get("token"),
                "hold_days": hd,
                "buy_date_iso": r.get("buy_date_iso"),
                "proceeds_eur": round(_as_float(r.get("proceeds_eur")), 2),
                "net_after_fee_eur": round(net_i, 2),
            }
        )
        if hd > th:
            long_term_net += net_i
        else:
            speculative_net += net_i

    hmin = min(holds) if holds else 0
    hmax = max(holds) if holds else 0
    return speculative_net, long_term_net, hmin, hmax, leg_debug


def build_tax_ready_economic_gains_de(
    economic_gains: List[Dict[str, Any]],
    fifo_gain_rows: List[Dict[str, Any]] | None = None,
    classified_dicts: List[Dict[str, Any]] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Tax interpretation layer (Germany, §23 holding period).

    Uses FIFO gain rows (per-lot disposals) when provided so multi-lot swaps are not
    forced into the minimum holding period of the grouped economic event.

    Input: economic_gains (grouped) — sole source for tax_ready rows (1:1 with gains.json);
    optional fifo_gain_rows from compute_gains (same tx_hash + method; use _fifo_category when reconciled);
    optional classified_dicts for exclusions and valuation_missing handling.

    Output: tax_ready list + tax_summary.

    Does NOT modify classification, swap grouping, or FIFO lot logic.
    """
    fifo_gain_rows = fifo_gain_rows or []
    out: List[Dict[str, Any]] = []

    total_net = 0.0
    speculative_net_total = 0.0
    long_term_net_total = 0.0
    taxfree_over_1y_positive = 0.0
    excluded_net_sum = 0.0
    excluded_count = 0
    suspicious: List[Dict[str, Any]] = []

    for e in list(economic_gains or []):
        cat = (e.get("category") or "").lower()
        dt_iso = e.get("dt_iso") or ""
        token = e.get("token") or ""
        txh = e.get("tx_hash") or ""

        if not dt_iso or not txh:
            raise ValueError(
                "Economic gain row missing dt_iso or tx_hash (gains.json is SSOT for tax_ready): "
                f"category={cat!r} tx_hash={txh!r} dt_iso={dt_iso!r}"
            )

        fifo_cat = _fifo_category_for_event(e)

        if _classified_tx_valuation_missing(txh, classified_dicts):
            excluded_count += 1
            hold_z = _as_int(e.get("hold_days"))
            out.append(
                {
                    "date": dt_iso.split("T")[0] if isinstance(dt_iso, str) else "",
                    "dt_iso": dt_iso,
                    "tx_hash": txh,
                    "category": cat,
                    "subtype": e.get("subtype"),
                    "token": token,
                    "amount": None,
                    "cost_basis": 0.0,
                    "proceeds": 0.0,
                    "pnl_gross_eur": 0.0,
                    "fees_eur": 0.0,
                    "gain": 0.0,
                    "taxable": False,
                    "holding_period_days": int(hold_z or 0),
                    "holding_period_days_min": int(hold_z or 0),
                    "holding_period_days_max": int(hold_z or 0),
                    "speculative_bucket_net_eur": 0.0,
                    "long_term_bucket_net_eur": 0.0,
                    "fifo_leg_count": 0,
                    "fifo_legs_debug": [],
                    "included_in_annual_totals": False,
                    "excluded_from_totals_reason": "valuation_missing",
                    "fifo_match_method": fifo_cat,
                }
            )
            continue

        proceeds = _as_float(e.get("proceeds_eur"))
        cost = _as_float(e.get("cost_basis_eur"))
        pnl = _as_float(e.get("pnl_eur"))
        net = _as_float(e.get("net_pnl_eur", pnl))
        fee_eur = _as_float(e.get("fees_eur"))

        fifo_rows = _fifo_rows_for_event(e, fifo_gain_rows)
        excluded, exclude_reason = _exclude_from_annual_totals(
            e, fifo_rows, cat, classified_dicts
        )
        if excluded:
            excluded_net_sum += net
            excluded_count += 1
        else:
            total_net += net

        mixed_holding = len(fifo_rows) > 1 and len({int(r.get("hold_days") or 0) for r in fifo_rows}) > 1

        if cat in PVG_CATEGORIES:
            spec_net, lt_net, hmin, hmax, legs = _split_pvg_net_by_holding(e, fifo_rows, fee_eur)

            if mixed_holding and (hmax - hmin) >= 200:
                suspicious.append(
                    {
                        "tx_hash": txh,
                        "category": cat,
                        "hold_days_min": hmin,
                        "hold_days_max": hmax,
                        "fifo_legs": len(fifo_rows),
                        "note": "mixed holding periods in one tx — review FIFO legs",
                    }
                )

            if not excluded:
                speculative_net_total += spec_net
                long_term_net_total += lt_net
                if lt_net > 0:
                    taxfree_over_1y_positive += lt_net

            # Short-term (≤365d) PnL bucket non-zero → §23 speculative scope; LP-remove with net loss → none
            if cat == "lp_remove" and net <= 0:
                taxable_flag = False
            else:
                taxable_flag = abs(spec_net) > 1e-6

            hold_primary = hmin if fifo_rows else _as_int(e.get("hold_days"))

            out.append(
                {
                    "date": dt_iso.split("T")[0] if isinstance(dt_iso, str) else "",
                    "dt_iso": dt_iso,
                    "tx_hash": txh,
                    "category": cat,
                    "subtype": e.get("subtype"),
                    "token": token,
                    "amount": None,
                    "cost_basis": round(cost, 2),
                    "proceeds": round(proceeds, 2),
                    "pnl_gross_eur": round(pnl, 2),
                    "fees_eur": round(fee_eur, 2),
                    "gain": round(net, 2),
                    "taxable": bool(taxable_flag),
                    "holding_period_days": int(hold_primary),
                    "holding_period_days_min": int(hmin) if fifo_rows else int(hold_primary),
                    "holding_period_days_max": int(hmax) if fifo_rows else int(hold_primary),
                    "speculative_bucket_net_eur": round(spec_net, 2),
                    "long_term_bucket_net_eur": round(lt_net, 2),
                    "fifo_leg_count": len(fifo_rows),
                    "fifo_legs_debug": legs if (mixed_holding or len(legs) > 1) else [],
                    "included_in_annual_totals": not excluded,
                    "excluded_from_totals_reason": exclude_reason if excluded else "",
                    "fifo_match_method": fifo_cat,
                }
            )
            continue

        # Non-PVG economic events: pass through with aggregated holding if present
        hold_days = _as_int(e.get("hold_days"))
        taxable_flag = bool(e.get("taxable"))

        out.append(
            {
                "date": dt_iso.split("T")[0] if isinstance(dt_iso, str) else "",
                "dt_iso": dt_iso,
                "tx_hash": txh,
                "category": cat,
                "subtype": e.get("subtype"),
                "token": token,
                "amount": None,
                "cost_basis": round(cost, 2),
                "proceeds": round(proceeds, 2),
                "pnl_gross_eur": round(pnl, 2),
                "fees_eur": round(fee_eur, 2),
                "gain": round(net, 2),
                "taxable": taxable_flag,
                "holding_period_days": int(hold_days or 0),
                "holding_period_days_min": int(hold_days or 0),
                "holding_period_days_max": int(hold_days or 0),
                "speculative_bucket_net_eur": round(net if taxable_flag else 0.0, 2),
                "long_term_bucket_net_eur": 0.0,
                "fifo_leg_count": 0,
                "fifo_legs_debug": [],
                "included_in_annual_totals": not excluded,
                "excluded_from_totals_reason": exclude_reason if excluded else "",
                "fifo_match_method": fifo_cat,
            }
        )

    sum_row_gains_all = round(sum(_as_float(r.get("gain")) for r in out), 2)
    summary = {
        "total_gains_net_eur": round(total_net, 2),
        # Short-term (<=365d) vs long-term (>365d) PnL after fee split — realistic §23 split
        "taxable_gains_net_eur": round(speculative_net_total, 2),
        "taxfree_gains_net_eur": round(long_term_net_total, 2),
        "taxfree_over_1y_net_eur": round(taxfree_over_1y_positive, 2),
        "excluded_from_totals_count": int(excluded_count),
        "excluded_from_totals_net_eur": round(excluded_net_sum, 2),
        "sum_row_gains_all_eur": sum_row_gains_all,
        "skipped_events": 0,
        "rows": len(out),
        "suspicious_mixed_holding": suspicious,
        "holding_threshold_days": HOLDING_SPECULATION_THRESHOLD_DAYS,
    }
    return out, summary


def build_reward_income_de(classified_dicts: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Rewards are taxed as income at receipt time (§22). This produces an income list and totals.

    NOTE: FIFO lot creation for rewards would be a FIFO concern; this layer only reports income.
    """
    rows: List[Dict[str, Any]] = []
    total_eur = 0.0
    skipped = 0

    for r in list(classified_dicts or []):
        cat = (r.get("category") or "").lower()
        if cat not in INCOME_CATEGORIES:
            continue
        dt_iso = r.get("dt_iso") or ""
        tok = (r.get("token") or "").upper()
        amt = _as_float(r.get("amount"))
        eur = _as_float(r.get("eur_value"))
        if not dt_iso or not tok or amt == 0:
            skipped += 1
            continue
        if eur <= 0:
            skipped += 1
            continue
        total_eur += eur
        rows.append(
            {
                "date": dt_iso.split("T")[0],
                "dt_iso": dt_iso,
                "category": cat,
                "token": tok,
                "amount": amt,
                "eur_value": round(eur, 2),
                "taxable": True,
            }
        )

    summary = {
        "rewards_income_eur": round(total_eur, 2),
        "rows": len(rows),
        "skipped": int(skipped),
    }
    return rows, summary
