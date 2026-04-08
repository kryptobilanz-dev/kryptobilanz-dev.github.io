from __future__ import annotations

"""
US capital-gains projection (FIFO lots, short-term vs long-term).

Uses the same economic events + FIFO legs as DE, but:
- Both short- and long-term realized gains are taxable (no §23-style exemption).
- Long-term: holding period > 365 days (aligns with common >1-year crypto treatment).
- Flags incomplete / unreliable basis for 1099-DA-style review (proceeds primary).
"""

from typing import Any, Dict, List, Tuple

from taxtrack.analyze.tax_interpreter_de import (
    INCOME_CATEGORIES,
    PVG_CATEGORIES,
    _allocate_fees_proportional,
    _as_float,
    _as_int,
    _classified_tx_valuation_missing,
    _exclude_from_annual_totals,
    _fifo_category_for_event,
    _fifo_leg_valuation_missing,
    _fifo_rows_for_event,
)


US_LONG_TERM_THRESHOLD_DAYS = 365


def _split_capital_net_by_holding_us(
    e: Dict[str, Any],
    fifo_rows: List[Dict[str, Any]],
    fee_eur: float,
) -> Tuple[float, float, int, int, List[Dict[str, Any]]]:
    """
    Split net PnL after fees into short-term (<= threshold) and long-term (> threshold).
    Both buckets count toward total capital gain/loss for US reporting.
    """
    th = US_LONG_TERM_THRESHOLD_DAYS
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
    short_net = 0.0
    long_net = 0.0
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
            long_net += net_i
        else:
            short_net += net_i

    hmin = min(holds) if holds else 0
    hmax = max(holds) if holds else 0
    return short_net, long_net, hmin, hmax, leg_debug


def _cost_basis_complete_for_row(
    e: Dict[str, Any],
    fifo_rows: List[Dict[str, Any]],
    excluded: bool,
) -> bool:
    if excluded:
        return False
    if bool(e.get("valuation_missing")):
        return False
    if _fifo_leg_valuation_missing(fifo_rows):
        return False
    # Proceeds without basis is a classic non-covered / incomplete case
    proceeds = _as_float(e.get("proceeds_eur"))
    cost = _as_float(e.get("cost_basis_eur"))
    if proceeds > 0.02 and abs(cost) < 1e-9:
        return False
    return True


def build_tax_ready_economic_gains_us(
    economic_gains: List[Dict[str, Any]],
    fifo_gain_rows: List[Dict[str, Any]] | None = None,
    classified_dicts: List[Dict[str, Any]] | None = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    US tax-ready rows + summary (English-oriented keys in summary).

    Row fields align with DE shape where tooling expects them:
    - speculative_bucket_net_eur = short-term (<=365d) capital component
    - long_term_bucket_net_eur = long-term (>365d) capital component (taxable for US)
    """
    fifo_gain_rows = fifo_gain_rows or []
    out: List[Dict[str, Any]] = []

    total_net = 0.0
    short_term_total = 0.0
    long_term_total = 0.0
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
                "Economic gain row missing dt_iso or tx_hash: "
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
                    "jurisdiction": "US",
                    "cost_basis_complete": False,
                    "capital_gain_term": "n/a",
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
        cost_complete = _cost_basis_complete_for_row(e, fifo_rows, excluded)

        if cat in PVG_CATEGORIES:
            st_net, lt_net, hmin, hmax, legs = _split_capital_net_by_holding_us(e, fifo_rows, fee_eur)

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
                short_term_total += st_net
                long_term_total += lt_net

            if cat == "lp_remove" and net <= 0:
                taxable_flag = False
            else:
                taxable_flag = abs(st_net) + abs(lt_net) > 1e-6

            hold_primary = hmin if fifo_rows else _as_int(e.get("hold_days"))
            term_label = "mixed" if mixed_holding else ("long" if hold_primary > US_LONG_TERM_THRESHOLD_DAYS else "short")

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
                    "speculative_bucket_net_eur": round(st_net, 2),
                    "long_term_bucket_net_eur": round(lt_net, 2),
                    "fifo_leg_count": len(fifo_rows),
                    "fifo_legs_debug": legs if (mixed_holding or len(legs) > 1) else [],
                    "included_in_annual_totals": not excluded,
                    "excluded_from_totals_reason": exclude_reason if excluded else "",
                    "fifo_match_method": fifo_cat,
                    "jurisdiction": "US",
                    "cost_basis_complete": cost_complete,
                    "capital_gain_term": term_label,
                }
            )
            continue

        hold_days = _as_int(e.get("hold_days"))
        taxable_flag = bool(e.get("taxable"))
        if hold_days > US_LONG_TERM_THRESHOLD_DAYS:
            st_part = 0.0
            lt_part = round(net if taxable_flag else 0.0, 2)
        else:
            st_part = round(net if taxable_flag else 0.0, 2)
            lt_part = 0.0

        if not excluded and taxable_flag:
            short_term_total += st_part
            long_term_total += lt_part

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
                "speculative_bucket_net_eur": st_part,
                "long_term_bucket_net_eur": lt_part,
                "fifo_leg_count": 0,
                "fifo_legs_debug": [],
                "included_in_annual_totals": not excluded,
                "excluded_from_totals_reason": exclude_reason if excluded else "",
                "fifo_match_method": fifo_cat,
                "jurisdiction": "US",
                "cost_basis_complete": _cost_basis_complete_for_row(e, fifo_rows, excluded),
                "capital_gain_term": "long" if hold_days > US_LONG_TERM_THRESHOLD_DAYS else "short",
            }
        )

    sum_row_gains_all = round(sum(_as_float(r.get("gain")) for r in out), 2)
    capital_total = round(short_term_total + long_term_total, 2)
    summary = {
        "jurisdiction": "US",
        "total_gains_net_eur": round(total_net, 2),
        "short_term_capital_net_eur": round(short_term_total, 2),
        "long_term_capital_net_eur": round(long_term_total, 2),
        # Total taxable capital gains (ST + LT); unlike DE, long-term is not "tax-free"
        "taxable_gains_net_eur": capital_total,
        "taxfree_gains_net_eur": 0.0,
        "excluded_from_totals_count": int(excluded_count),
        "excluded_from_totals_net_eur": round(excluded_net_sum, 2),
        "sum_row_gains_all_eur": sum_row_gains_all,
        "skipped_events": 0,
        "rows": len(out),
        "suspicious_mixed_holding": suspicious,
        "long_term_threshold_days": US_LONG_TERM_THRESHOLD_DAYS,
        "note": "Amounts in EUR (same unit as pipeline). US reporting often uses USD; convert externally if needed.",
    }
    return out, summary


def build_reward_income_us(classified_dicts: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Ordinary income at receipt (rewards, airdrops, etc.) — US-oriented labels."""
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
                "jurisdiction": "US",
                "income_character": "ordinary",
            }
        )

    summary = {
        "jurisdiction": "US",
        "ordinary_income_eur": round(total_eur, 2),
        "rewards_income_eur": round(total_eur, 2),
        "rows": len(rows),
        "skipped": int(skipped),
    }
    return rows, summary
