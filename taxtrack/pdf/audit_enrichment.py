# taxtrack/pdf/audit_enrichment.py
"""
Post-processing metadata for tax-ready economic rows (no FIFO/tax/price changes).

Adds traceability and explainability fields from classified rows + FIFO gain rows.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from taxtrack.pdf.theme.pnl_colors import row_bg_for_tax_row

_CONF_ORDER = {"low": 0, "medium": 1, "high": 2}


def _worst_confidence(*levels: Optional[str]) -> str:
    worst = "high"
    worst_i = 2
    for lv in levels:
        if not lv:
            continue
        k = (lv or "").lower().strip()
        if k not in _CONF_ORDER:
            continue
        if _CONF_ORDER[k] < worst_i:
            worst_i = _CONF_ORDER[k]
            worst = k
    return worst


def _meta_confidence(meta: Any) -> Optional[str]:
    if not isinstance(meta, dict):
        return None
    return (meta.get("price_confidence") or "").lower() or None


def _collect_leg_confidences(meta: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for key in ("tokens_in", "tokens_out"):
        legs = meta.get(key) or []
        if not isinstance(legs, list):
            continue
        for leg in legs:
            if isinstance(leg, dict) and leg.get("price_confidence"):
                out.append(str(leg["price_confidence"]).lower())
    return out


def _index_classified_by_tx(classified_dicts: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_tx: Dict[str, List[Dict[str, Any]]] = {}
    for r in classified_dicts or []:
        txh = str(r.get("tx_hash") or "").strip().lower()
        if not txh:
            continue
        by_tx.setdefault(txh, []).append(r)
    return by_tx


def _merge_token_snapshots(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Union token legs from swap meta + direction-based rows."""
    ins: List[Dict[str, Any]] = []
    outs: List[Dict[str, Any]] = []
    seen_in: set = set()
    seen_out: set = set()

    def _add(side: str, token: str, amount: Any) -> None:
        try:
            a = float(amount or 0.0)
        except Exception:
            a = 0.0
        t = (token or "").upper().strip()
        if not t:
            return
        key = (t, round(a, 12))
        if side == "in":
            if key not in seen_in:
                seen_in.add(key)
                ins.append({"token": t, "amount": a})
        else:
            if key not in seen_out:
                seen_out.add(key)
                outs.append({"token": t, "amount": a})

    for r in rows:
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        for leg in meta.get("tokens_in") or []:
            if isinstance(leg, dict):
                _add("in", leg.get("token"), leg.get("amount"))
        for leg in meta.get("tokens_out") or []:
            if isinstance(leg, dict):
                _add("out", leg.get("token"), leg.get("amount"))

        dirn = (r.get("direction") or "").lower()
        cat = (r.get("category") or "").lower()
        if not meta.get("tokens_in") and not meta.get("tokens_out"):
            tok = (r.get("token") or "").upper()
            amt = r.get("amount")
            if dirn == "in":
                _add("in", tok, amt)
            elif dirn in ("out", "swap"):
                if cat == "swap" and dirn == "swap":
                    _add("out", tok, amt)
                elif dirn == "out":
                    _add("out", tok, amt)

    return ins, outs


def _infer_price_source(rows: List[Dict[str, Any]]) -> str:
    """
    Single label: contract_map > derived > direct.
    Uses only existing meta flags (no guessing).
    """
    has_maps = False
    has_derived_leg = False
    has_direct_path = False

    for r in rows:
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        src = (meta.get("effective_token_source") or "").lower()
        if src == "maps_to":
            has_maps = True

        for leg in (meta.get("tokens_in") or []) + (meta.get("tokens_out") or []):
            if not isinstance(leg, dict):
                continue
            pc = (leg.get("price_confidence") or "").lower()
            if pc == "medium":
                has_derived_leg = True
            if pc == "high":
                has_direct_path = True

        pc_row = (meta.get("price_confidence") or "").lower()
        if pc_row == "high" and (meta.get("tokens_in") or meta.get("tokens_out")):
            has_direct_path = True

        cat = (r.get("category") or "").lower()
        if cat != "swap" and pc_row == "high":
            has_direct_path = True

    if has_maps:
        return "contract_map"
    if has_derived_leg:
        return "derived"
    if has_direct_path:
        return "direct"
    return "direct"


def _aggregate_price_confidence(rows: List[Dict[str, Any]]) -> str:
    levels: List[str] = []
    for r in rows:
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        mc = _meta_confidence(meta)
        if mc:
            levels.append(mc)
        levels.extend(_collect_leg_confidences(meta))
    if not levels:
        return "medium"
    return _worst_confidence(*levels)


def _build_explanation_short(row: Dict[str, Any]) -> str:
    if str(row.get("jurisdiction") or "").upper() == "US":
        cat = (row.get("category") or "").lower()
        tok = (row.get("token") or "").upper()
        net = row.get("gain")
        fifo_n = int(row.get("fifo_leg_count") or 0)
        hmin = row.get("holding_period_days_min", "")
        hmax = row.get("holding_period_days_max", "")
        term = (row.get("capital_gain_term") or "").lower()
        tax = "taxable (capital)" if row.get("taxable") else "non-taxable / zero"
        if cat == "swap":
            return (
                f"Swap: realized disposal (FIFO). Net {net} EUR; token {tok}; "
                f"FIFO lots {fifo_n}; hold {hmin}–{hmax} d; {tax}; term={term or 'n/a'}"
            )
        return f"{cat} · token {tok}; net {net} EUR; FIFO lots {fifo_n}; hold {hmin}–{hmax} d; {tax}"

    cat = (row.get("category") or "").lower()
    tok = (row.get("token") or "").upper()
    net = row.get("gain")
    fifo_n = int(row.get("fifo_leg_count") or 0)
    hmin = row.get("holding_period_days_min", "")
    hmax = row.get("holding_period_days_max", "")
    tax = "steuerpflichtig (≤365d)" if row.get("taxable") else "steuerfrei / langfristig"
    if cat == "swap":
        return (
            f"Swap: steuerliche Realisierung (FIFO auf die veräußerte Position); "
            f"Netto {net} EUR = Veräußerungserlös − Anschaffungskosten (nicht gleich „Gewinn“ im Alltagssinn). "
            f"Token {tok}; FIFO-Lots {fifo_n}; Haltedauer {hmin}–{hmax} Tage; {tax}"
        )
    return (
        f"{cat} · Token {tok}; Netto {net} EUR; FIFO-Lots {fifo_n}; "
        f"Haltedauer {hmin}–{hmax} Tage; {tax}"
    )


def _build_explanation_details(
    row: Dict[str, Any],
    fifo_gain_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    txh = (row.get("tx_hash") or "").strip()
    cat = (row.get("fifo_match_method") or row.get("category") or "").lower()
    fifo_matched = [
        g
        for g in fifo_gain_rows or []
        if (g.get("tx_hash") or "").strip() == txh and (g.get("method") or "").lower() == cat
    ]
    legs_debug = row.get("fifo_legs_debug") or []
    return {
        "cost_basis_eur": row.get("cost_basis"),
        "proceeds_eur": row.get("proceeds"),
        "fees_eur": row.get("fees_eur"),
        "pnl_gross_eur": row.get("pnl_gross_eur"),
        "fifo_lots_used": fifo_matched,
        "fifo_legs_debug": legs_debug,
        "speculative_bucket_net_eur": row.get("speculative_bucket_net_eur"),
        "long_term_bucket_net_eur": row.get("long_term_bucket_net_eur"),
    }


def _build_derivation_path(row: Dict[str, Any], src_rows: List[Dict[str, Any]]) -> List[str]:
    path: List[str] = []
    ps = (row.get("price_source") or "").lower()
    cat = (row.get("category") or "").lower()
    if ps == "direct":
        path.append("direct_price")
    elif ps == "contract_map":
        path.append("contract_map")
    elif ps == "recovered":
        path.append("swap_recovery")
    elif ps == "derived":
        path.append("swap_leg_out")
    if row.get("fifo_leg_count"):
        path.append("fifo_lot")
    if cat == "swap":
        path.append("swap_group")
    return path


def _build_source_trace(
    row: Dict[str, Any],
    tokens_in: List[Dict[str, Any]],
    tokens_out: List[Dict[str, Any]],
) -> Dict[str, Any]:
    source_tx = str(row.get("tx_hash") or "").strip()
    src_hashes: List[str] = [source_tx] if source_tx else []
    source_tokens: List[str] = []
    source_amounts: List[float] = []
    for leg in (tokens_in or []) + (tokens_out or []):
        if not isinstance(leg, dict):
            continue
        tok = str(leg.get("token") or "").upper().strip()
        if tok:
            source_tokens.append(tok)
        try:
            source_amounts.append(float(leg.get("amount") or 0.0))
        except Exception:
            source_amounts.append(0.0)
    return {
        "source_tx_hashes": src_hashes,
        "source_tokens": source_tokens,
        "source_amounts": source_amounts,
    }


def enrich_tax_ready_rows(
    tax_ready: List[Dict[str, Any]],
    classified_dicts: List[Dict[str, Any]],
    fifo_gain_rows: List[Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    """
    Mutates each dict in tax_ready in place: adds audit/trace fields.
    """
    fifo_gain_rows = fifo_gain_rows or []
    by_tx = _index_classified_by_tx(classified_dicts)

    for row in tax_ready:
        txh = str(row.get("tx_hash") or "").strip()
        txk = txh.lower()
        src_rows = by_tx.get(txk, [])
        tokens_in, tokens_out = _merge_token_snapshots(src_rows)

        row["source_tx_hash"] = txh
        row["source_rows_count"] = len(src_rows)
        row["tokens_in"] = tokens_in
        row["tokens_out"] = tokens_out

        ps = _infer_price_source(src_rows)
        pc = _aggregate_price_confidence(src_rows)
        row["price_source"] = ps
        row["price_confidence"] = pc
        row["price_origin"] = ps
        row["derivation_path"] = _build_derivation_path(row, src_rows)
        row["source_trace"] = _build_source_trace(row, tokens_in, tokens_out)

        row["explanation_short"] = _build_explanation_short(row)
        row["explanation_details"] = _build_explanation_details(row, fifo_gain_rows)

        row["audit_row_bg"] = row_bg_for_tax_row(row)

    return tax_ready
