# taxtrack/analyze/gain_grouping.py

from collections import defaultdict
from typing import List, Dict, Any

# Reihenfolge ist wichtig: erste passende Kategorie gewinnt
ECONOMIC_PRIORITY = [
    "position_exit",
    "vault_exit",
    "lp_remove",
    "pendle_redeem",
    "restake_out",
    "swap",
    "sell",
]


def _priority(cat: str) -> int:
    try:
        return ECONOMIC_PRIORITY.index(cat)
    except ValueError:
        return 999


def group_gains_economic(gains: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Gruppiert FIFO-Gains zu wirtschaftlichen Events.
    Eingabe: Liste von Gain-Dicts (aus compute_gains)
    Ausgabe: Liste von verdichteten Events
    """

    # 1) Nach tx_hash gruppieren
    by_tx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for g in gains:
        txh = g.get("tx_hash") or ""
        by_tx[txh].append(g)

    grouped_events: List[Dict[str, Any]] = []

    # 2) Pro tx_hash wirtschaftliche Gruppen bilden
    for txh, rows in by_tx.items():
        def _row_proceeds_eur(r: Dict[str, Any]) -> float:
            try:
                return float(r.get("proceeds_eur") or 0.0)
            except (TypeError, ValueError):
                return 0.0

        def _row_valuation_missing(r: Dict[str, Any]) -> bool:
            m = r.get("meta")
            if isinstance(m, dict):
                return bool(m.get("valuation_missing"))
            return False

        # Detect swap groups by tx_hash (category==swap).
        # Note: at this stage (economic grouping) we may not have `direction` available in gain rows.
        # We still only emit a swap-category event.
        swap_rows_in_tx = [
            r
            for r in rows
            if (r.get("category") or "").lower() == "swap"
        ]
        has_swap_group = bool(swap_rows_in_tx)
        # If the swap gain rows themselves are either zero-proceeds OR explicitly marked missing valuation,
        # we still want an economic event so we don't drop the whole tx.
        swap_rows_zero_or_missing = all(
            (abs(_row_proceeds_eur(r)) < 1e-12) or _row_valuation_missing(r)
            for r in swap_rows_in_tx
        )
        swap_fallback_trigger = has_swap_group and swap_rows_zero_or_missing

        def _emit_event(event_rows: List[Dict[str, Any]], category: str) -> None:
            proceeds = sum(float(r.get("proceeds_eur", 0.0)) for r in event_rows)
            cost = sum(float(r.get("cost_basis_eur", 0.0)) for r in event_rows)
            pnl = sum(float(r.get("pnl_eur", 0.0)) for r in event_rows)
            fees = sum(float(r.get("fee_eur", 0.0)) for r in event_rows)
            net_pnl = pnl - fees

            taxable = any(bool(r.get("taxable")) for r in event_rows)
            hold_days = min(int(r.get("hold_days", 0)) for r in event_rows)

            dt_candidates = [r.get("dt_iso") for r in event_rows if r.get("dt_iso")]
            dt_iso = min(dt_candidates) if dt_candidates else None

            token = (
                event_rows[0].get("token")
                if len({r.get("token") for r in event_rows}) == 1
                else "MULTI"
            )

            # Only mark valuation_missing for the special missing-valuation case;
            # don't invent any prices, only propagate the flag from gain rows.
            any_valuation_missing = any(_row_valuation_missing(r) for r in event_rows)

            grouped_events.append({
                "tx_hash": txh,
                "category": category,
                "dt_iso": dt_iso,
                "token": token,
                "proceeds_eur": round(proceeds, 2),
                "cost_basis_eur": round(cost, 2),
                "pnl_eur": round(pnl, 2),
                "fees_eur": round(fees, 2),
                "net_pnl_eur": round(net_pnl, 2),
                "taxable": taxable,
                "hold_days": hold_days,
                "rows": len(event_rows),
                "valuation_missing": bool(any_valuation_missing and round(proceeds, 2) == 0.0),
            })

        # 2a) Hauptevent bestimmen
        main_rows = sorted(
            rows,
            key=lambda r: _priority(r.get("method") or r.get("category") or ""),
        )

        main_cat = None
        for r in main_rows:
            cat = (r.get("method") or r.get("category") or "").lower()
            if cat in ECONOMIC_PRIORITY:
                main_cat = cat
                break

        # Falls kein Hauptevent: überspringen (reine Transfers)
        if not main_cat:
            # Special-case: don't drop swap txs just because proceeds are 0 and valuation is missing.
            if swap_fallback_trigger:
                _emit_event(swap_rows_in_tx, "swap")
            continue

        # 2b) Relevante Zeilen bestimmen
        # Regel: vault_exit schlägt alle anderen wirtschaftlichen Kategorien derselben TX
        if main_cat == "vault_exit":
            event_rows = rows
        else:
            event_rows = [
                r for r in rows
                if (r.get("method") or r.get("category") or "").lower() == main_cat
            ]

        if main_cat == "vault_exit":
            event_rows = rows
        else:
            event_rows = [
                r for r in rows
                if (r.get("method") or r.get("category") or "").lower() == main_cat
            ]

        if not event_rows:
            # Special-case: when the tx contains swap(direction=swap) but can't be grouped normally,
            # still emit an economic event if we have only zero-proceeds / missing-valuation rows.
            if swap_fallback_trigger:
                _emit_event(swap_rows_in_tx, "swap")
            continue

        # 3) Aggregation
        proceeds = sum(float(r.get("proceeds_eur", 0.0)) for r in event_rows)
        cost = sum(float(r.get("cost_basis_eur", 0.0)) for r in event_rows)
        pnl = sum(float(r.get("pnl_eur", 0.0)) for r in event_rows)
        fees = sum(float(r.get("fee_eur", 0.0)) for r in event_rows)
        net_pnl = pnl - fees

        taxable = any(bool(r.get("taxable")) for r in event_rows)
        hold_days = min(int(r.get("hold_days", 0)) for r in event_rows)

        dt_iso = min(r.get("dt_iso") for r in event_rows if r.get("dt_iso"))
        token = (
            event_rows[0].get("token")
            if len({r.get("token") for r in event_rows}) == 1
            else "MULTI"
        )

        grouped_events.append({
            "tx_hash": txh,
            "category": main_cat,
            "dt_iso": dt_iso,
            "token": token,
            "proceeds_eur": round(proceeds, 2),
            "cost_basis_eur": round(cost, 2),
            "pnl_eur": round(pnl, 2),
            "fees_eur": round(fees, 2),
            "net_pnl_eur": round(net_pnl, 2),
            "taxable": taxable,
            "hold_days": hold_days,
            "rows": len(event_rows),
            "valuation_missing": bool(main_cat == "swap" and any(_row_valuation_missing(r) for r in event_rows) and round(proceeds, 2) == 0.0),
        })
    return grouped_events
