# taxtrack/validation/harvest_consistency.py
"""In-memory checks: classified vs economic gains (gains.json) vs tax_ready."""

from __future__ import annotations

from typing import Any

# Strict 2-decimal EUR match: tax_ready.gain must equal gains.net_pnl_eur (after rounding).
GAIN_STRICT_DECIMALS = 2
REALIZATION_CATEGORIES = frozenset({"swap", "sell", "position_exit"})


def _norm_tx(h: str | None) -> str:
    if not h:
        return ""
    return str(h).strip().lower()


def _as_float(x: Any) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _gains_reference_for_tax_gain(g: dict[str, Any]) -> float:
    """tax_ready ``gain`` follows economic ``net_pnl_eur`` after fees (interpreter); fallback ``pnl_eur``."""
    if g.get("net_pnl_eur") is not None:
        return _as_float(g.get("net_pnl_eur"))
    return _as_float(g.get("pnl_eur"))


def _find_gains_row(
    gains_rows: list[dict[str, Any]],
    tx_norm: str,
    category: str | None,
) -> dict[str, Any] | None:
    same_tx = [g for g in gains_rows if _norm_tx(g.get("tx_hash")) == tx_norm]
    if not same_tx:
        return None
    if len(same_tx) == 1:
        return same_tx[0]
    cat = (category or "").strip().lower()
    for g in same_tx:
        if str(g.get("category", "")).strip().lower() == cat:
            return g
    return same_tx[0]


def validate_consistency_lists(
    classified: list[dict[str, Any]],
    gains_rows: list[dict[str, Any]],
    tax_rows: list[dict[str, Any]],
    *,
    wallet_id: str = "pipeline",
    year: str | int = "",
) -> list[str]:
    """
    Same rules as taxtrack.tools.check_consistency for JSON files.
    Returns only [FAIL] ... lines (no [SKIP]).

    Raises:
        RuntimeError: tax_ready.gain != gains.net_pnl_eur (2-decimal), or
            valuation_missing tx with non-zero tax_ready.gain, or
            tax_ready swap without classified swap (FALSE_SWAP), or
            tax_ready swap when classified indicates cp_protocol restake.
    """
    errors: list[str] = []
    y = str(year) if year is not None else ""

    classified_by_tx: dict[str, list[dict[str, Any]]] = {}
    for c in classified:
        k = _norm_tx(c.get("tx_hash"))
        if k:
            classified_by_tx.setdefault(k, []).append(c)

    realization_count: dict[str, int] = {}
    for tr in tax_rows:
        cat = str(tr.get("category", "")).strip().lower()
        if cat not in REALIZATION_CATEGORIES:
            continue
        k = _norm_tx(tr.get("tx_hash"))
        if not k:
            continue
        realization_count[k] = realization_count.get(k, 0) + 1
    for tx_norm, n in sorted(realization_count.items()):
        if n > 1:
            errors.append(
                f"[FAIL] DUPLICATE_REALIZATION tx={tx_norm} wallet={wallet_id} year={y} "
                f"count={n} (max 1 for swap/sell/position_exit)"
            )

    reported_missing_gain: set[str] = set()
    for tr in tax_rows:
        tx_norm = _norm_tx(tr.get("tx_hash"))
        if not tx_norm:
            errors.append(
                f"[FAIL] MISSING_TX_HASH wallet={wallet_id} year={y} tax_ready_row={tr.get('id', '?')}"
            )
            continue

        g = _find_gains_row(gains_rows, tx_norm, tr.get("category"))
        if g is None:
            if tx_norm not in reported_missing_gain:
                reported_missing_gain.add(tx_norm)
                errors.append(
                    f"[FAIL] MISSING_GAIN_ENTRY tx={tx_norm} wallet={wallet_id} year={y}"
                )
            continue

        gain_tr = _as_float(tr.get("gain"))
        ref_net = _gains_reference_for_tax_gain(g)
        if round(gain_tr, GAIN_STRICT_DECIMALS) != round(ref_net, GAIN_STRICT_DECIMALS):
            tx_display = str(tr.get("tx_hash") or tx_norm or "")
            raise RuntimeError(f"Inconsistent gain for tx {tx_display}")

    for tr in tax_rows:
        tax_category = str(tr.get("category", "")).strip().lower()
        if tax_category != "swap":
            continue
        tx_norm = _norm_tx(tr.get("tx_hash"))
        if not tx_norm:
            continue
        cls_rows = classified_by_tx.get(tx_norm, [])
        has_classified_swap = any(
            str(c.get("category", "")).strip().lower() == "swap" for c in cls_rows
        )
        has_restake_proto = any(
            isinstance(c.get("meta"), dict)
            and str((c.get("meta") or {}).get("cp_protocol") or "").strip().lower() == "restake"
            for c in cls_rows
        )
        # Classified must include swap when tax_ready is swap; restake protocol forbids swap entirely.
        if has_restake_proto or not has_classified_swap:
            raise RuntimeError("FALSE_SWAP detected")

    txs_with_vm: set[str] = set()
    for c in classified:
        meta = c.get("meta") if isinstance(c.get("meta"), dict) else {}
        if meta.get("valuation_missing"):
            k = _norm_tx(c.get("tx_hash"))
            if k:
                txs_with_vm.add(k)

    for tx_norm in sorted(txs_with_vm):
        for tr in tax_rows:
            if _norm_tx(tr.get("tx_hash")) != tx_norm:
                continue
            gain_tr = _as_float(tr.get("gain"))
            if round(gain_tr, GAIN_STRICT_DECIMALS) != 0:
                raise RuntimeError("valuation_missing but gain != 0")

    return errors
