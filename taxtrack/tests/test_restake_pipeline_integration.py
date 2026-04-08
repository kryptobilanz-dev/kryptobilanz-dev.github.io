# taxtrack/tests/test_restake_pipeline_integration.py
"""End-to-end: evaluate_batch → compute_gains for restake_in / restake_out."""

from __future__ import annotations

import pytest

from taxtrack.rules.evaluate import evaluate_batch
from taxtrack.analyze.gains import compute_gains

WALLET = "0x1111111111111111111111111111111111111111"
RESTAKE_ROUTER = "0x5cc9400ffb4da168cf271e912f589462c3a00d1f"  # address_map: restake, eth
BURN = "0x0000000000000000000000000000000000000000"


def _tx(
    *,
    tx_hash: str,
    dt_iso: str,
    token: str,
    amount: float,
    eur_value: float,
    direction: str,
    from_addr: str,
    to_addr: str,
    category: str = "",
    method: str = "ERC20_TRANSFER",
) -> dict:
    return {
        "tx_hash": tx_hash,
        "dt_iso": dt_iso,
        "token": token,
        "amount": amount,
        "eur_value": eur_value,
        "direction": direction,
        "from": from_addr,
        "to": to_addr,
        "method": method,
        "category": category,
        "chain_id": "eth",
        "meta": {"owner_wallet": WALLET},
    }


def test_restake_in_then_restake_out_fifo_and_gain():
    """
    Stake: restake_in (WSTETH out) → synthetic lot LRT_WSTETH.
    Unstake: restake_in (ETH in) + internal out leg (WSTETH burn) same tx → disposal consumes lot, PnL = proceeds - cost.
    """
    stake_tx = "0xrestakestake00000000000000000000000000000000000000000000000001"
    exit_tx = "0xrestakeexit00000000000000000000000000000000000000000000000002"

    txs = [
        # Deposit to EigenLayer / restake router
        _tx(
            tx_hash=stake_tx,
            dt_iso="2025-01-01T12:00:00+00:00",
            token="WSTETH",
            amount=10.0,
            eur_value=100.0,
            direction="out",
            from_addr=WALLET,
            to_addr=RESTAKE_ROUTER,
        ),
        # Exit: burn leg (not restake_in; kept internal so gains loop skips FIFO on raw WSTETH)
        _tx(
            tx_hash=exit_tx,
            dt_iso="2025-06-01T12:00:00+00:00",
            token="WSTETH",
            amount=10.0,
            eur_value=0.0,
            direction="out",
            from_addr=WALLET,
            to_addr=BURN,
            category="internal_transfer",
        ),
        # Proceeds
        _tx(
            tx_hash=exit_tx,
            dt_iso="2025-06-01T12:00:00+00:00",
            token="ETH",
            amount=10.0,
            eur_value=130.0,
            direction="in",
            from_addr=RESTAKE_ROUTER,
            to_addr=WALLET,
        ),
    ]

    classified, _dbg = evaluate_batch(txs, WALLET)

    cats = {(c.category or "").lower() for c in classified}
    assert "swap" not in cats, "restake flow must not be grouped as swap"
    assert "restake_in" in cats
    assert "restake_out" in cats

    gains, totals = compute_gains(classified)

    restake_gains = [g for g in gains if (g.method or "").lower() == "restake_out"]
    assert restake_gains, "restake_out must produce at least one gain row (lot consumed)"
    assert len(restake_gains) == 1

    g = restake_gains[0]
    assert (g.token or "").upper() == "LRT_WSTETH"
    assert abs(g.amount_out - 10.0) < 1e-9
    assert abs(g.proceeds_eur - 130.0) < 0.02
    assert abs(g.cost_basis_eur - 100.0) < 0.02
    assert abs(g.pnl_eur - 30.0) < 0.02

    assert abs(totals.get("LRT_WSTETH", 0.0) - 30.0) < 0.02


def test_restake_router_out_is_never_swap():
    """Guard: single leg to restake router must classify as restake_in, not swap."""
    txh = "0xrestakesingle00000000000000000000000000000000000000000000000003"
    txs = [
        _tx(
            tx_hash=txh,
            dt_iso="2025-03-01T10:00:00+00:00",
            token="WSTETH",
            amount=1.0,
            eur_value=50.0,
            direction="out",
            from_addr=WALLET,
            to_addr=RESTAKE_ROUTER,
        ),
    ]
    classified, _ = evaluate_batch(txs, WALLET)
    assert len(classified) == 1
    assert (classified[0].category or "").lower() == "restake_in"
    assert (classified[0].direction or "").lower() == "out"
