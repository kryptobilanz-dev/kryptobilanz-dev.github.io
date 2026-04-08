from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from taxtrack.analyze.gains import compute_gains


def _dt(days_ago: int) -> str:
    return (datetime.utcnow() - timedelta(days=days_ago)).isoformat()


@dataclass
class _CI:
    tx_hash: str
    dt_iso: str
    token: str
    amount: float
    eur_value: float
    from_addr: str = "0x"
    to_addr: str = "0x"
    direction: str = ""
    category: str = ""
    method: str = ""
    meta: dict | None = None


def test_partial_lot_consumption_scales_cost_basis():
    items = [
        _CI(tx_hash="0xBUY", dt_iso=_dt(10), token="ETH", amount=10.0, eur_value=100.0, direction="in", category="buy"),
        _CI(tx_hash="0xSELL", dt_iso=_dt(1), token="ETH", amount=-4.0, eur_value=60.0, direction="out", category="sell"),
    ]
    gains, totals = compute_gains(items)
    assert len(gains) == 1
    g = gains[0]
    assert g.amount_out == 4.0
    # cost basis for 4/10 of 100€ = 40€
    assert g.cost_basis_eur == 40.0
    assert g.proceeds_eur == 60.0
    assert g.pnl_eur == 20.0


def test_internal_transfer_creates_no_lots_and_no_gains():
    items = [
        _CI(tx_hash="0xINT", dt_iso=_dt(5), token="ETH", amount=-1.0, eur_value=2000.0, direction="out", category="internal_transfer"),
        _CI(tx_hash="0xINT", dt_iso=_dt(5), token="ETH", amount=1.0, eur_value=2000.0, direction="in", category="internal_transfer"),
    ]
    gains, totals = compute_gains(items)
    assert gains == []
    summary = totals.get("fifo_summary") or {}
    assert summary.get("lots_created") == 0.0
    assert summary.get("lots_consumed") == 0.0


def test_missing_price_disposal_preserved_with_gain_row():
    meta = {}
    items = [
        _CI(tx_hash="0xSELL", dt_iso=_dt(20), token="ETH", amount=-1.0, eur_value=0.0, direction="out", category="sell", meta=meta),
        _CI(tx_hash="0xBUY", dt_iso=_dt(1), token="ETH", amount=1.0, eur_value=1000.0, direction="in", category="buy"),
    ]
    gains, totals = compute_gains(items)
    # Sell first: no lots -> negative_balance row, disposal preserved
    assert len(gains) == 1
    assert gains[0].proceeds_eur == 0.0
    assert gains[0].meta and gains[0].meta.get("negative_balance") is True
    assert meta.get("valuation_missing") is True
    summary = totals.get("fifo_summary") or {}
    assert summary.get("skipped_missing_price") >= 1.0


def test_missing_price_after_buy_consumes_lot_negative_pnl():
    meta = {}
    items = [
        _CI(tx_hash="0xBUY", dt_iso=_dt(20), token="ETH", amount=1.0, eur_value=1000.0, direction="in", category="buy"),
        _CI(tx_hash="0xSELL", dt_iso=_dt(1), token="ETH", amount=-1.0, eur_value=0.0, direction="out", category="sell", meta=meta),
    ]
    gains, totals = compute_gains(items)
    assert len(gains) == 1
    assert gains[0].proceeds_eur == 0.0
    assert gains[0].cost_basis_eur == 1000.0
    assert gains[0].pnl_eur == -1000.0
    assert gains[0].meta and gains[0].meta.get("valuation_missing") is True


def test_swap_disposal_only_on_tokens_out_and_acquire_tokens_in():
    meta = {
        "tokens_out": [{"token": "ETH", "amount": 1.0, "eur_value": 2000.0}],
        "tokens_in": [{"token": "USDC", "amount": 2000.0, "eur_value": 2000.0}],
        "total_out_value_eur": 2000.0,
    }
    items = [
        _CI(tx_hash="0xBUY", dt_iso=_dt(20), token="ETH", amount=1.0, eur_value=1000.0, direction="in", category="buy"),
        _CI(tx_hash="0xSWAP", dt_iso=_dt(1), token="ETH", amount=1.0, eur_value=2000.0, direction="swap", category="swap", meta=meta),
    ]
    gains, totals = compute_gains(items)
    # one disposal gain row for ETH
    assert len(gains) == 1
    assert gains[0].token == "ETH"
    # USDC must not create a disposal; it becomes a new lot (no direct assert, but should not create extra gains)

