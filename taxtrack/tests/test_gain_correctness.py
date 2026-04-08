# taxtrack/tests/test_gain_correctness.py
"""Critical correctness: valuation_missing, restake vs swap, pipeline consistency."""

import pytest

from taxtrack.analyze.tax_interpreter_de import build_tax_ready_economic_gains_de
from taxtrack.root.pipeline import _validate_pipeline_consistency_or_raise
from taxtrack.rules.evaluate import _refine_category


def test_valuation_missing_zeros_tax_ready_gain():
    """Classified meta.valuation_missing True → tax_ready gain 0, excluded, reason valuation_missing."""
    tx = "0xvalmissingtest00000000000000000000000000000000000000000000000001"
    econ = [
        {
            "tx_hash": tx,
            "category": "swap",
            "dt_iso": "2025-07-14T21:00:00+00:00",
            "token": "X",
            "proceeds_eur": 999.0,
            "cost_basis_eur": 100.0,
            "pnl_eur": 899.0,
            "net_pnl_eur": 899.0,
            "fees_eur": 0.0,
            "taxable": True,
            "hold_days": 5,
        }
    ]
    classified = [
        {
            "tx_hash": tx,
            "category": "swap",
            "meta": {"valuation_missing": True},
        }
    ]
    rows, _ = build_tax_ready_economic_gains_de(econ, fifo_gain_rows=[], classified_dicts=classified)
    assert len(rows) == 1
    assert rows[0]["gain"] == 0.0
    assert rows[0]["included_in_annual_totals"] is False
    assert rows[0]["excluded_from_totals_reason"] == "valuation_missing"


def test_restake_router_is_not_swap_category():
    """cp_protocol restake + restake_router must classify as restake_in/out, not swap."""
    cp_info = {
        "label": "restake_router",
        "protocol": "restake",
        "type": "router",
        "tags": ["restake", "swap"],
    }
    cat_out = _refine_category(
        base_category="transfer",
        raw_category="erc20_transfer",
        method="ERC20_TRANSFER",
        direction="out",
        wallet="0xwallet0000000000000000000000000000000000000",
        from_addr="0xwallet0000000000000000000000000000000000000",
        to_addr="0xrouter000000000000000000000000000000000000",
        cp_info=cp_info,
    )
    assert cat_out != "swap"
    assert cat_out in ("restake_in", "restake_out")

    cat_in = _refine_category(
        base_category="transfer",
        raw_category="erc20_transfer",
        method="ERC20_TRANSFER",
        direction="in",
        wallet="0xwallet0000000000000000000000000000000000000",
        from_addr="0xfrom00000000000000000000000000000000000000",
        to_addr="0xwallet0000000000000000000000000000000000000",
        cp_info=cp_info,
    )
    assert cat_in != "swap"
    assert cat_in in ("restake_in", "restake_out")


def test_pipeline_consistency_raises_on_gain_mismatch():
    """Mismatched tax_ready.gain vs gains.net_pnl_eur must raise RuntimeError."""
    tx = "0xmismatchtest00000000000000000000000000000000000000000000000001"
    classified = [{"tx_hash": tx, "category": "swap", "meta": {}}]
    gains_rows = [
        {
            "tx_hash": tx,
            "category": "swap",
            "pnl_eur": 0.0,
            "net_pnl_eur": 0.0,
        }
    ]
    tax_rows = [{"tx_hash": tx, "category": "swap", "gain": 500.0}]
    with pytest.raises(RuntimeError, match="Inconsistent gain for tx"):
        _validate_pipeline_consistency_or_raise(
            classified,
            gains_rows,
            tax_rows,
            wallet_id="test_wallet",
            tax_year=2025,
        )
