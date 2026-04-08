# taxtrack/tests/test_tax_interpreter_de.py
"""§23 / holding-period interpretation using FIFO legs (no FIFO engine changes)."""

from taxtrack.analyze.tax_interpreter_de import (
    HOLDING_SPECULATION_THRESHOLD_DAYS,
    build_tax_ready_economic_gains_de,
)


def _econ_swap(tx: str, net: float, pnl: float, fee: float = 0.0) -> dict:
    return {
        "tx_hash": tx,
        "category": "swap",
        "dt_iso": "2025-06-01T12:00:00",
        "token": "MULTI",
        "proceeds_eur": 200.0,
        "cost_basis_eur": 20.0,
        "pnl_eur": pnl,
        "fees_eur": fee,
        "net_pnl_eur": net,
        "taxable": True,
        "hold_days": 5,
    }


def test_mixed_holding_swap_splits_buckets():
    """Grouped economic event used min(hold_days)=10; FIFO has 10d + 400d legs → §23 split."""
    tx = "0xabc123"
    fifo = [
        {
            "tx_hash": tx,
            "method": "swap",
            "token": "A",
            "pnl_eur": 50.0,
            "proceeds_eur": 100.0,
            "hold_days": 10,
            "buy_date_iso": "2025-01-01",
        },
        {
            "tx_hash": tx,
            "method": "swap",
            "token": "B",
            "pnl_eur": 50.0,
            "proceeds_eur": 100.0,
            "hold_days": 400,
            "buy_date_iso": "2024-01-01",
        },
    ]
    econ = [_econ_swap(tx, net=100.0, pnl=100.0, fee=0.0)]
    rows, summary = build_tax_ready_economic_gains_de(econ, fifo_gain_rows=fifo)
    assert len(rows) == 1
    r = rows[0]
    assert r["speculative_bucket_net_eur"] == 50.0
    assert r["long_term_bucket_net_eur"] == 50.0
    assert r["holding_period_days_min"] == 10
    assert r["holding_period_days_max"] == 400
    assert summary["taxable_gains_net_eur"] == 50.0
    assert summary["taxfree_gains_net_eur"] == 50.0
    assert len(summary["suspicious_mixed_holding"]) >= 1


def test_fee_split_across_fifo_legs():
    tx = "0xfee"
    fifo = [
        {"tx_hash": tx, "method": "swap", "token": "A", "pnl_eur": 60.0, "proceeds_eur": 60.0, "hold_days": 5},
        {"tx_hash": tx, "method": "swap", "token": "B", "pnl_eur": 40.0, "proceeds_eur": 40.0, "hold_days": 400},
    ]
    econ = [_econ_swap(tx, net=90.0, pnl=100.0, fee=10.0)]
    rows, _ = build_tax_ready_economic_gains_de(econ, fifo_gain_rows=fifo)
    # fee 10 split 60/40 → 6 + 4; nets 54 + 36 = 90
    assert rows[0]["speculative_bucket_net_eur"] == 54.0
    assert rows[0]["long_term_bucket_net_eur"] == 36.0


def test_all_long_term_taxfree_bucket():
    tx = "0xlong"
    fifo = [
        {"tx_hash": tx, "method": "swap", "token": "X", "pnl_eur": 30.0, "proceeds_eur": 50.0, "hold_days": 400},
        {"tx_hash": tx, "method": "swap", "token": "Y", "pnl_eur": 20.0, "proceeds_eur": 50.0, "hold_days": 500},
    ]
    econ = [_econ_swap(tx, net=50.0, pnl=50.0, fee=0.0)]
    rows, summary = build_tax_ready_economic_gains_de(econ, fifo_gain_rows=fifo)
    assert rows[0]["taxable"] is False
    assert rows[0]["speculative_bucket_net_eur"] == 0.0
    assert rows[0]["long_term_bucket_net_eur"] == 50.0
    assert summary["taxable_gains_net_eur"] == 0.0


def test_threshold_day_365_vs_366():
    """hold_days 365 → speculative; 366 → long-term (aligned with tax_rules)."""
    tx = "0xth"
    fifo = [
        {"tx_hash": tx, "method": "swap", "token": "A", "pnl_eur": 10.0, "proceeds_eur": 10.0, "hold_days": 365},
        {"tx_hash": tx, "method": "swap", "token": "B", "pnl_eur": 10.0, "proceeds_eur": 10.0, "hold_days": 366},
    ]
    econ = [_econ_swap(tx, net=20.0, pnl=20.0, fee=0.0)]
    rows, _ = build_tax_ready_economic_gains_de(econ, fifo_gain_rows=fifo)
    assert rows[0]["speculative_bucket_net_eur"] == 10.0
    assert rows[0]["long_term_bucket_net_eur"] == 10.0
    assert HOLDING_SPECULATION_THRESHOLD_DAYS == 365


def test_position_exit_fallback_no_fifo_rows():
    econ = [
        {
            "tx_hash": "0xp",
            "category": "position_exit",
            "dt_iso": "2025-01-01T00:00:00",
            "token": "MULTI",
            "proceeds_eur": 100.0,
            "cost_basis_eur": 50.0,
            "pnl_eur": 50.0,
            "fees_eur": 0.0,
            "net_pnl_eur": 50.0,
            "hold_days": 400,
            "taxable": True,
        }
    ]
    rows, summary = build_tax_ready_economic_gains_de(econ, fifo_gain_rows=[])
    assert rows[0]["long_term_bucket_net_eur"] == 50.0
    assert rows[0]["speculative_bucket_net_eur"] == 0.0
    assert rows[0]["taxable"] is False


def test_lp_remove_net_loss_not_taxable_flag():
    tx = "0xlp"
    fifo = [
        {"tx_hash": tx, "method": "lp_remove", "token": "A", "pnl_eur": -30.0, "proceeds_eur": 10.0, "hold_days": 10},
    ]
    econ = [
        {
            "tx_hash": tx,
            "category": "lp_remove",
            "dt_iso": "2025-06-01T12:00:00",
            "token": "LP",
            "proceeds_eur": 10.0,
            "cost_basis_eur": 40.0,
            "pnl_eur": -30.0,
            "fees_eur": 0.0,
            "net_pnl_eur": -30.0,
            "hold_days": 10,
            "taxable": True,
        }
    ]
    rows, _ = build_tax_ready_economic_gains_de(econ, fifo_gain_rows=fifo)
    assert rows[0]["taxable"] is False


def test_unreliable_classified_swap_out_excluded_from_headline_totals():
    """Low-confidence swap-out legs must not drive annual total_gains_net_eur (marketing-safe headline)."""
    tx = "0xbadswap"
    fifo = [
        {
            "tx_hash": tx,
            "method": "swap",
            "token": "MOO",
            "pnl_eur": 5000.0,
            "proceeds_eur": 6000.0,
            "hold_days": 10,
        },
    ]
    econ = [
        {
            "tx_hash": tx,
            "category": "swap",
            "dt_iso": "2025-06-01T12:00:00",
            "token": "MULTI",
            "proceeds_eur": 6000.0,
            "cost_basis_eur": 1000.0,
            "pnl_eur": 5000.0,
            "fees_eur": 0.0,
            "net_pnl_eur": 5000.0,
            "taxable": True,
            "hold_days": 10,
            "valuation_missing": False,
        }
    ]
    classified = [
        {
            "tx_hash": tx,
            "direction": "swap",
            "category": "swap",
            "token": "MOO",
            "amount": 1.0,
            "eur_value": 6000.0,
            "meta": {
                "tokens_out": [
                    {"token": "MOO", "amount": 1.0, "eur_value": 6000.0, "price_confidence": "low"},
                ],
                "tokens_in": [{"token": "ETH", "amount": 2.0, "eur_value": 6000.0, "price_confidence": "high"}],
            },
        }
    ]
    rows, summary = build_tax_ready_economic_gains_de(
        econ, fifo_gain_rows=fifo, classified_dicts=classified
    )
    assert rows[0]["gain"] == 5000.0
    assert rows[0]["included_in_annual_totals"] is False
    assert rows[0]["excluded_from_totals_reason"] == "classified_swap_out_low_confidence"
    assert summary["total_gains_net_eur"] == 0.0
    assert summary["taxable_gains_net_eur"] == 0.0
    assert summary["excluded_from_totals_count"] == 1
    assert summary["excluded_from_totals_net_eur"] == 5000.0
    assert summary["sum_row_gains_all_eur"] == 5000.0


def test_near_zero_proceeds_material_net_excluded():
    tx = "0xbroken"
    econ = [
        {
            "tx_hash": tx,
            "category": "swap",
            "dt_iso": "2025-06-01T12:00:00",
            "token": "X",
            "proceeds_eur": 0.0,
            "cost_basis_eur": 5000.0,
            "pnl_eur": -5000.0,
            "fees_eur": 0.0,
            "net_pnl_eur": -5000.0,
            "taxable": True,
            "hold_days": 10,
        }
    ]
    rows, summary = build_tax_ready_economic_gains_de(econ, fifo_gain_rows=[], classified_dicts=[])
    assert rows[0]["included_in_annual_totals"] is False
    assert rows[0]["excluded_from_totals_reason"] == "near_zero_proceeds_material_net"
    assert summary["total_gains_net_eur"] == 0.0
    assert summary["sum_row_gains_all_eur"] == -5000.0
