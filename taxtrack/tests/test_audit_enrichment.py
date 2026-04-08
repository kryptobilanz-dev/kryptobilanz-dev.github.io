from taxtrack.pdf.audit_enrichment import enrich_tax_ready_rows
from taxtrack.pdf.audit_validation import validate_tax_ready_audit
from taxtrack.pdf.theme.pnl_colors import ROW_BG_GAIN, ROW_BG_NEUTRAL


def test_enrich_adds_trace_fields():
    row = {
        "dt_iso": "2025-01-01T00:00:00",
        "tx_hash": "0xabc",
        "category": "swap",
        "token": "ETH",
        "cost_basis": 1.0,
        "proceeds": 2.0,
        "pnl_gross_eur": 1.0,
        "fees_eur": 0.0,
        "gain": 1.0,
        "taxable": True,
        "holding_period_days_min": 10,
        "holding_period_days_max": 10,
        "speculative_bucket_net_eur": 1.0,
        "long_term_bucket_net_eur": 0.0,
        "fifo_leg_count": 1,
        "fifo_legs_debug": [],
    }
    classified = [
        {
            "tx_hash": "0xabc",
            "direction": "swap",
            "category": "swap",
            "token": "ETH",
            "amount": 1.0,
            "meta": {
                "price_confidence": "high",
                "effective_token_source": "maps_to",
                "tokens_out": [{"token": "ETH", "amount": 1.0, "price_confidence": "high"}],
                "tokens_in": [{"token": "USDC", "amount": 100.0, "price_confidence": "high"}],
            },
        }
    ]
    out = enrich_tax_ready_rows([row], classified, fifo_gain_rows=[])
    r = out[0]
    assert r["source_tx_hash"] == "0xabc"
    assert r["source_rows_count"] == 1
    assert r["price_source"] == "contract_map"
    assert r["price_confidence"] == "high"
    assert "explanation_short" in r
    assert "fifo_lots_used" in r["explanation_details"]
    # Kleines Swap-Netto vs. Volumen → neutrale Zeilenfarbe (kein automatisches „Gewinn-Grün“)
    assert r["audit_row_bg"] == ROW_BG_NEUTRAL


def test_enrich_swap_row_large_net_green():
    row = {
        "dt_iso": "2025-01-01T00:00:00",
        "tx_hash": "0xdef",
        "category": "swap",
        "token": "ETH",
        "cost_basis": 40000.0,
        "proceeds": 41000.0,
        "pnl_gross_eur": 1000.0,
        "fees_eur": 0.0,
        "gain": 1000.0,
        "taxable": True,
        "holding_period_days_min": 10,
        "holding_period_days_max": 10,
        "speculative_bucket_net_eur": 1000.0,
        "long_term_bucket_net_eur": 0.0,
        "fifo_leg_count": 1,
        "fifo_legs_debug": [],
    }
    classified = [
        {
            "tx_hash": "0xdef",
            "direction": "swap",
            "category": "swap",
            "token": "ETH",
            "amount": 1.0,
            "meta": {"price_confidence": "high"},
        }
    ]
    out = enrich_tax_ready_rows([row], classified, fifo_gain_rows=[])
    assert out[0]["audit_row_bg"] == ROW_BG_GAIN


def test_validate_sums_pvg():
    rows = [
        {
            "gain": 5.0,
            "category": "swap",
            "speculative_bucket_net_eur": 5.0,
            "long_term_bucket_net_eur": 0.0,
            "fees_eur": 0.0,
            "tx_hash": "0x1",
        }
    ]
    ts = {"total_gains_net_eur": 5.0, "taxable_gains_net_eur": 5.0, "taxfree_gains_net_eur": 0.0}
    v = validate_tax_ready_audit(rows, ts, [])
    assert v["ok"] is True
