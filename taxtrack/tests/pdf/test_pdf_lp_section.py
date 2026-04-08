from taxtrack.pdf.pdf_report import build_pdf
from pathlib import Path

def test_pdf_contains_lp_section(tmp_path):
    records = [
        {
            "dt_iso": "2024-06-01T12:00:00",
            "category": "lp_remove",
            "tx_hash": "0xabc123",
            "token": "LP::eth::pool1",
            "eur_value": 1234.56,
            "counterparty": "uniswap",
            "taxable": True,
        }
    ]

    out = tmp_path / "lp_test.pdf"
    build_pdf(records, summary={}, debug_info={}, outpath=out)

    assert out.exists()
