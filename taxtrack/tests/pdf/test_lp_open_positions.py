def test_open_lp_positions_rendered(tmp_path):
    records = []
    summary = {
        "open_lp_positions": [
            {
                "lp_token": "LP::eth::0xPOOL",
                "amount": 1.0,
                "cost_basis_eur": 3500.0,
                "acquired_at": "2024-06-01",
            }
        ]
    }

    from taxtrack.pdf.pdf_report import build_pdf

    out = tmp_path / "lp_open.pdf"
    build_pdf(records, summary, {}, out)

    assert out.exists()
