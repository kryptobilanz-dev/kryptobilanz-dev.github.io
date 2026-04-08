from taxtrack.pdf.utils import group_lp_gains

def test_group_lp_gains():
    records = [
        {"category": "lp_remove", "tx_hash": "0xabc", "eur_value": 100},
        {"category": "lp_remove", "tx_hash": "0xabc", "eur_value": 50},
        {"category": "swap", "tx_hash": "0xdef", "eur_value": 200},
    ]

    groups = group_lp_gains(records)

    assert "0xabc" in groups
    assert len(groups["0xabc"]) == 2
    assert "0xdef" not in groups
