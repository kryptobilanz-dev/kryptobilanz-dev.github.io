import pytest

@pytest.fixture
def records_with_lp():
    return [
        {
            "dt_iso": "2024-06-01T12:00:00",
            "category": "lp_remove",
            "tx_hash": "0xabc",
            "token": "LP::eth::pool1",
            "amount": -1,
            "eur_value": 1000,
            "counterparty": "uniswap",
            "taxable": True,
        },
        {
            "dt_iso": "2024-06-01T12:00:00",
            "category": "swap",
            "tx_hash": "0xdef",
            "token": "ETH",
            "amount": -0.1,
            "eur_value": 200,
            "counterparty": "uniswap",
            "taxable": True,
        },
    ]
