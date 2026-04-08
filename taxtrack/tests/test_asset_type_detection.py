from __future__ import annotations

import pytest

from taxtrack.root.pipeline import classify_asset_type, is_non_realizing_swap


@pytest.mark.parametrize(
    "token, expected",
    [
        ("ETH", "base_asset"),
        ("BTC", "base_asset"),
        ("USDC", "stable"),
        ("USDT", "stable"),
        ("DAI", "stable"),
        ("EUR", "stable"),
        ("USD", "stable"),
        ("stETH", "derivative"),
        ("wstETH", "derivative"),
        ("eETH", "derivative"),
        ("ezETH", "derivative"),
        ("rsETH", "derivative"),
        ("rETH", "derivative"),
        ("cbETH", "derivative"),
        ("LRT_EETH", "derivative"),
        ("LRT_WEETH", "derivative"),
        ("PENDLE_LPT", "position_token"),
        ("MOOETH", "position_token"),
        ("BEEFY_VAULT", "position_token"),
        ("UNI-V2", "unknown"),  # no LP substring; keep strict
        ("", "unknown"),
        (None, "unknown"),
        ("UNKNOWN", "unknown"),
        ("UNKNOWN_CONTRACT", "unknown"),
        ("ERC20 ***", "unknown"),
    ],
)
def test_classify_asset_type(token, expected):
    assert classify_asset_type(token) == expected


def test_is_non_realizing_swap_rule_triggers_only_on_derivative_to_position_neutral_derived():
    tx = {
        "category": "swap",
        "price_source": "derived",
        "tokens_out": [{"token": "EETH", "amount": 1.0}],
        "tokens_in": [{"token": "PENDLE_LPT", "amount": 0.5}],
        "_classified_total_in_value_eur": 100.0,
        "_classified_total_out_value_eur": 100.0,
        "_classified_cp_protocols": ["dex"],
    }
    assert is_non_realizing_swap(tx) is True

    # Stable involved => must not trigger
    tx2 = dict(tx)
    tx2["tokens_in"] = [{"token": "USDC", "amount": 100.0}]
    assert is_non_realizing_swap(tx2) is False

    # Not neutral totals => must not trigger
    tx3 = dict(tx)
    tx3["_classified_total_in_value_eur"] = 100.0
    tx3["_classified_total_out_value_eur"] = 105.0
    assert is_non_realizing_swap(tx3) is False

