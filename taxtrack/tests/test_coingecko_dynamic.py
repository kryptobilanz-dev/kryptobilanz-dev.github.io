# tests for CoinGecko dynamic symbol resolution (mocked HTTP)

from unittest.mock import patch

import pytest

import taxtrack.prices.coingecko_price_provider as cg


def test_blocked_symbols_no_search():
    """UNKNOWN / LP-like symbols must not hit search."""
    with patch.object(cg, "_search_coingecko") as s:
        assert cg.resolve_coingecko_id("UNKNOWN") is None
        assert cg.resolve_coingecko_id("PENDLE_LPT") is None
        assert cg.resolve_coingecko_id("MOOXYZ") is None
        s.assert_not_called()


def test_dynamic_search_exact_symbol(mock_search_coins):
    cg.clear_dynamic_resolution_log()
    cg._SEARCH_MEMO.clear()
    cg._NEGATIVE_ID_CACHE.clear()
    sym = "TSTDYN123"
    assert cg._get_coingecko_id_static(sym) is None
    with patch.object(cg, "_search_coingecko", return_value=mock_search_coins):
        with patch.object(cg, "_persist_new_mapping"):
            cid = cg.resolve_coingecko_id(sym)
    assert cid == "test-dynamic-coin"
    assert any(sym in e for e in cg.get_dynamic_resolutions_session())


@pytest.fixture
def mock_search_coins():
    return [
        {
            "id": "test-dynamic-coin",
            "name": "Test Dynamic",
            "symbol": "TSTDYN123",
            "market_cap_rank": 150,
        }
    ]


def test_builtin_not_overridden():
    assert cg.resolve_coingecko_id("ETH") == "ethereum"
    assert cg.resolve_coingecko_id("WETH") == "ethereum"
