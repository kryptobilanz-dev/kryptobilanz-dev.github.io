# taxtrack/tests/test_token_mapper.py

from taxtrack.prices.token_mapper import map_token, normalize


def test_normalize():
    assert normalize(" eth ") == "ETH"


def test_alias_mapping_eth():
    assert map_token("WETH") == "ETH"
    assert map_token("stETH") == "ETH"
    assert map_token("rsweth") == "ETH"
    assert map_token("EZETH") == "ETH"


def test_stablecoins():
    assert map_token("USDT") == "USD"
    assert map_token("USDC") == "USD"


def test_stablecoin_bridge_suffixes():
    assert map_token("USDC.E") == "USD"
    assert map_token("usdc.e.e") == "USD"
    assert map_token("USDT.E") == "USD"
    assert map_token("PYUSD") == "USD"
    assert map_token("BUSD") == "USD"


def test_wrapped_native():
    assert map_token("WBTC") == "BTC"
    assert map_token("WETHE") == "ETH"
    assert map_token("WPOL") == "POL"
    assert map_token("WAVAX.E") == "AVAX"
    assert map_token("WBNB.E") == "BNB"


def test_unmapped_passthrough():
    assert map_token("CRV") == "CRV"
    assert map_token("WELL.E") == "WELL.E"


def test_cosmos():
    assert map_token("STATOM") == "ATOM"
