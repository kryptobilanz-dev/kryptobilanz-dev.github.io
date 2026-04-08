# taxtrack/tests/test_price_resolver.py

import time
from unittest.mock import patch

from taxtrack.prices.price_provider import get_price
from taxtrack.prices.price_resolver import resolve_prices_batch
from taxtrack.prices import PriceQuery


def test_batch_resolver_deduplicates():
    ts = int(time.time()) - 86400

    q1 = PriceQuery(symbol="ETH", ts=ts)
    q2 = PriceQuery(symbol="ETH", ts=ts)
    queries = [q1, q2]

    # Wichtig: resolve_prices_batch nutzt DIE IMPORTIERTE Version
    with patch("taxtrack.prices.price_resolver.get_price", return_value={"price": 2000.0}) as mock:
        res = resolve_prices_batch(queries)

    assert len(res) == 2
    assert mock.call_count == 1
    assert res[0]["price"] == 2000.0
