# taxtrack/tests/test_price_provider_integration.py

import time
import os
from unittest.mock import patch

from taxtrack.prices import get_eur_price
from taxtrack.prices.price_provider import _memory, _connect
from taxtrack.prices.provider_master import price_provider


def _clear_cache():
    # RAM leeren
    _memory.clear()

    # Disk Cache leeren
    with _connect() as c:
        c.execute("DELETE FROM eur_prices;")


def test_csv_priority():
    ts = int(time.time())

    _clear_cache()

    # HybridProvider mocken
    with patch.object(price_provider, "get_eur_price", return_value=1234.56):
        eur = get_eur_price("ETH", ts)

    assert eur == 1234.56


def test_binance_fallback():
    ts = int(time.time()) - 86400

    _clear_cache()

    with patch.object(price_provider, "get_eur_price", return_value=2500.99):
        eur = get_eur_price("ETH", ts)

    assert eur == 2500.99
