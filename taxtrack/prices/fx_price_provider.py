# taxtrack/prices/fx_price_provider.py
from __future__ import annotations
from datetime import datetime
from typing import Dict, Any


def fetch_fx_rate(base: str, quote: str, date: str) -> Dict[str, Any]:
    """
    Minimaler FX-Provider.
    Aktuell:
      - base == quote -> 1.0
      - sonst ValueError (bewusst, damit man es nicht vergisst)

    Später:
      - echte FX-Rates (ECB, Frankfurter API, Coingecko-Fiat, etc.)
    """
    base_ccy = base.upper()
    quote_ccy = quote.upper()

    if base_ccy == quote_ccy:
        rate = 1.0
    else:
        raise ValueError(
            f"FX-Rate für {base_ccy}->{quote_ccy} am {date} noch nicht implementiert."
        )

    return {
        "symbol": f"{base_ccy}/{quote_ccy}",
        "date": date,
        "price": rate,
        "quote": quote_ccy,
        "source": "fx",
        "granularity": "daily",
        "normalize_v2": True,
        "fetched_ts": int(datetime.utcnow().timestamp()),
        "meta": {
            "provider": "fx_stub",
        },
    }
