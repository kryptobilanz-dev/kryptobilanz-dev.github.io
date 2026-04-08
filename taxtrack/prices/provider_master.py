# taxtrack/prices/provider_master.py
# ZenTaxCore Hybrid Price Engine v3.0
# -----------------------------------
# Priorität:
# 1. local CSV (eur_daily)
# 2. Yahoo Finance live
# 3. Binance OHLC live
# 4. Kraken OHLC live
# 5. Stablecoin fallback
# 6. Restaking-ETH fallback
# 7. Warnung → 0.0

from __future__ import annotations
import requests
from datetime import datetime
from taxtrack.prices.token_mapper import map_token
from taxtrack.prices.provider_csv import CSVPriceProvider
from taxtrack.utils.debug_log import log
from taxtrack.utils.path import PRICES_DIR


class HybridPriceProvider:

    # ---------------------------------------------------------
    # Init
    # ---------------------------------------------------------

    def __init__(self, price_dir=None):
        self.price_dir = price_dir or PRICES_DIR
        self.csv = CSVPriceProvider(self.price_dir)

        log(f"[PRICE] HybridPriceProvider aktiv (root={self.price_dir})")

    # ---------------------------------------------------------
    # Helper: Yahoo Finance (EUR)
    # ---------------------------------------------------------
    def _fetch_yahoo(self, token: str, dt_iso: str):
        symbol_map = {
            "BTC": "BTC-EUR", "ETH": "ETH-EUR", "SOL": "SOL-EUR",
            "DOT": "DOT-EUR", "ADA": "ADA-EUR", "ATOM": "ATOM-EUR",
            "AKT": "AKT-EUR", "DOGE": "DOGE-EUR", "FET": "FET-EUR",
            "GRT": "GRT-EUR", "XLM": "XLM-EUR", "XTZ": "XTZ-EUR",
            "NEAR": "NEAR-EUR",
        }
        if token not in symbol_map:
            return None

        yf = symbol_map[token]
        d = datetime.fromisoformat(dt_iso).strftime("%Y-%m-%d")

        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/{yf}"
            f"?interval=1d&period1={int(datetime.fromisoformat(d).timestamp())}"
            f"&period2={int(datetime.fromisoformat(d).timestamp())+86400}"
        )

        try:
            r = requests.get(url, timeout=8)
            js = r.json()
            res = js["chart"]["result"][0]
            close = res["indicators"]["quote"][0]["close"][0]
            if close is not None:
                log(f"[PRICE·YF] {token}@{d} = {close:.4f} €")
                return float(close)
        except Exception:
            pass

        return None

    # ---------------------------------------------------------
    # Helper: Binance (EUR / USDT)
    # ---------------------------------------------------------
    def _fetch_binance(self, token: str, dt_iso: str):
        symbols = {
            "BTC": "BTCEUR", "ETH": "ETHEUR", "SOL": "SOLEUR",
            "DOT": "DOTEUR", "ADA": "ADAEUR",
            "ATOM": "ATOMEUR", "DOGE": "DOGEEUR",
            "XLM": "XLMEUR",  "XTZ": "XTZEUR",
            "NEAR": "NEAREUR",
            # Exoten (USDT-Paare)
            "AKT": "AKTUSDT", "AMP": "AMPUSDT", "FET": "FETUSDT",
            "GRT": "GRTUSDT", "VARA": "VARAUSDT", "RONIN": "RONINUSDT",
        }

        if token not in symbols:
            return None

        symbol = symbols[token]
        d = datetime.fromisoformat(dt_iso)
        start = int(d.timestamp() * 1000)
        end = start + 86400 * 1000

        url = "https://api.binance.com/api/v3/klines"
        params = {
            "symbol": symbol,
            "interval": "1d",
            "startTime": start,
            "endTime": end
        }

        try:
            r = requests.get(url, params=params, timeout=8)
            data = r.json()
            if not data:
                return None

            # Close Price
            close = float(data[0][4])

            # Falls USDT-Paar → zu EUR umrechnen über Yahoo
            if symbol.endswith("USDT"):
                fx = self._fetch_yahoo_fx(d)
                close *= fx

            log(f"[PRICE·BIN] {token}@{d.date()} = {close:.4f} €")
            return close

        except Exception:
            return None

    # ---------------------------------------------------------
    # Helper: Yahoo USD/EUR FX
    # ---------------------------------------------------------
    def _fetch_yahoo_fx(self, date_obj):
        url = (
            f"https://query1.finance.yahoo.com/v8/finance/chart/EURUSD=X"
            f"?interval=1d&period1={int(date_obj.timestamp())}"
            f"&period2={int(date_obj.timestamp())+86400}"
        )
        try:
            r = requests.get(url, timeout=8)
            js = r.json()
            close = js["chart"]["result"][0]["indicators"]["quote"][0]["close"][0]
            if close:
                return 1 / float(close)
        except:
            pass
        return 1.0

    # ---------------------------------------------------------
    # Helper: Kraken EUR-Paare
    # ---------------------------------------------------------
    def _fetch_kraken(self, token: str, dt_iso: str):
        pairs = {
            "BTC": "XBT/EUR",
            "ETH": "ETH/EUR",
            "ADA": "ADA/EUR",
            "DOT": "DOT/EUR",
            "SOL": "SOL/EUR",
            "ATOM":"ATOM/EUR",
            "DOGE":"DOGE/EUR",
            "XLM":"XLM/EUR",
            "XTZ":"XTZ/EUR",
        }
        if token not in pairs:
            return None

        url = f"https://api.kraken.com/0/public/OHLC?pair={pairs[token]}&interval=1440"
        try:
            r = requests.get(url, timeout=8)
            js = r.json().get("result", {})
            first_key = list(js.keys())[0]
            for row in js[first_key]:
                ts = int(row[0])
                date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
                if date == dt_iso[:10]:
                    close = float(row[4])
                    log(f"[PRICE·KRK] {token}@{date} = {close:.4f} €")
                    return close
        except:
            pass
        return None

    # ---------------------------------------------------------
    # Hauptfunktion
    # ---------------------------------------------------------
    def get_eur_price(self, token: str, ts: int) -> float | None:
        if not token:
            return None

        base = map_token(token)
        base = (base or "").strip().upper()
        dt_iso = datetime.utcfromtimestamp(ts).isoformat()

        # Base currency anchor:
        # USD is not a market price; it is a normalization anchor for the internal EUR-equivalent system.
        # Must bypass all providers and caching.
        if base == "USD":
            log("[PRICE BASE] USD base currency applied (1.0)")
            return 1.0

        # 1) CSV
        csv_price = self.csv.get_eur_price(base.lower(), ts)
        if csv_price is not None and csv_price > 0:
            return csv_price

        # 2) Yahoo
        p = self._fetch_yahoo(base, dt_iso)
        if p:
            return p

        # 3) Binance
        p = self._fetch_binance(base, dt_iso)
        if p:
            return p

        # 4) Kraken
        p = self._fetch_kraken(base, dt_iso)
        if p:
            return p

        # 5) Stablecoins
        if base in ["USDT", "USDC", "DAI", "TUSD", "USDE"]:
            fx = self._fetch_yahoo_fx(datetime.fromisoformat(dt_iso))
            return fx

        # 6) Restaking → ETH
        if base == "ETH":
            eth_p = self.csv.get_eur_price("eth", ts)
            if eth_p is not None and eth_p > 0:
                return eth_p

        # 7) Fallback
        log(f"[PRICE·MISS] Kein Preis für {token} @ {dt_iso}")
        return None


# Globale Instanz
price_provider = HybridPriceProvider()
