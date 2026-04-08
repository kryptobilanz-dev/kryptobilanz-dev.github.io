# taxtrack/prices/coingecko_price_provider.py
import requests
import datetime
import time
import json
import csv

from pathlib import Path

class CoinGeckoPriceProvider:
    """Holt historische Preise (EUR) über die CoinGecko API (ohne API-Key), mit lokalem JSON-Cache + CSV-Fallback."""

    def __init__(self):
        self.base_url = "https://api.coingecko.com/api/v3"
        self.cache_path = Path(__file__).resolve().parents[2] / "data" / "prices_cache.json"
        self.csv_dir = Path(__file__).resolve().parents[2] / "data" / "prices"
        self._cache = self._load_cache()

    # ---------------------------
    # Öffentliche Hauptfunktion
    # ---------------------------
    def get_price(self, token_symbol: str, unix_ts: int) -> float:
        """
        Gibt den historischen EUR-Preis eines Tokens zum angegebenen Unix-Timestamp zurück.
        Prüft zuerst lokale CSV-Datei, dann JSON-Cache, dann CoinGecko.
        """
        token_symbol = (token_symbol or "").lower()
        if not unix_ts:
            return 0.0

        # Datum für CSV und Cache
        date_str = datetime.datetime.utcfromtimestamp(int(unix_ts)).strftime("%Y-%m-%d")
        cache_key = f"{token_symbol}:{date_str}"

        # 1️⃣ Versuch: Lokale CSV-Datei
        local_price = self._try_local_csv(token_symbol, date_str)
        if local_price:
            return local_price

        # 2️⃣ Versuch: Cache
        if cache_key in self._cache:
            return self._cache[cache_key]

        # 3️⃣ Versuch: CoinGecko-API
        coingecko_id = self._map_token_to_id(token_symbol)
        if not coingecko_id:
            self._cache[cache_key] = 0.0
            self._save_cache()
            return 0.0

        price = self._fetch_price(coingecko_id, unix_ts)
        self._cache[cache_key] = price
        self._save_cache()
        return price

    # ---------------------------
    # Interne CSV-Fallback
    # ---------------------------
    def _try_local_csv(self, token_symbol: str, date_str: str) -> float:
        """Lädt Preis aus data/prices/<token>_eur_daily.csv, falls vorhanden."""
        csv_path = self.csv_dir / f"{token_symbol.lower()}_eur_daily.csv"
        if not csv_path.exists():
            return 0.0

        try:
            with open(csv_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row["date"] == date_str:
                        return float(row["eur"])
        except Exception:
            pass
        return 0.0

    # ---------------------------
    # CoinGecko-API
    # ---------------------------
    def _fetch_price(self, coingecko_id: str, unix_ts: int) -> float:
        dt = datetime.datetime.utcfromtimestamp(int(unix_ts))
        date_str = dt.strftime("%d-%m-%Y")
        url = f"{self.base_url}/coins/{coingecko_id}/history?date={date_str}&localization=false"

        for _ in range(3):
            try:
                r = requests.get(url, timeout=12)
                if r.status_code != 200:
                    time.sleep(1.0)
                    continue
                data = r.json()
                return float(data.get("market_data", {}).get("current_price", {}).get("eur", 0.0))
            except Exception:
                time.sleep(1.0)
        return 0.0

    # ---------------------------
    # Token-Mapping
    # ---------------------------
    def _map_token_to_id(self, symbol: str) -> str:
        mapping = {
            "eth": "ethereum",
            "weth": "weth",
            "steth": "staked-ether",
            "sweth": "sweth",
            "rsweth": "rsweth",
            "weeth": "weeth",
            "ezeth": "ezeth",
            "rseth": "rseth",
            "pendle": "pendle",
            "stars": "stargaze",
        }
        if "/" in symbol or "-" in symbol:
            return "ethereum"
        return mapping.get(symbol.lower(), symbol.lower())

    # ---------------------------
    # Cache
    # ---------------------------
    def _load_cache(self) -> dict:
        try:
            if self.cache_path.exists():
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception:
            pass
        return {}

    def _save_cache(self):
        try:
            self.cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.cache_path, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # Kompatibilität
    def get_eur_price(self, token_symbol: str, ts: int | None = None) -> float:
        return self.get_price(token_symbol, ts)
