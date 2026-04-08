import requests
import csv
import time
from datetime import datetime, timedelta
from pathlib import Path

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------

TOKENS = [
    "BTC", "ETH", "ETH2", "SOL", "DOT", "ADA", "ATOM", "AKT", "AMP",
    "FET", "GRT", "NEAR", "DOGE", "USDT", "USDC", "XLM", "XTZ", "VARA", "RONIN"
]

# Mappings zu CoinGecko IDs (Market Chart API)
TOKEN_TO_COINGECKO = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "ETH2": "ethereum",

    "SOL": "solana",
    "DOT": "polkadot",
    "ADA": "cardano",

    "ATOM": "cosmos",
    "AKT": "akash-network",

    "DOGE": "dogecoin",
    "FET": "fetch-ai",
    "GRT": "the-graph",

    "NEAR": "near",
    "XLM": "stellar",
    "XTZ": "tezos",

    # Stablecoins
    "USDT": "tether",
    "USDC": "usd-coin",

    # Eventuell nicht vorhanden
    "AMP": "amp-token",
    "VARA": None,
    "RONIN": None,
}

OUT_DIR = Path("data/prices")
OUT_DIR.mkdir(parents=True, exist_ok=True)

START = datetime(2024, 1, 1)
END = datetime(2024, 12, 31)

# ---------------------------------------------------------
# HELPER
# ---------------------------------------------------------

def load_market_chart(coin_id: str):
    """
    Holt alle historischen Preise eines Tokens mit market_chart API.
    Gibt Liste von (timestamp, eur_price) zurück.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        "vs_currency": "eur",
        "days": "max"
    }

    print(f"[INFO] Lade market_chart für {coin_id} …")
    r = requests.get(url, params=params, timeout=20)

    if r.status_code != 200:
        print(f"[ERROR] CoinGecko API Fehler {r.status_code} für {coin_id}")
        return []

    data = r.json()
    prices = data.get("prices", [])  # Format: [[timestamp_ms, price], ...]
    result = []

    for ts_ms, price in prices:
        ts = ts_ms // 1000
        result.append((ts, float(price)))

    return result


def extract_daily(prices_all: list[tuple[int, float]]):
    """
    Wandelt die Market-Chart-Preise in Tagespreise um:
    Wir nutzen jeweils den letzten Preis eines Tages.
    """
    if not prices_all:
        return {}

    daily = {}
    for ts, price in prices_all:
        day = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
        daily[day] = price  # letzter Preis des Tages gewinnt

    return daily


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    print("[INFO] Starte Market-Chart Preis-Download für 2024…")

    for token in TOKENS:
        print(f"\n======================================")
        print(f"[INFO] Token: {token}")

        cg_id = TOKEN_TO_COINGECKO.get(token)
        out_path = OUT_DIR / f"{token.lower()}_eur_daily.csv"

        if not cg_id:
            print(f"[WARN] Kein CoinGecko-ID für {token} → leere Datei erzeugt.")
            out_path.write_text("date,eur\n")
            continue

        prices_all = load_market_chart(cg_id)
        daily_prices = extract_daily(prices_all)

        rows = [("date", "eur")]

        date = START
        while date <= END:
            key = date.strftime("%Y-%m-%d")
            eur = daily_prices.get(key)

            if eur is None:
                print(f"[MISS] {token} @ {key} → kein Preis")
            else:
                rows.append((key, eur))

            date += timedelta(days=1)

        # Datei schreiben
        with out_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerows(rows)

        print(f"[DONE] {token}: {out_path}")

        # API Limit respektieren
        time.sleep(1.1)

    print("\n[INFO] Preis-Download abgeschlossen.")


if __name__ == "__main__":
    main()
