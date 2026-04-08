import csv
import time
from datetime import datetime, timedelta
from pathlib import Path
import requests

TOKENS = [
    "BTC","ETH","SOL","DOT","ADA","ATOM","AKT","AMP",
    "FET","GRT","NEAR","DOGE","USDT","USDC","XLM","XTZ","VARA","RONIN"
]

# Yahoo Finance Symbols (EUR)
YF_SYMBOL = {
    "BTC": "BTC-EUR",
    "ETH": "ETH-EUR",
    "SOL": "SOL-EUR",
    "DOT": "DOT-EUR",
    "ADA": "ADA-EUR",
    "ATOM": "ATOM-EUR",
    "AKT":  "AKT-EUR",
    "DOGE": "DOGE-EUR",
    "FET":  "FET-EUR",
    "GRT":  "GRT-EUR",
    "XLM":  "XLM-EUR",
    "XTZ":  "XTZ-EUR",
    "NEAR": "NEAR-EUR",

    # Stablecoins → über USD
    "USDT": "USDT-USD",
    "USDC": "USDC-USD",

    # Exoten fehlen
    "AMP": None,
    "VARA": None,
    "RONIN": None,
}

OUT = Path("data/prices"); OUT.mkdir(parents=True, exist_ok=True)

START = int(datetime(2024,1,1).timestamp())
END   = int(datetime(2024,12,31).timestamp())

def download_yahoo(symbol):
    url = (
        f"https://query1.finance.yahoo.com/v7/finance/download/{symbol}"
        f"?period1={START}&period2={END}&interval=1d&events=history"
    )
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        print("[ERROR] Yahoo:", symbol, r.text[:120])
        return None
    return r.text

def main():
    for token in TOKENS:
        yf = YF_SYMBOL.get(token)
        out = OUT / f"{token.lower()}_eur_daily.csv"

        if yf is None:
            print(f"[MISS] Kein Yahoo Symbol für {token}")
            out.write_text("date,eur\n")
            continue

        print("[INFO] Lade:", token, yf)
        text = download_yahoo(yf)
        if text is None:
            out.write_text("date,eur\n")
            continue

        rows = [("date", "eur")]
        lines = text.strip().split("\n")[1:]  # skip header

        for line in lines:
            parts = line.split(",")
            if len(parts) < 5: continue
            date = parts[0]
            close = parts[4]
            if close == "null": continue
            rows.append((date, close))

        with out.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)

        print("[DONE] →", out)
        time.sleep(1)

if __name__ == "__main__":
    main()
