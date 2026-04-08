import csv
import time
import requests
from pathlib import Path
from datetime import datetime

TOKENS = [
    "BTC","ETH","SOL","DOT","ADA","ATOM","AKT","AMP",
    "FET","GRT","NEAR","DOGE","USDT","USDC","XLM","XTZ","VARA","RONIN"
]

BINANCE_SYMBOL = {
    "BTC": "BTCEUR",
    "ETH": "ETHEUR",
    "SOL": "SOLEUR",
    "DOT": "DOTEUR",
    "ADA": "ADAEUR",
    "ATOM":"ATOMEUR",
    "DOGE":"DOGEEUR",
    "XLM":"XLMEUR",
    "XTZ":"XTZEUR",
    "NEAR":"NEAREUR",

    # USDT Paare (um danach zu EUR umzurechnen)
    "AKT":"AKTUSDT",
    "AMP":"AMPUSDT",
    "FET":"FETUSDT",
    "GRT":"GRTUSDT",
    "VARA":"VARAUSDT",
    "RONIN":"RONINUSDT",
    "USDT":"USDTUSD",
    "USDC":"USDCUSD"
}

OUT = Path("data/prices"); OUT.mkdir(parents=True, exist_ok=True)

def binance_klines(symbol, start, end):
    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": "1d",
        "startTime": int(start.timestamp()*1000),
        "endTime": int(end.timestamp()*1000)
    }
    r = requests.get(url, params=params, timeout=20)
    if r.status_code != 200:
        print("[ERROR] Binance:", symbol, r.text[:120])
        return []
    return r.json()

def get_usd_eur(date):
    """Yahoo USDEUR Tageskurs laden."""
    ts = int(date.timestamp())
    url = (
        f"https://query1.finance.yahoo.com/v7/finance/download/EURUSD=X?"
        f"period1={ts}&period2={ts+86400}&interval=1d&events=history"
    )
    r = requests.get(url, timeout=15)
    if r.status_code != 200:
        return 1.0
    rows = r.text.strip().split("\n")
    if len(rows) < 2: return 1.0
    close = rows[1].split(",")[4]
    try:
        return 1/float(close)
    except:
        return 1.0

def main():
    start = datetime(2024,1,1)
    end   = datetime(2024,12,31)

    for token in TOKENS:
        symbol = BINANCE_SYMBOL.get(token)
        out = OUT / f"{token.lower()}_eur_daily.csv"

        if not symbol:
            out.write_text("date,eur\n")
            print("[MISS] Kein Binance Symbol:", token)
            continue

        print("[INFO] Lade Binance:", token, symbol)
        data = binance_klines(symbol, start, end)
        rows = [("date","eur")]

        for d in data:
            ts = d[0]//1000
            date = datetime.utcfromtimestamp(ts)
            close = float(d[4])

            # Falls Paar ein USDT-pair ist → in EUR umrechnen
            if symbol.endswith("USDT") or symbol.endswith("USD"):
                eur_rate = get_usd_eur(date)
                close = close * eur_rate

            rows.append((date.strftime("%Y-%m-%d"), close))

        with out.open("w", encoding="utf-8", newline="") as f:
            csv.writer(f).writerows(rows)

        print("[DONE] →", out)
        time.sleep(1)

if __name__ == "__main__":
    main()
