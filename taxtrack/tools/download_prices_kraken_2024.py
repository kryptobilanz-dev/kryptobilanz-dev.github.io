import csv
import time
import requests
from pathlib import Path
from datetime import datetime

TOKENS = [
    "BTC","ETH","SOL","DOT","ADA","ATOM","DOGE","XLM","XTZ","USDT"
]

KRAKEN_PAIR = {
    "BTC": "XBTEUR",
    "ETH": "ETHEUR",
    "ADA": "ADAEUR",
    "DOT": "DOTEUR",
    "SOL": "SOLEUR",
    "ATOM":"ATOMEUR",
    "DOGE":"DOGEEUR",
    "XLM":"XLMEUR",
    "XTZ":"XTZEUR",
    "USDT":"USDTEUR",
}

OUT = Path("data/prices"); OUT.mkdir(parents=True, exist_ok=True)

def load_kraken(pair):
    url = "https://api.kraken.com/0/public/OHLC"
    params = {"pair": pair, "interval": 1440}
    r = requests.get(url, params=params, timeout=20)
    if r.status_code != 200:
        print("[ERROR] Kraken:", r.text[:120])
        return []
    res = r.json().get("result", {})
    key = list(res.keys())[0]
    return res[key]

def main():
    for token in TOKENS:
        pair = KRAKEN_PAIR.get(token)
        out = OUT / f"{token.lower()}_eur_daily.csv"

        if not pair:
            out.write_text("date,eur\n")
            print("[MISS] Kein Kraken Paar:", token)
            continue

        print("[INFO] Kraken:", token, pair)
        data = load_kraken(pair)

        rows = [("date","eur")]
        for d in data:
            ts = int(d[0])
            close = float(d[4])
            date = datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d")
            rows.append((date, close))

        with out.open("w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerows(rows)

        print("[DONE] →", out)
        time.sleep(1)

if __name__ == "__main__":
    main()
