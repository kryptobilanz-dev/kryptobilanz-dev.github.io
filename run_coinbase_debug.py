from pathlib import Path
from taxtrack.loaders.coinbase.loader import load_coinbase

# <- Pfad anpassen: nimm eine echte Coinbase CSV
p = Path("data/inbox/coinbase/coinbase/coinbase.csv")

rows = load_coinbase(p)
print("Loaded rows:", len(rows))
