# taxtrack/prices/provider_csv.py
# ✔ geprüft: kompatibel mit neuer Struktur, keine Importfixes nötig

from pathlib import Path
import csv
from datetime import datetime
from taxtrack.utils.path import PRICES_DIR

class CSVPriceProvider:
    """
    Liest historische Preise (EUR) aus CSV-Dateien in data/prices/.
    Format: date,eur

    Verbesserungen:
    - Caching pro Token
    - explizite Sortierung der Zeitreihen
    """

    def __init__(self, root=None):
        # root in Path konvertieren (egal ob str oder None)
        if root:
            self.root = Path(root).resolve()
        else:
            # Default = taxtrack/data/prices
            self.root = (Path(__file__).resolve().parents[2] / "data" / "prices")

        # Cache für bereits geladene Token
        self._cache: dict[str, list[dict]] = {}

    def _load_csv(self, token: str) -> list[dict]:
        # Cache nutzen
        token_id = token.lower()
        if token_id in self._cache:
            return self._cache[token_id]

        path = self.root / f"{token_id}_eur_daily.csv"
        print(f"[DEBUG CSV] Versuche Datei zu laden: {path}")

        if not path.exists():
            print(f"[WARN CSV] Datei nicht gefunden: {path}")
            self._cache[token_id] = []
            return []

        rows = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                first_line = f.readline().strip()
                print(f"[DEBUG CSV] Header-Zeile: {first_line}")
                f.seek(0)

                reader = csv.DictReader(f)
                for row in reader:
                    date_str = row.get("date") or row.get("Date") or ""
                    eur_str  = row.get("eur")  or row.get("EUR")  or row.get("price") or "0"

                    try:
                        price = float(eur_str.replace(",", "."))
                        if "-" in date_str:
                            ts = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp())
                        elif "." in date_str:
                            ts = int(datetime.strptime(date_str, "%d.%m.%Y").timestamp())
                        else:
                            ts = int(datetime.strptime(date_str.strip(), "%Y%m%d").timestamp())

                        rows.append({"ts": ts, "price": price})
                    except Exception as e:
                        print(f"[WARN CSV] Zeile übersprungen: {row} ({e})")
                        continue

            # Zeitreihe sortieren (aufsteigend)
            rows = sorted(rows, key=lambda x: x["ts"])
            print(f"[DEBUG CSV] {len(rows)} Zeilen geladen aus {path.name}")

        except Exception as e:
            print(f"[ERROR CSV] Fehler beim Lesen {path}: {e}")
            rows = []

        self._cache[token_id] = rows
        return rows

    def get_eur_price(self, token: str, timestamp: int | None = None) -> float | None:
        """Gibt den EUR-Preis für das Datum zurück oder None (missing)."""
        data = self._load_csv(token)
        if not data:
            return None

        # Kein Timestamp → letzter bekannter Preis
        if not timestamp:
            return data[-1]["price"]

        # Finde den Eintrag mit dem nächsten kleineren oder gleichen Datum
        # (klassisch: "zu diesem Tag")
        candidates = [entry for entry in data if entry["ts"] <= timestamp]
        if candidates:
            return candidates[-1]["price"]

        # Falls nichts <= timestamp existiert → nutze ältesten Preis (frühester Eintrag)
        return data[0]["price"]
