# taxtrack/export/export_summary.py

import csv
from pathlib import Path
from collections import defaultdict
from taxtrack.utils.debug_log import log

def summarize_csv(raw_csv: Path, audit_csv: Path, out_csv: Path):
    """
    Liest raw_*.csv und audit_*.csv ein und erstellt eine Token-basierte Zusammenfassung:
      Token | IN_Count | OUT_Count | Sum_EUR | Gewinn_EUR | Gebühren_EUR
    """
    log(f"[SUMMARY] Erstelle Auswertung aus {raw_csv.name} & {audit_csv.name}")

    data = defaultdict(lambda: {"in": 0, "out": 0, "sum_eur": 0.0, "gain_eur": 0.0, "fee_eur": 0.0})

    # Audit-Datei (tax-ready: gain_net_eur/fees_eur; legacy: gain_eur/fee_eur)
    if audit_csv.exists():
        with open(audit_csv, "r", encoding="utf-8") as f:
            first = f.readline()
            f.seek(0)
            delim = ";" if first.count(";") > first.count(",") else ","
            reader = csv.DictReader(f, delimiter=delim)
            for row in reader:
                if (row.get("tx_hash") or "").startswith("#"):
                    continue
                token = (row.get("token") or "UNKNOWN").strip()
                data[token]["out"] += 1
                g = row.get("gain_net_eur") or row.get("gain_eur") or 0
                fe = row.get("fees_eur") or row.get("fee_eur") or 0
                try:
                    data[token]["gain_eur"] += float(g)
                    data[token]["fee_eur"] += float(fe)
                except (TypeError, ValueError):
                    pass

    # Raw-Datei
    if raw_csv.exists():
        with open(raw_csv, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f, delimiter=";")
            for row in reader:
                token = (row.get("token") or "UNKNOWN").strip()
                val = float(row.get("eur_value") or 0)
                fee = float(row.get("fee_eur") or 0)
                direction = (row.get("direction") or "").lower()
                data[token]["sum_eur"] += val
                data[token]["fee_eur"] += fee
                if direction == "in":
                    data[token]["in"] += 1
                elif direction == "out":
                    data[token]["out"] += 1

    # Ausgabe
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["token", "IN_Count", "OUT_Count", "Sum_EUR", "Gewinn_EUR", "Gebühren_EUR"])
        for token, stats in sorted(data.items()):
            w.writerow([
                token,
                stats["in"],
                stats["out"],
                f"{stats['sum_eur']:.2f}",
                f"{stats['gain_eur']:.2f}",
                f"{stats['fee_eur']:.2f}"
            ])

    log(f"[SUMMARY] Fertig! Ergebnisse unter {out_csv}")

def main():
    base = Path("data/out")
    raw = base / "raw_ethereum_2025.csv"
    audit = base / "audit_ethereum_2025.csv"
    out = base / "summary_ethereum.csv"

    if not raw or not audit:
        log("[SUMMARY] Keine CSV-Dateien gefunden.")
        return

    summarize_csv(raw, audit, out)

if __name__ == "__main__":
    main()
