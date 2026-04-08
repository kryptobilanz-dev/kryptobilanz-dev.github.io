import csv

def fix_coinbase_csv(infile, outfile):
    """
    Repariert Coinbase-CSV-Dateien, die durch Excel/UTF16 zerstört wurden.
    Erkennt 1-Spalten-CSV und wandelt sie in echte CSV um.
    """
    # 1) Datei im Bytes-Modus lesen (Excel/UTF-16 sicher handhabbar)
    raw_lines = open(infile, "rb").read().splitlines()

    # 2) Jede Zeile decodieren (UTF-16 oder UTF-8 auto)
    decoded = []
    for line in raw_lines:
        try:
            decoded.append(line.decode("utf-16"))
        except:
            decoded.append(line.decode("utf-8", errors="ignore"))

    # 3) Zeilen splitten
    split_rows = [l.strip() for l in decoded if l.strip()]

    # Coinbase-Datei hat oft 3 Header-Zeilen, wir filtern die echte
    # Wir suchen die, die "Transaction Type" enthält
    header_line = None
    for l in split_rows:
        if "Transaction Type" in l:
            header_line = l
            break

    if header_line is None:
        raise ValueError("Header 'Transaction Type' nicht gefunden. Datei ist nicht Coinbase-kompatibel.")

    header = header_line.split(",")

    # 4) Datenteile extrahieren (alles unterhalb der Header-Zeile)
    start_index = split_rows.index(header_line) + 1
    data_lines = split_rows[start_index:]

    processed_rows = []
    for l in data_lines:
        cols = l.split(",")
        if len(cols) < len(header):
            # Auffüllen, wenn Zeilen zu kurz
            cols = cols + [""] * (len(header) - len(cols))
        processed_rows.append(cols[:len(header)])

    # 5) Schreibe reparierte CSV als UTF-8
    with open(outfile, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(processed_rows)

    print(f"[OK] Reparierte CSV gespeichert unter: {outfile}")


if __name__ == "__main__":
    fix_coinbase_csv(
        infile="data/inbox/coinbase_rewards_2024.csv",
        outfile="data/inbox/coinbase_rewards_2024_FIXED.csv"
    )
