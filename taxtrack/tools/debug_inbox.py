# taxtrack/tools/debug_inbox.py
from __future__ import annotations

import argparse
from pathlib import Path

from taxtrack.loaders.auto_detect import detect_loader, load_auto
from taxtrack.utils.debug_log import log


def parse_args():
    p = argparse.ArgumentParser(prog="taxtrack-debug-inbox")
    p.add_argument("--wallet", required=True, help="Wallet-Adresse (wie im Legacy-Runner / CLI)")
    return p.parse_args()


def debug_inbox(wallet: str):
    # Inbox relativ zum taxtrack-Paket
    inbox_dir = Path(__file__).resolve().parents[1] / "data" / "inbox"
    print(f"[DEBUG] Inbox-Ordner: {inbox_dir}")

    if not inbox_dir.exists():
        print("[DEBUG] Inbox existiert nicht.")
        return

    csv_files = sorted(inbox_dir.glob("*.csv"))
    print(f"[DEBUG] {len(csv_files)} CSV-Datei(en) gefunden.\n")

    if not csv_files:
        return

    for f in csv_files:
        print("=" * 60)
        print(f"[DEBUG] Datei: {f.name}")
        raw = f.read_bytes()
        print(f"[DEBUG] Byte-Prefix: {raw[:40]!r}")

        # Encoding grob testen
        for enc in ("utf-8", "utf-8-sig", "utf-16", "cp1252", "latin1"):
            try:
                txt = raw.decode(enc)
                first_line = txt.splitlines()[0] if txt.splitlines() else ""
                print(f"[DEBUG] Encoding-Kandidat: {enc} → erste Zeile: {first_line!r}")
                break
            except Exception:
                continue

        # detect_loader prüfen
        try:
            loader_name = detect_loader(f)
        except Exception as e:
            loader_name = f"<ERROR detect_loader: {e}>"

        print(f"[DEBUG] detect_loader → {loader_name}")

        # load_auto testen
        try:
            rows = load_auto(f, wallet)
            n_rows = len(rows)
            print(f"[DEBUG] load_auto() → {n_rows} Zeile(n)")
            if n_rows > 0:
                sample = rows[0]
                if hasattr(sample, "to_dict"):
                    sd = sample.to_dict()
                else:
                    # falls schon dict
                    try:
                        sd = dict(sample)
                    except Exception:
                        sd = {"_repr": repr(sample)}
                print(f"[DEBUG] Beispiel-Zeile Keys: {list(sd.keys())}")
                print(f"[DEBUG] Beispiel-Zeile: {sd}")
        except Exception as e:
            print(f"[DEBUG] FEHLER bei load_auto: {e}")

        print()  # Leerzeile zwischen Dateien


def main():
    args = parse_args()
    debug_inbox(args.wallet)


if __name__ == "__main__":
    main()
