# taxtrack/analyze/fee_validator.py
import csv
from pathlib import Path

def validate_fees(csv_path: Path):
    """
    Prüft, ob in der Audit-CSV Withdrawals ohne Fees vorhanden sind.
    Gibt eine kleine Zusammenfassung zurück.
    """
    withdraws_total = 0
    withdraws_no_fee = 0
    total_fee_count = 0
    total_fee_sum = 0.0

    with csv_path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fee = float(row.get("fee_amount") or 0)
            total_fee_sum += fee
            if abs(fee) > 0:
                total_fee_count += 1

            if (row.get("category") or "").lower() == "withdraw":
                withdraws_total += 1
                if fee == 0:
                    withdraws_no_fee += 1

    print(f"\n[AUDIT·FEE] {withdraws_total} Withdrawals erkannt.")
    print(f"[AUDIT·FEE] → {withdraws_no_fee} davon ohne Gebührseintrag.")
    print(f"[AUDIT·FEE] {total_fee_count} Transaktionen mit erfassten Gebühren (Σ {total_fee_sum:.2f} €).")

    if withdraws_no_fee > 0:
        print(f"[INFO] Coinbase liefert für {withdraws_no_fee} Withdrawals keine Fees – kein Fehler, sondern Coinbase-Standard.")

    return {
        "withdraws_total": withdraws_total,
        "withdraws_no_fee": withdraws_no_fee,
        "total_fee_count": total_fee_count,
        "total_fee_sum": round(total_fee_sum, 2),
    }
