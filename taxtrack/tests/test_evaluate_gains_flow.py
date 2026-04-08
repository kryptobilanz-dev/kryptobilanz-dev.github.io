# taxtrack/tests/test_evaluate_gains_flow.py

import time
from datetime import datetime, timedelta

from taxtrack.rules.evaluate import evaluate_batch
from taxtrack.analyze.gains import compute_gains


def _dt(days_ago: int):
    """
    Hilfsfunktion:
    Erzeugt einen ISO-Timestamp X Tage vor jetzt.
    """
    dt = datetime.utcnow() - timedelta(days=days_ago)
    return dt.isoformat()


def test_buy_and_sell_flow():
    """
    Vollständiger Integrationstest für:
    Transaktion → evaluate_batch → compute_gains
    """

    # -----------------------------------------------
    # 1. Testtransaktionen simulieren
    # -----------------------------------------------
    txs = [
        {
            "tx_hash": "0xBUY",
            "dt_iso": _dt(10),    # gekauft vor 10 Tagen
            "token": "ETH",
            "amount": 1.0,
            "eur_value": 2000.00,
            "from": "0xexchange",
            "to": "0xmywallet",
            "method": "buy",
            "category": "",
            "direction": "in",
        },
        {
            "tx_hash": "0xSELL",
            "dt_iso": _dt(1),     # verkauft vor 1 Tag
            "token": "ETH",
            "amount": -1.0,
            "eur_value": 2500.00,
            "from": "0xmywallet",
            "to": "0xexchange",
            "method": "sell",
            "category": "",
            "direction": "out",
        },
    ]

    wallet = "0xmywallet"

    # -----------------------------------------------
    # 2. evaluate_batch ausführen
    # -----------------------------------------------
    classified_items, debug = evaluate_batch(txs, wallet)

    # Prüfen, ob 2 Items erzeugt wurden
    assert len(classified_items) == 2

    # Kategorien prüfen
    assert classified_items[0].category == "buy"
    assert classified_items[1].category == "sell"

    # -----------------------------------------------
    # 3. Nun compute_gains aufrufen
    # -----------------------------------------------
    gains, totals = compute_gains(classified_items)

    # Prüfen: genau 1 GainRow (Verkauf)
    assert len(gains) == 1

    g = gains[0]

    # -----------------------------------------------
    # 4. Erwartete Werte prüfen
    # -----------------------------------------------
    assert g.token == "ETH"
    assert g.amount_out == 1.0
    assert g.proceeds_eur == 2500.00
    assert g.cost_basis_eur == 2000.00
    assert g.pnl_eur == 500.00

    # Haltefrist ca. 9 Tage (10 Tage - 1 Tag)
    assert g.hold_days >= 8
    assert g.hold_days <= 15

    # Verkauft < 365 Tage → steuerpflichtig
    assert g.taxable is True

    # Totals prüfen
    assert totals["ETH"] == 500.00
