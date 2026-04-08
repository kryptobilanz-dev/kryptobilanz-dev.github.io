from datetime import datetime, timedelta

from taxtrack.analyze.vault_exit_resolver import apply_vault_exits


def _iso_days_ago(days_ago: int) -> str:
    dt = datetime.utcnow() - timedelta(days=days_ago)
    return dt.isoformat()


def test_simple_vault_mint_and_exit():
    """
    Minimaler Szenario-Test für apply_vault_exits:

    - 1 Vault-Mint (direction='in', eur_value > 0)
    - 1 Vault-Exit (Vault-Token out + Base-Asset in)

    Erwartung:
    - cost_basis_eur == Mint-eur_value
    - pnl_eur = proceeds_eur - cost_basis_eur
    - hold_days korrekt (Differenz der Tage zwischen Mint und Exit)
    """

    wallet = "0xmywallet"
    chain_id = "eth"

    # Mint: wir erhalten 1.0 MOO-Token im Wert von 1000 EUR
    dt_mint = _iso_days_ago(10)
    mint_row = {
        "tx_hash": "0xMINT",
        "dt_iso": dt_mint,
        "wallet": wallet,
        "chain_id": chain_id,
        "token": "MOO_VAULT",
        "amount": 1.0,
        "eur_value": 1000.0,
        "direction": "in",
    }

    # Exit: wir geben 1.0 MOO-Token ab und erhalten 1200 EUR in ETH
    dt_exit = _iso_days_ago(1)
    exit_vault_out = {
        "tx_hash": "0xEXIT",
        "dt_iso": dt_exit,
        "wallet": wallet,
        "chain_id": chain_id,
        "token": "MOO_VAULT",
        "amount": -1.0,
        "eur_value": 0.0,  # Wert wird über die Base-Assets repräsentiert
        "direction": "out",
    }
    exit_base_in = {
        "tx_hash": "0xEXIT",
        "dt_iso": dt_exit,
        "wallet": wallet,
        "chain_id": chain_id,
        "token": "ETH",
        "amount": 1.0,
        "eur_value": 1200.0,
        "direction": "in",
    }

    classified_dicts = [mint_row, exit_vault_out, exit_base_in]

    # economic_gains: leerer Startzustand
    economic_gains = []
    gains_rows = []  # aktuell nicht genutzt, Signatur erfordert es aber

    result = apply_vault_exits(economic_gains, classified_dicts, gains_rows)

    # Es sollte genau ein position_exit-Eintrag hinzugekommen sein
    position_exits = [r for r in result if r.get("category") == "position_exit"]
    assert len(position_exits) == 1

    pe = position_exits[0]

    # Kostenbasis = ursprünglicher Mint-Wert
    assert pe["cost_basis_eur"] == 1000.0

    # Proceeds = 1200 EUR, also PnL = 200 EUR
    assert pe["proceeds_eur"] == 1200.0
    assert pe["pnl_eur"] == 200.0

    # Haltefrist: ca. 9 Tage (10 Tage - 1 Tag)
    assert pe["hold_days"] is not None
    assert pe["hold_days"] >= 8
    assert pe["hold_days"] <= 15

