# taxtrack/analyze/pendle_engine.py
# ZenTaxCore Pendle Engine v0.1

from dataclasses import dataclass

@dataclass
class PendleEvent:
    token: str
    amount: float
    eur_value: float
    is_inflow: bool


def pendle_token_symbol(base_token: str) -> str:
    """
    Erzeugt ein synthetisches Pendle-Token-Symbol.
    Beispiel:
      base_token = "EZETH" → "PENDLE_EZETH"
    """
    base = (base_token or "").upper()
    return f"PENDLE_{base}"


def process_pendle_deposit(item) -> PendleEvent:
    """
    Pendle-Deposit:
    - Steuerlich: NICHT direkt steuerbar (ähnlich LP-Add).
    - Wir erzeugen einen Lot-Zugang für ein synthetisches Pendle-Token.
    - cost_basis = eur_value der Transaktion.
    """
    token = pendle_token_symbol(getattr(item, "token", ""))
    amount = abs(float(getattr(item, "amount", 0.0) or 0.0))
    eur = float(getattr(item, "eur_value", 0.0) or 0.0)

    return PendleEvent(token=token, amount=amount, eur_value=eur, is_inflow=True)


def process_pendle_redeem(item, total_underlying_eur: float) -> PendleEvent:
    """
    Pendle-Redeem:
    - Wir behandeln einen Redeem als Verkauf des synthetischen Pendle-Tokens.
    - Erlös = Summe der Underlyings (total_underlying_eur).
    """
    token = pendle_token_symbol(getattr(item, "token", ""))
    amount = abs(float(getattr(item, "amount", 0.0) or 0.0))

    return PendleEvent(token=token, amount=amount, eur_value=total_underlying_eur, is_inflow=False)
