# taxtrack/analyze/lp_engine.py
# ZenTaxCore LP Processing 1.0

from dataclasses import dataclass

@dataclass
class LPEvent:
    token: str
    amount: float
    eur_value: float
    is_inflow: bool

def lp_token_id(item):
    chain = getattr(item, "chain", "eth")
    pool = (
        getattr(item, "pool_id", None)
        or getattr(item, "contract", None)
        or getattr(item, "counterparty", None)
        or "unknown_pool"
    )
    return f"LP::{chain}::{pool}"

def process_lp_add(item):
    lp_token = lp_token_id(item)
    return LPEvent(
        token=lp_token,
        amount=abs(item.amount),
        eur_value=abs(item.eur_value),
        is_inflow=True
    )

def process_lp_remove(item, underlying_outputs):
    lp_token = lp_token_id(item)

    events = []

    # LP-Disposal
    events.append(LPEvent(
        token=lp_token,
        amount=abs(item.amount),
        eur_value=sum(x[2] for x in underlying_outputs),
        is_inflow=False,
    ))

    # Underlying-Neuanschaffung
    for tok, amt, eur in underlying_outputs:
        events.append(LPEvent(
            token=tok,
            amount=amt,
            eur_value=eur,
            is_inflow=True
        ))

    return events

