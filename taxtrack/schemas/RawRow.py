from dataclasses import dataclass, asdict
from typing import Optional, Dict, Any

@dataclass
class RawRow:
    # -------------------------
    # Pflichtfelder (KEINE Defaults)
    # -------------------------
    source: str
    tx_hash: str
    timestamp: int
    dt_iso: str

    from_addr: str
    to_addr: str

    token: str
    amount: float

    direction: str
    method: str

    # -------------------------
    # Optionale Felder (alle mit Default)
    # -------------------------
    amount_raw: Optional[str] = None
    decimals: Optional[int] = None
    contract_addr: Optional[str] = None

    fee_token: Optional[str] = None
    fee_amount: float = 0.0

    category: str = ""
    eur_value: float = 0.0
    fee_eur: float = 0.0
    taxable: bool = False

    chain_id: str = ""   # Canonical chain (eth, arb, op, base, bnb, matic, avax, etc.)
    meta: Optional[Dict[str, Any]] = None

    def to_dict(self):
        return asdict(self)
