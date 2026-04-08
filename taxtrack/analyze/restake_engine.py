# taxtrack/analyze/restake_engine.py
# ZenTaxCore Restaking Engine v0.1

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Sequence

_ZERO = "0x0000000000000000000000000000000000000000"
_DEAD = "0x000000000000000000000000000000000000dead"


def _addr_norm(a: str) -> str:
    return (a or "").strip().lower()


def _is_burn_destination(to_addr: str) -> bool:
    t = _addr_norm(to_addr)
    return t in (_ZERO, _DEAD)


@dataclass
class RestakeEvent:
    token: str
    amount: float
    eur_value: float
    is_inflow: bool


def lrt_symbol(base_token: str) -> str:
    """
    Canonical FIFO / lot identity for restaked positions (must match swap_engine restake resolution).

    ``canonical = f"LRT_{base_token}"`` with uppercased base from the on-chain row symbol.
    Examples: swETH → LRT_SWETH, eETH → LRT_EETH; unknown/generic ETH restake bucket → LRT_ETH.
    """
    base = (base_token or "").upper()
    return f"LRT_{base}"


def process_restake_in(item):
    """
    Restake-IN:
    Der Nutzer staked einen Token in ein Restaking-System.
    Steuerlich NICHT relevant.
    """
    token = lrt_symbol(getattr(item, "token", ""))
    amount = abs(float(getattr(item, "amount", 0.0)))
    eur    = float(getattr(item, "eur_value", 0.0))
    return RestakeEvent(token, amount, eur, True)


def _owner_wallet(item: Any) -> str:
    m = getattr(item, "meta", None)
    if isinstance(m, dict):
        return (m.get("owner_wallet") or "").strip().lower()
    return ""


def _disposal_candidates_for_restake_out(item: Any, tx_items: Sequence[Any]) -> List[Any]:
    """
    Same tx_hash as item; legs that dispose of the LRT position:
    - direction == 'out' (wallet sends token away), or
    - ERC20-style burn: from owner_wallet -> zero/dead address.
    """
    txh = getattr(item, "tx_hash", "") or ""
    ow = _owner_wallet(item)
    candidates: List[Any] = []

    for row in tx_items:
        if (getattr(row, "tx_hash", "") or "") != txh:
            continue
        amt = abs(float(getattr(row, "amount", 0.0) or 0.0))
        if amt <= 0:
            continue

        d = (getattr(row, "direction", "") or "").lower()
        if d == "out":
            candidates.append(row)
            continue

        if ow and _is_burn_destination(getattr(row, "to_addr", "")):
            fa = _addr_norm(getattr(row, "from_addr", ""))
            if fa == ow:
                candidates.append(row)

    return candidates


def _select_disposal_leg(candidates: List[Any]) -> Any:
    """
    Prefer the economically dominant outgoing leg (typical: single LRT burn;
    if several outs e.g. fee + LRT, take max EUR then max amount).
    """
    if not candidates:
        return None

    def _score(r: Any) -> tuple:
        eur = float(getattr(r, "eur_value", 0.0) or 0.0)
        amt = abs(float(getattr(r, "amount", 0.0) or 0.0))
        return (eur, amt)

    return max(candidates, key=_score)


def process_restake_out(item, total_underlying_eur, tx_items: Sequence[Any]):
    """
    Restake-OUT:
    Der Nutzer erhält underlying zurück (z.B. ETH).
    Steuerlich ein VERKAUF des LRT-Tokens — Token/Menge kommen von der
    ausgehenden / verbrannten Leg, nicht von der eingehenden Underlying-Zeile.
    """
    candidates = _disposal_candidates_for_restake_out(item, tx_items)
    leg = _select_disposal_leg(candidates)
    if leg is None:
        raise ValueError("restake_out: no disposal leg found")

    base = (getattr(leg, "token", "") or "").strip()
    token = lrt_symbol(base)
    amount = abs(float(getattr(leg, "amount", 0.0)))
    return RestakeEvent(token, amount, total_underlying_eur, False)
