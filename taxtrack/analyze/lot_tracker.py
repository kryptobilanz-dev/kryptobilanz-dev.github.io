# taxtrack/analyze/lot_tracker.py

from __future__ import annotations
from dataclasses import dataclass
from collections import defaultdict
from typing import Dict, List, Tuple
import sys

@dataclass
class Lot:
    token: str
    amount: float
    cost_eur: float
    timestamp: int
    reinvest: bool = False


def _lot_key(chain_id: str, token: str, owner_wallet: str = "") -> Tuple[str, str, str]:
    return ((chain_id or "").lower(), (token or "").upper(), (owner_wallet or "").strip().lower())


class FIFOTracker:
    def __init__(self) -> None:
        self.lots: Dict[Tuple[str, str, str], List[Lot]] = defaultdict(list)

    def add_lot(self, chain_id: str, token: str, amount: float, cost_eur: float, timestamp: int, reinvest: bool = False, owner_wallet: str = "") -> None:
        if amount <= 0:
            return
        key = _lot_key(chain_id, token, owner_wallet)
        self.lots[key].append(
            Lot(token=token, amount=float(amount), cost_eur=float(cost_eur), timestamp=int(timestamp), reinvest=reinvest)
        )

    def _consume_lots(self, chain_id: str, token: str, amount: float, owner_wallet: str = "") -> List[Lot]:
        used: List[Lot] = []
        remaining = float(amount)
        if remaining <= 0:
            return used

        queue = self.lots[_lot_key(chain_id, token, owner_wallet)]

        while remaining > 1e-12 and queue:
            lot = queue[0]
            if lot.amount <= remaining + 1e-12:
                used.append(
                    Lot(token=lot.token, amount=lot.amount, cost_eur=lot.cost_eur,
                        timestamp=lot.timestamp, reinvest=lot.reinvest)
                )
                remaining -= lot.amount
                queue.pop(0)
            else:
                ratio = remaining / lot.amount
                part_cost = lot.cost_eur * ratio

                used.append(
                    Lot(token=lot.token, amount=remaining, cost_eur=part_cost,
                        timestamp=lot.timestamp, reinvest=lot.reinvest)
                )

                lot.amount -= remaining
                lot.cost_eur -= part_cost
                remaining = 0.0

        return used


def add_lot(
    lots: Dict[Tuple[str, str, str], List[Lot]],
    chain_id: str,
    token: str,
    amount: float,
    cost_eur: float,
    timestamp: int,
    reinvest: bool = False,
    owner_wallet: str = "",
) -> None:
    if amount <= 0:
        return
    # Keep lots in deterministic chronological order for FIFO.
    lot = Lot(token=token, amount=float(amount), cost_eur=float(cost_eur), timestamp=int(timestamp), reinvest=reinvest)
    q = lots[_lot_key(chain_id, token, owner_wallet)]
    if not q or q[-1].timestamp <= lot.timestamp:
        q.append(lot)
        return
    # Insert while preserving stable order among equal timestamps.
    i = len(q)
    while i > 0 and q[i - 1].timestamp > lot.timestamp:
        i -= 1
    q.insert(i, lot)


def remove_lot(
    lots: Dict[Tuple[str, str, str], List[Lot]],
    chain_id: str,
    token: str,
    amount: float,
    owner_wallet: str = "",
) -> List[Lot]:
    """Backward-compatible removal; logs negative balance but returns used lots only."""
    used, shortfall = remove_lot_checked(lots, chain_id, token, amount, owner_wallet=owner_wallet)
    if shortfall > 1e-12:
        print(f"[LOT ERROR] negative balance chain={chain_id} token={token} shortfall={shortfall}", file=sys.stderr)
    return used


def remove_lot_checked(
    lots: Dict[Tuple[str, str, str], List[Lot]],
    chain_id: str,
    token: str,
    amount: float,
    owner_wallet: str = "",
) -> Tuple[List[Lot], float]:
    """
    Strict FIFO consumption in chronological order.
    Returns (used_lots, shortfall_amount). Never raises.
    """
    used: List[Lot] = []
    remaining = float(amount)
    if remaining <= 0:
        return used, 0.0

    queue = lots[_lot_key(chain_id, token, owner_wallet)]

    # Defensive: if queue ever gets out-of-order, re-sort deterministically.
    if len(queue) >= 2 and any(queue[i].timestamp > queue[i + 1].timestamp for i in range(len(queue) - 1)):
        queue.sort(key=lambda l: l.timestamp)

    while remaining > 1e-12 and queue:
        lot = queue[0]

        if lot.amount <= remaining + 1e-12:
            used.append(
                Lot(token=lot.token, amount=lot.amount, cost_eur=lot.cost_eur,
                    timestamp=lot.timestamp, reinvest=lot.reinvest)
            )
            remaining -= lot.amount
            queue.pop(0)
        else:
            ratio = remaining / lot.amount
            part_cost = lot.cost_eur * ratio

            used.append(
                Lot(token=lot.token, amount=remaining, cost_eur=part_cost,
                    timestamp=lot.timestamp, reinvest=lot.reinvest)
            )

            lot.amount -= remaining
            lot.cost_eur -= part_cost
            remaining = 0.0

    shortfall = remaining if remaining > 1e-12 else 0.0
    return used, shortfall


# Für alte Imports, falls irgendwo verwendet
LotClass = Lot
