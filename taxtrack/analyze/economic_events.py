# taxtrack/analyze/economic_events.py

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Optional, Any, Tuple

from taxtrack.analyze.swap_engine import SwapEvent

SECONDS_PER_DAY = 86400


# ------------------------------------------------------
# Datamodel für ökonomische Events
# ------------------------------------------------------

@dataclass
class EconomicLeg:
    """
    Reiner ökonomischer Handel, abgeleitet aus einem SwapEvent.

    Beispiel:
      SwapEvent:  TON -> LRT_EETH  (canonical restake / EigenLayer receipt)
      EconomicLeg:
        - sold_token   = "TON"
        - amount_sold  = 1234
        - bought_token = "LRT_EETH"
        - amount_bought= 0.42
        - eur_value    = 400.0   (Wert des Verkaufs)
    """
    tx_hash: str
    timestamp: int

    sold_token: str
    amount_sold: float

    bought_token: str
    amount_bought: float

    # EUR-Wert der Veräußerung (Brutto, ohne Gebührenabzug)
    eur_value: float

    # Optionale Meta-Infos (z. B. Route, Gas, Kategorie)
    meta: Optional[Dict[str, Any]] = None


@dataclass
class InventoryLot:
    """
    FIFO-Los im Bestand.

    - amount          → noch vorhandene Menge
    - eur_cost        → verbleibende Anschaffungskosten für 'amount'
    - acquisition_ts  → Zeitpunkt der Anschaffung
    """
    token: str
    amount: float
    eur_cost: float
    acquisition_timestamp: int
    meta: Optional[Dict[str, Any]] = None


@dataclass
class SellEvent:
    """
    Steuerrelevanter Verkauf einzelner Teilmengen,
    die aus einem konkreten FIFO-Los stammen.

    Ein EconomicLeg kann mehrere SellEvents erzeugen,
    wenn er in mehrere Lots greift.
    """
    tx_hash: str
    timestamp: int  # Verkaufszeitpunkt

    token: str
    amount: float

    eur_proceeds: float  # Veräußerungserlös (anteilig)
    eur_cost: float      # Anschaffungskosten (anteilig)
    eur_gain: float      # Gewinn/Verlust (proceeds - cost)

    acquisition_timestamp: int
    holding_days: int
    taxable: bool  # nach §23 (true/false)

    meta: Optional[Dict[str, Any]] = None


InventoryState = Dict[str, List[InventoryLot]]


# ------------------------------------------------------
# Hilfsfunktionen
# ------------------------------------------------------

def _holding_days(acq_ts: int, sell_ts: int) -> int:
    if acq_ts is None or sell_ts is None:
        return 0
    if sell_ts <= acq_ts:
        return 0
    return int((sell_ts - acq_ts) // SECONDS_PER_DAY)


def _ensure_inventory(state: InventoryState, token: str) -> List[InventoryLot]:
    lots = state.get(token)
    if lots is None:
        lots = []
        state[token] = lots
    return lots


# ------------------------------------------------------
# 1. Aus SwapEvents → EconomicLegs
# ------------------------------------------------------

def economic_legs_from_swaps(swaps: List[SwapEvent]) -> List[EconomicLeg]:
    """
    Transformiert SwapEvents in reine Handelspaare (EconomicLegs).

    Annahme:
    - Du veräußerst den input_token (token_in)
    - Du erwirbst den output_token (token_out)
    - Der steuerliche Veräußerungswert entspricht dem EUR-Wert
      des erhaltenen Tokens (eur_out) oder ersatzweise eur_in.
    """

    legs: List[EconomicLeg] = []

    # Sicherheit: nach Zeit / Hash sortieren
    swaps_sorted = sorted(swaps, key=lambda s: (s.timestamp, s.tx_hash))

    for s in swaps_sorted:
        if not s.taxable:
            # Nicht steuerbare Swaps (z. B. interne Umbuchung) überspringen
            continue

        sold_token = (s.token_in or "").upper()
        bought_token = (s.token_out or "").upper()

        if not sold_token or not bought_token:
            # Unvollständige Daten → kein EconomicLeg
            continue

        # EUR-Wert der Veräußerung:
        # Standard: Wert des erhaltenen Tokens (eur_out),
        # Fallback: eur_in
        eur_value = s.eur_out or s.eur_in or 0.0

        meta = {
            "category": getattr(s, "category", "swap"),
            "gas_token": getattr(s, "gas_token", None),
            "gas_amount": getattr(s, "gas_amount", 0.0),
            "eur_fee": getattr(s, "eur_fee", 0.0),
            "raw_meta": s.meta or {},
        }

        leg = EconomicLeg(
            tx_hash=s.tx_hash,
            timestamp=s.timestamp,
            sold_token=sold_token,
            amount_sold=s.amount_in,
            bought_token=bought_token,
            amount_bought=s.amount_out,
            eur_value=eur_value,
            meta=meta,
        )
        legs.append(leg)

    return legs


# ------------------------------------------------------
# 2. FIFO-Logik: EconomicLegs → SellEvents + neues Inventar
# ------------------------------------------------------

def fifo_from_economic_legs(
    legs: List[EconomicLeg],
    initial_inventory: Optional[InventoryState] = None,
    holding_period_days: int = 365,
) -> Tuple[List[SellEvent], InventoryState]:
    """
    Wendet eine einfache FIFO-Logik auf eine Liste von EconomicLegs an.

    Ergebnis:
      - Liste von SellEvents (steuerrelevante Verkäufe)
      - Aktualisierte Inventar-Struktur (InventoryState)

    initial_inventory:
      - optionaler Startbestand (z. B. aus Vorjahren)
      - Format: { "TOKEN": [InventoryLot, ...], ... }

    holding_period_days:
      - Frist für Steuerfreiheit gemäß §23
      - default: 365 Tage
    """

    inventory: InventoryState = {k: list(v) for k, v in (initial_inventory or {}).items()}
    sell_events: List[SellEvent] = []

    # Sicherheit: chronologisch sortieren
    legs_sorted = sorted(legs, key=lambda l: (l.timestamp, l.tx_hash))

    for leg in legs_sorted:
        sold = leg.sold_token
        bought = leg.bought_token

        # -----------------------
        # 2.1 Verkauf (sold side)
        # -----------------------
        if leg.amount_sold and leg.amount_sold > 0:
            remaining_to_sell = float(leg.amount_sold)
            total_amount_sold = float(leg.amount_sold)
            total_proceeds = float(leg.eur_value or 0.0)

            lots = _ensure_inventory(inventory, sold)

            # FIFO: älteste Lots zuerst
            lots.sort(key=lambda lot: lot.acquisition_timestamp)

            for lot in list(lots):  # Kopie, weil wir evtl. Elemente entfernen
                if remaining_to_sell <= 0:
                    break
                if lot.amount <= 0:
                    continue

                take = min(lot.amount, remaining_to_sell)
                if take <= 0:
                    continue

                # Anteil dieses Lots am gesamten Verkauf
                proportion = take / total_amount_sold if total_amount_sold > 0 else 0.0

                # Erlös-Anteil
                eur_proceeds = total_proceeds * proportion

                # Kosten-Anteil aus diesem Lot
                lot_amount_before = lot.amount
                lot_cost_before = lot.eur_cost

                eur_cost = 0.0
                if lot_amount_before > 0 and lot_cost_before > 0:
                    eur_cost = lot_cost_before * (take / lot_amount_before)

                # Lot anpassen
                lot.amount -= take
                lot.eur_cost -= eur_cost
                if lot.amount <= 0.0:
                    lots.remove(lot)

                gain = eur_proceeds - eur_cost
                hd = _holding_days(lot.acquisition_timestamp, leg.timestamp)
                taxable = hd < holding_period_days

                se_meta: Dict[str, Any] = {
                    "acquisition_ts": lot.acquisition_timestamp,
                    "holding_days": hd,
                }
                if leg.meta:
                    se_meta.update(leg.meta)

                sell_events.append(
                    SellEvent(
                        tx_hash=leg.tx_hash,
                        timestamp=leg.timestamp,
                        token=sold,
                        amount=take,
                        eur_proceeds=eur_proceeds,
                        eur_cost=eur_cost,
                        eur_gain=gain,
                        acquisition_timestamp=lot.acquisition_timestamp,
                        holding_days=hd,
                        taxable=taxable,
                        meta=se_meta,
                    )
                )

                remaining_to_sell -= take

            # Falls kein/zu wenig Bestand vorhanden war:
            # Rest als "ohne Cost-Basis" behandeln (Cost=0, voll steuerpflichtig)
            if remaining_to_sell > 1e-12:
                proportion = remaining_to_sell / total_amount_sold if total_amount_sold > 0 else 0.0
                eur_proceeds = total_proceeds * proportion
                gain = eur_proceeds  # cost = 0

                se_meta: Dict[str, Any] = {
                    "no_fifo_lot_found": True,
                }
                if leg.meta:
                    se_meta.update(leg.meta)

                sell_events.append(
                    SellEvent(
                        tx_hash=leg.tx_hash,
                        timestamp=leg.timestamp,
                        token=sold,
                        amount=remaining_to_sell,
                        eur_proceeds=eur_proceeds,
                        eur_cost=0.0,
                        eur_gain=gain,
                        acquisition_timestamp=leg.timestamp,
                        holding_days=0,
                        taxable=True,
                        meta=se_meta,
                    )
                )

        # -----------------------
        # 2.2 Zugang (bought side)
        # -----------------------
        if leg.amount_bought and leg.amount_bought > 0:
            lots_bought = _ensure_inventory(inventory, bought)

            lot_meta: Dict[str, Any] = {"tx_hash": leg.tx_hash}
            if leg.meta:
                lot_meta.update(leg.meta)

            lots_bought.append(
                InventoryLot(
                    token=bought,
                    amount=float(leg.amount_bought),
                    eur_cost=float(leg.eur_value or 0.0),
                    acquisition_timestamp=leg.timestamp,
                    meta=lot_meta,
                )
            )

    return sell_events, inventory
