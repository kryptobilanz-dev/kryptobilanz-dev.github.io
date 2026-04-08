# taxtrack/analyze/gains.py
# ZenTaxCore Gains Engine v1.1 (stabil, fehlerfrei, stefangerecht)

from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple
import sys

from taxtrack.analyze.lot_tracker import Lot, add_lot, remove_lot_checked
from taxtrack.analyze.tax_rules import calc_holding_days, classify_tax_type, taxable_status



# ============================================================
# Helper: LP + Pendle + Restake Underlying Normalization
# ============================================================

def _normalize_liquidity_and_pendle(classified_items):
    """
    LP-, Pendle- & Restake-Underlyings v1.1

    - Gruppiert pro tx_hash
    - Summiert EUR-Werte aller IN-Flows
    - Setzt diese Summe als Erlös für:
        * lp_remove
        * pendle_redeem
        * restake_out
    """

    by_tx = defaultdict(list)

    for it in classified_items:
        txh = getattr(it, "tx_hash", "") or ""
        by_tx[txh].append(it)

    normalized = []

    for txh, items in by_tx.items():

        inflow_eur = sum(
            float(getattr(it, "eur_value", 0.0) or 0.0)
            for it in items
            if (getattr(it, "direction", "").lower() == "in")
        )

        if inflow_eur > 0:
            for it in items:
                cat = getattr(it, "category", "").lower()
                if cat in ("pendle_redeem", "restake_out"):
                    it.eur_value = inflow_eur
        normalized.extend(items)

    return normalized

# ============================================================
# Helper: LP-Remove → Underlying einsammeln (tx_hash-basiert)
# ============================================================

def _collect_lp_underlyings(all_items, lp_item):
    """
    Sammelt alle Underlying-Tokens eines LP-Remove aus derselben tx_hash.
    Regeln:
    - gleiche tx_hash
    - direction == 'in'
    - nicht lp_add / lp_remove
    """
    underlyings = []

    txh = getattr(lp_item, "tx_hash", "") or ""

    for it in all_items:
        if getattr(it, "tx_hash", "") != txh:
            continue
        if getattr(it, "direction", "").lower() != "in":
            continue

        cat = (getattr(it, "category", "") or "").lower()
        if cat in ("lp_add", "lp_remove"):
            continue

        token = (getattr(it, "token", "") or "").upper()
        amt = abs(float(getattr(it, "amount", 0.0) or 0.0))
        eur = float(getattr(it, "eur_value", 0.0) or 0.0)

        if token and amt > 0:
            underlyings.append((token, amt, eur))

    return underlyings


# ============================================================
# GainRow Datenmodell
# ============================================================

@dataclass
class GainRow:
    dt_iso: str
    token: str
    amount_out: float
    proceeds_eur: float
    cost_basis_eur: float
    pnl_eur: float
    method: str
    tx_hash: str
    buy_date_iso: str
    hold_days: int
    tax_type: str
    taxable: bool
    is_reinvest: bool
    meta: Optional[Dict[str, Any]] = None

    def to_dict(self):
        return asdict(self)


# ============================================================
# Hauptfunktion compute_gains
# ============================================================

def compute_gains(classified_items) -> Tuple[List[GainRow], Dict[str, float]]:

    # 1. Normalize LP + Pendle + Restake
    classified_items = _normalize_liquidity_and_pendle(classified_items)
    
    # 1b. Chronologisch & deterministisch sortieren (FIFO braucht Zeitordnung)
    def _get_sort_key(it):
        dt_iso = getattr(it, "dt_iso", "") or ""
        try:
            ts = datetime.fromisoformat(dt_iso).timestamp()
        except Exception:
            ts = 0
        txh = getattr(it, "tx_hash", "") or ""
        dirn = (getattr(it, "direction", "") or "").lower()
        m = getattr(it, "meta", None)
        ow = (m.get("owner_wallet") or "").lower() if isinstance(m, dict) else ""
        # Deterministic within same timestamp:
        # inflows before outflows to avoid transient negative balances.
        rank = 3
        if dirn == "in":
            rank = 0
        elif dirn == "swap":
            rank = 1
        elif dirn == "out":
            rank = 2
        return (ts, txh, rank, ow)

    classified_items = sorted(classified_items, key=_get_sort_key)


    # 2. LOT Speicher (pro Chain, Token, Wallet – Multi-Wallet getrennt)
    lots: Dict[Tuple[str, str, str], List[Lot]] = defaultdict(list)
    gains: List[GainRow] = []
    fifo_stats: Dict[str, float] = defaultdict(float)
    fifo_stats["lots_created"] = 0.0
    fifo_stats["lots_consumed"] = 0.0
    fifo_stats["gain_rows"] = 0.0
    fifo_stats["skipped_missing_price"] = 0.0
    fifo_stats["negative_balance_warnings"] = 0.0

    # --------------------------------------------------------
    # Hilfsfunktionen für INFLOW / OUTFLOW
    # --------------------------------------------------------

    def inflow(
        chain_id: str,
        token: str,
        amount: float,
        eur: float,
        ts: int,
        reinvest: bool,
        *,
        owner_wallet: str = "",
    ):
        if amount > 0:
            add_lot(lots, chain_id, token, amount, eur, ts, reinvest=reinvest, owner_wallet=owner_wallet)
            fifo_stats["lots_created"] += 1.0

    def outflow(
        chain_id: str,
        dt_iso: str,
        token: str,
        amount: float,
        eur_val: float,
        cat: str,
        txh: str,
        meta: dict | None = None,
        *,
        owner_wallet: str = "",
    ):
        if amount <= 0:
            return
        try:
            ev = float(eur_val) if eur_val is not None else 0.0
        except (TypeError, ValueError):
            ev = 0.0
        missing_val = eur_val is None or ev <= 0
        proceeds_total = 0.0 if missing_val else ev

        if missing_val:
            fifo_stats["skipped_missing_price"] += 1.0
            print(
                f"[GAIN WARN] missing price -> disposal preserved tx={txh}",
                file=sys.stderr,
            )
            if isinstance(meta, dict):
                meta["valuation_missing"] = True

        ts_sell = int(datetime.fromisoformat(dt_iso).timestamp())
        used, shortfall = remove_lot_checked(lots, chain_id, token, amount, owner_wallet=owner_wallet)

        if not used:
            if shortfall > 1e-12:
                fifo_stats["negative_balance_warnings"] += 1.0
            sell_date = datetime.utcfromtimestamp(ts_sell).strftime("%Y-%m-%d")
            tax_type = classify_tax_type(cat)
            gains.append(
                GainRow(
                    dt_iso=dt_iso,
                    token=token,
                    amount_out=float(amount),
                    proceeds_eur=0.0,
                    cost_basis_eur=0.0,
                    pnl_eur=0.0,
                    method=cat,
                    tx_hash=txh,
                    buy_date_iso=sell_date,
                    hold_days=0,
                    tax_type=tax_type,
                    taxable=taxable_status(tax_type, 0),
                    is_reinvest=False,
                    meta={
                        "valuation_missing": missing_val,
                        "negative_balance": True,
                    },
                )
            )
            fifo_stats["gain_rows"] += 1.0
            return

        if shortfall > 1e-12:
            fifo_stats["negative_balance_warnings"] += 1.0

        total_amt = sum(l.amount for l in used) or 1e-12

        for lot in used:
            fifo_stats["lots_consumed"] += 1.0

            share = lot.amount / total_amt
            proceeds = proceeds_total * share
            hold_days = calc_holding_days(lot.timestamp, ts_sell)

            tax_type = classify_tax_type(cat)
            taxable = taxable_status(tax_type, hold_days)
            pnl = proceeds - lot.cost_eur

            row_meta: Dict[str, Any] | None = (
                {"valuation_missing": True} if missing_val else None
            )
            gains.append(
                GainRow(
                    dt_iso=dt_iso,
                    token=token,
                    amount_out=lot.amount,
                    proceeds_eur=round(proceeds, 2),
                    cost_basis_eur=round(lot.cost_eur, 2),
                    pnl_eur=round(pnl, 2),
                    method=cat,
                    tx_hash=txh,
                    buy_date_iso=datetime.utcfromtimestamp(lot.timestamp).strftime("%Y-%m-%d"),
                    hold_days=hold_days,
                    tax_type=tax_type,
                    taxable=taxable,
                    is_reinvest=lot.reinvest,
                    meta=row_meta,
                )
            )
            fifo_stats["gain_rows"] += 1.0

        if shortfall > 1e-12:
            sell_date = datetime.utcfromtimestamp(ts_sell).strftime("%Y-%m-%d")
            tax_type = classify_tax_type(cat)
            gains.append(
                GainRow(
                    dt_iso=dt_iso,
                    token=token,
                    amount_out=float(shortfall),
                    proceeds_eur=0.0,
                    cost_basis_eur=0.0,
                    pnl_eur=0.0,
                    method=cat,
                    tx_hash=txh,
                    buy_date_iso=sell_date,
                    hold_days=0,
                    tax_type=tax_type,
                    taxable=taxable_status(tax_type, 0),
                    is_reinvest=False,
                    meta={
                        "negative_balance": True,
                        "valuation_missing": missing_val,
                    },
                )
            )
            fifo_stats["gain_rows"] += 1.0

    # ============================================================
    # Hauptschleife
    # ============================================================

    for it in classified_items:
        chain_id = (getattr(it, "chain_id", "") or "").lower()
        token = (getattr(it, "token", "") or "").upper()
        amt = float(getattr(it, "amount", 0.0) or 0.0)
        eur = float(getattr(it, "eur_value", 0.0) or 0.0)
        cat = (getattr(it, "category", "") or "").lower()
        dirn = (getattr(it, "direction", "") or "").lower()
        dt_iso = getattr(it, "dt_iso", None)
        txh = getattr(it, "tx_hash", "") or ""
        meta = getattr(it, "meta", None) if isinstance(getattr(it, "meta", None), dict) else None
        ow = (meta.get("owner_wallet") or "").strip().lower() if isinstance(meta, dict) else ""

        # Internal transfers must never affect lots/gains.
        if cat in ("internal_transfer", "self_transfer") or dirn == "internal":
            continue
        # Rewards sind Einkommen (§22) → NICHT FIFO
        if cat in ("staking_reward", "reward", "learning_reward", "earn_reward"):
            continue

        if not dt_iso:
            continue

        ts = int(datetime.fromisoformat(dt_iso).timestamp())

        # --------------------------
        # SWAP (single economic event)
        # - FIFO disposal ONLY on token_out
        # - token_in creates a new position with cost basis = eur_value(token_out)
        # --------------------------
        if cat == "swap" and dirn == "swap":
            meta = getattr(it, "meta", None) or {}
            tokens_out = meta.get("tokens_out") or []
            tokens_in = meta.get("tokens_in") or []

            # Backward-compat fallback (older swap meta)
            if not tokens_out and (meta.get("token_out") and meta.get("amount_out")):
                tokens_out = [{
                    "token": (meta.get("token_out") or token).upper(),
                    "amount": float(meta.get("amount_out") or amt or 0.0),
                    "eur_value": float(meta.get("eur_value_out") or eur or 0.0),
                }]
            if not tokens_in and (meta.get("token_in") and meta.get("amount_in")):
                tokens_in = [{
                    "token": (meta.get("token_in") or "").upper(),
                    "amount": float(meta.get("amount_in") or 0.0),
                    "eur_value": float(meta.get("eur_value_in") or 0.0),
                }]

            # --- disposals: FIFO per token_out ---
            out_total_eur = 0.0
            out_legs = []
            for leg in tokens_out if isinstance(tokens_out, list) else []:
                tok = (leg.get("token") or "").upper()
                a = abs(float(leg.get("amount") or 0.0))
                e = float(leg.get("eur_value") or 0.0)
                if not tok or a <= 0:
                    continue
                out_legs.append((tok, a, e))
                out_total_eur += e
            if out_total_eur <= 0:
                out_total_eur = float(meta.get("total_out_value_eur") or eur or 0.0)

            # If individual eur_value missing, allocate proceeds proportionally by amount
            amt_sum = sum(a for _, a, _ in out_legs) or 0.0
            # Safeguard: if we cannot value either side, do not create taxable gain
            in_total_eur_probe = 0.0
            for leg in tokens_in if isinstance(tokens_in, list) else []:
                try:
                    in_total_eur_probe += float(leg.get("eur_value") or 0.0)
                except Exception:
                    pass
            basis_probe = out_total_eur if out_total_eur > 0 else (in_total_eur_probe if in_total_eur_probe > 0 else 0.0)
            if basis_probe <= 0:
                if isinstance(meta, dict):
                    meta["valuation_missing"] = True
                # Do NOT skip: preserve disposal rows so tx_hash appears in gains.
                # We intentionally do not invent proceeds; use 0.0 so FIFO cost basis drives pnl (=-cost) when lots exist.
                # If no lots exist, outflow() will emit a 0-row with negative_balance meta.
                for tok, a, _e in out_legs:
                    outflow(chain_id, dt_iso, tok, a, 0.0, "swap", txh, meta=meta if isinstance(meta, dict) else None, owner_wallet=ow)
                # No valuation → cannot set basis for acquisitions reliably.
                continue

            for tok, a, e in out_legs:
                proceeds = e if e > 0 else (out_total_eur * (a / amt_sum) if (out_total_eur > 0 and amt_sum > 0) else 0.0)
                if proceeds <= 0:
                    # If still not valuated, skip this disposal to avoid artificial gains
                    continue
                outflow(chain_id, dt_iso, tok, a, proceeds, "swap", txh, meta=meta if isinstance(meta, dict) else None, owner_wallet=ow)

            # --- acquisitions: new lots; basis allocated proportionally across tokens_in ---
            in_legs = []
            in_total_eur = 0.0
            in_total_amt = 0.0
            for leg in tokens_in if isinstance(tokens_in, list) else []:
                tok = (leg.get("token") or "").upper()
                a = abs(float(leg.get("amount") or 0.0))
                e = float(leg.get("eur_value") or 0.0)
                if not tok or a <= 0:
                    continue
                in_legs.append((tok, a, e))
                in_total_eur += e
                in_total_amt += a

            basis_total = out_total_eur if out_total_eur > 0 else float(eur or 0.0)
            if basis_total <= 0 and in_total_eur > 0:
                basis_total = in_total_eur
            if basis_total > 0 and in_legs:
                if in_total_eur > 0:
                    for tok, a, e in in_legs:
                        basis = basis_total * (e / in_total_eur)
                        inflow(chain_id, tok, a, basis, ts, reinvest=False, owner_wallet=ow)
                elif in_total_amt > 0:
                    for tok, a, _e in in_legs:
                        basis = basis_total * (a / in_total_amt)
                        inflow(chain_id, tok, a, basis, ts, reinvest=False, owner_wallet=ow)
            continue

        # Pendle Swap → normaler Swap
        if cat == "pendle_swap":
            cat = "swap"

        # --------------------------
        # INFLOWS
        # --------------------------

        # restake_out is direction=in (underlying received) but economically a disposal of LRT; handled below.
        if cat in ("buy", "receive", "bridge_in", "deposit") or (
            dirn == "in" and cat != "restake_out"
        ):
            inflow(chain_id, token, abs(amt), eur, ts, reinvest=False, owner_wallet=ow)
            continue

        if cat in ("reward", "staking_reward", "reinvest", "airdrop"):
            inflow(chain_id, token, abs(amt), eur, ts, reinvest=True, owner_wallet=ow)
            continue

        if cat == "lp_add":
            from taxtrack.analyze.lp_engine import process_lp_add
            ev = process_lp_add(it)
            inflow(chain_id, ev.token, ev.amount, ev.eur_value, ts, reinvest=False, owner_wallet=ow)
            continue

        if cat == "pendle_deposit":
            from taxtrack.analyze.pendle_engine import process_pendle_deposit
            ev = process_pendle_deposit(it)
            inflow(chain_id, ev.token, ev.amount, ev.eur_value, ts, reinvest=False, owner_wallet=ow)
            continue

        if cat == "restake_in":
            from taxtrack.analyze.restake_engine import process_restake_in
            ev = process_restake_in(it)
            inflow(chain_id, ev.token, ev.amount, ev.eur_value, ts, reinvest=False, owner_wallet=ow)
            continue

        # --------------------------
        # OUTFLOWS
        # --------------------------

        if cat == "lp_remove":
            from taxtrack.analyze.lp_engine import process_lp_remove

            underlyings = _collect_lp_underlyings(classified_items, it)
            events = process_lp_remove(it, underlyings)

            for ev in events:
                if ev.is_inflow:
                    # neue Anschaffung der Underlying-Tokens
                    inflow(chain_id, ev.token, abs(ev.amount), ev.eur_value, ts, reinvest=False, owner_wallet=ow)
                else:
                    # einzig steuerlich relevanter Disposal
                    outflow(chain_id, dt_iso, ev.token, abs(ev.amount), ev.eur_value, cat, txh, owner_wallet=ow)

            continue


        if cat == "pendle_redeem":
            from taxtrack.analyze.pendle_engine import process_pendle_redeem
            ev = process_pendle_redeem(it, eur)
            outflow(chain_id, dt_iso, ev.token, abs(ev.amount), ev.eur_value, cat, txh, meta=meta, owner_wallet=ow)
            continue

        if cat == "restake_out":
            from taxtrack.analyze.restake_engine import process_restake_out
            ev = process_restake_out(it, eur, classified_items)
            outflow(chain_id, dt_iso, ev.token, abs(ev.amount), ev.eur_value, cat, txh, meta=meta, owner_wallet=ow)
            continue

        if cat in ("sell", "swap", "withdraw", "trade", "stable_swap") or dirn == "out":
            outflow(chain_id, dt_iso, token, abs(amt), eur, cat, txh, meta=meta, owner_wallet=ow)
            continue

        # internal_transfer handled earlier

    # ============================================================
    # Totals
    # ============================================================

    totals = defaultdict(float)
    for g in gains:
        totals[g.token] += g.pnl_eur

    # ============================================================
    # Debug-Ausgabe
    # ============================================================

    print("\n=== ZenTaxCore FIFO/GAIN Übersicht ===", file=sys.stderr)
    for g in gains:
        status = "steuerpflichtig" if g.taxable else "steuerfrei"
        print(
            f"[{g.token}] {g.buy_date_iso} → {g.dt_iso.split('T')[0]} = "
            f"{g.hold_days}T | {status:<15} | {g.tax_type:<12} | "
            f"PnL {g.pnl_eur:>8.2f} €",
            file=sys.stderr,
        )
    print("======================================\n", file=sys.stderr)
    
    # ============================================================
    # Offene LP-Positionen zum Stichtag (31.12)
    # ============================================================

    def _collect_open_lp_positions(lots):
        positions = []

        for (lot_chain_id, token, _owner_w), stack in lots.items():
            if not token.startswith("LP::"):
                continue

            for lot in stack:
                positions.append({
                    "chain_id": lot_chain_id,
                    "lp_token": token,
                    "amount": lot.amount,
                    "cost_basis_eur": round(lot.cost_eur, 2),
                    "acquired_at": datetime.utcfromtimestamp(lot.timestamp).strftime("%Y-%m-%d"),
                })

        return positions

    open_lp_positions = _collect_open_lp_positions(lots)
    totals["open_lp_positions"] = open_lp_positions
    totals["fifo_summary"] = dict(fifo_stats)

    return gains, totals
