# taxtrack/analyze/vault_exit_resolver.py

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from typing import Any, List, Dict, Deque, Tuple, Optional, Set

VAULT_TOKEN_HINTS = ("moo", "beefy", "lpt", "lp", "rcow", "pendle")
BASE_ASSETS = {"ETH", "WETH", "USDC", "USDT", "DAI", "OP", "ARB", "ZRO"}

# Swap-based vault lots: only when counterparty protocol matches (if present on meta).
_ALLOWED_SWAP_MINT_CP_PROTOCOLS = frozenset({"pendle", "beefy", "curve", "balancer", "vault"})

DEBUG_VAULT_EXIT = True


def is_vault_token(token: str) -> bool:
    t = (token or "").lower()
    return any(h in t for h in VAULT_TOKEN_HINTS)


def is_base_asset(token: str) -> bool:
    return (token or "").upper() in BASE_ASSETS


def _ts(dt_iso: str | None) -> int:
    if not dt_iso:
        return 0
    try:
        return int(datetime.fromisoformat(dt_iso).timestamp())
    except Exception:
        return 0


def _ledger_token_key(token_raw: str) -> str:
    """Uppercase symbol for ledger keys; keep PENDLE_* as-is (no double-prefix)."""
    return (token_raw or "").strip().upper()


def _meta_allows_swap_vault_ledger_mint(meta: Dict[str, Any]) -> bool:
    """
    If cp_protocol is missing → allow (fallback to is_vault_token on legs).
    If present → must be in allowed DeFi/vault protocols.
    """
    raw = (meta.get("cp_protocol") or "").strip().lower()
    if not raw:
        return True
    return raw in _ALLOWED_SWAP_MINT_CP_PROTOCOLS


def _subtype_from_token(tok: str) -> str:
    t = (tok or "").upper()
    if "PENDLE" in t:
        return "pendle"
    if "MOO" in t or "BEEFY" in t or "RCOW" in t:
        return "vault"
    return "generic"


@dataclass
class Lot:
    amount: float
    cost_eur: float
    ts_buy: int


def _consume_lots(lots: Deque[Lot], amount_out: float) -> Tuple[float, Optional[int]]:
    """
    FIFO-Verbrauch. Gibt (cost_eur, oldest_ts_buy_used) zurück.
    """
    remaining = float(amount_out)
    cost = 0.0
    oldest_ts: Optional[int] = None

    while remaining > 0 and lots:
        lot = lots[0]
        if oldest_ts is None:
            oldest_ts = lot.ts_buy

        if lot.amount <= remaining + 1e-18:
            cost += lot.cost_eur
            remaining -= lot.amount
            lots.popleft()
        else:
            # anteilig
            ratio = remaining / lot.amount
            cost += lot.cost_eur * ratio
            lot.amount = lot.amount - remaining
            lot.cost_eur = lot.cost_eur * (1 - ratio)
            remaining = 0.0

    # Wenn remaining > 0: keine Historie -> cost für den Rest bleibt 0 (sichtbar über Debug)
    return cost, oldest_ts



def apply_vault_exits(
    economic_gains: List[Dict],
    classified_dicts: List[Dict],
    gains_rows: List[Dict],  # wird hier nicht mehr benötigt, aber wir lassen die Signatur stabil
) -> List[Dict]:
    """
    Positions-Ledger v1 (übertragbar)
    - baut Lots aus:
        * direction=in, is_vault_token, eur_value>0
        * direction=swap: meta.tokens_in legs (vault tokens) mit eur_value>0 — aligned mit FIFO-Anschaffungen
    - Exit: OUT Position-Token + IN Base-Asset => position_exit
    - Netto-PnL berücksichtigt Fees (fee_eur)
    """

    # ----------------------------
    # 1) Classified nach tx_hash gruppieren
    # ----------------------------
    by_tx: Dict[str, List[Dict]] = defaultdict(list)
    for r in classified_dicts:
        txh = r.get("tx_hash") or ""
        if txh:
            by_tx[txh].append(r)

    # ----------------------------
    # 2) Positions-Ledger aufbauen: (chain, token) -> deque[Lot]
    #    Quelle:
    #      A) direction == "in", is_vault_token, eur_value > 0
    #      B) direction == "swap", meta.tokens_in legs (vault token, eur_value > 0), optional cp_protocol filter
    #    Dedupe: (tx_hash, chain, token, amount) — keine doppelte Lot-Einlage
    # ----------------------------
    ledger: Dict[Tuple[str, str], Deque[Lot]] = defaultdict(deque)
    seen_mint_keys: Set[Tuple[str, str, str, float]] = set()

    def _register_mint(tx_hash: str, chain: str, tok: str, amt: float, cost_eur: float, dt_iso: str | None) -> None:
        """Ledger keys use normalized chain (lowercase). Skips duplicate (tx, chain, token, amount)."""
        ch_key = (chain or "eth").strip().lower() or "eth"
        key = (
            (tx_hash or "").strip().lower(),
            ch_key,
            tok,
            round(float(amt), 12),
        )
        if key in seen_mint_keys:
            return
        seen_mint_keys.add(key)
        ledger[(ch_key, tok)].append(
            Lot(
                amount=float(amt),
                cost_eur=float(cost_eur),
                ts_buy=_ts(dt_iso),
            )
        )

    # chronologisch sortieren über dt_iso
    all_rows_sorted = sorted(
        [r for r in classified_dicts if r.get("dt_iso")],
        key=lambda r: r.get("dt_iso"),
    )

    for r in all_rows_sorted:
        txh = (r.get("tx_hash") or "").strip()
        dirn = (r.get("direction") or "").lower()
        dt_iso = r.get("dt_iso")
        chain = (r.get("chain_id") or "eth").strip().lower() or "eth"

        # --- A) klassische Inflows (Vault-Token) ---
        if dirn == "in":
            token_raw = r.get("token") or ""
            if not is_vault_token(token_raw):
                continue

            amt = abs(float(r.get("amount") or 0.0))
            eur = float(r.get("eur_value") or 0.0)
            if eur <= 0 or amt <= 0:
                continue

            tok = _ledger_token_key(token_raw)
            _register_mint(txh, chain, tok, amt, eur, dt_iso)
            continue

        # --- B) Swap: tokens_in → gleiche ökonomische Anschaffung wie FIFO ---
        if dirn != "swap":
            continue

        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        if not _meta_allows_swap_vault_ledger_mint(meta):
            continue

        for leg in meta.get("tokens_in") or []:
            if not isinstance(leg, dict):
                continue
            token_raw = leg.get("token") or ""
            if not is_vault_token(token_raw):
                continue
            amt = abs(float(leg.get("amount") or 0.0))
            eur = float(leg.get("eur_value") or 0.0)
            if amt <= 0 or eur <= 0:
                continue
            tok = _ledger_token_key(token_raw)
            _register_mint(txh, chain, tok, amt, eur, dt_iso)


    # ----------------------------
    # 3) Exits bewerten
    # ----------------------------
    extra: List[Dict] = []

    for txh, rows in by_tx.items():
        outs_pos = [
            r for r in rows
            if (r.get("direction") == "out")
            and is_vault_token(r.get("token", ""))
        ]
        ins_base = [
            r for r in rows
            if (r.get("direction") == "in")
            and is_base_asset(r.get("token", ""))
        ]
        if not outs_pos or not ins_base:
            continue

        proceeds = sum(float(r.get("eur_value") or 0.0) for r in ins_base)

        # ------------------------------------------------------------
        # Fees robust (pro tx_hash)
        # 1) sum fee_eur (wenn vorhanden)
        # 2) fallback: fee_amount * price(fee_token) via dt_iso
        # ------------------------------------------------------------
        fees = 0.0

        # (1) fee_eur direkt (falls schon befüllt)
        fees = sum(float(r.get("fee_eur") or 0.0) for r in rows)

        # (2) fallback über fee_amount/fee_token (wenn fee_eur fehlt)
        if fees <= 0:
            try:
                from taxtrack.prices import get_eur_price

                # Timestamp aus dt_iso ziehen (eine tx = ein Zeitpunkt reicht)
                dt0 = None
                for rr in rows:
                    if rr.get("dt_iso"):
                        dt0 = rr.get("dt_iso")
                        break

                ts0 = int(datetime.fromisoformat(dt0).timestamp()) if dt0 else 0

                if ts0 > 0:
                    for rr in rows:
                        fa = float(rr.get("fee_amount") or 0.0)
                        ft = (rr.get("fee_token") or "").upper()
                        chain = rr.get("chain_id") or ""
                        if fa > 0 and ft:
                            fees += fa * float(get_eur_price(ft, ts0, chain=chain))
            except Exception:
                # wenn price fehlt -> fees bleiben 0 (sichtbar)
                pass

        fees = float(fees or 0.0)
        
        if DEBUG_VAULT_EXIT and fees > 0:
            print("[FEES][OK]", "tx=", txh[:10], "fees=", round(fees, 2))
        elif DEBUG_VAULT_EXIT:
            print("[FEES][MISS]", "tx=", txh[:10], "fees=0")

        cost = 0.0
        oldest_ts: Optional[int] = None
        subtype = "generic"

        # FIFO-Cost über alle Position-Tokens, die rausgehen
        for o in outs_pos:
            tok = (o.get("token") or "").upper()
            subtype = _subtype_from_token(tok) or subtype

            amt_out = abs(float(o.get("amount") or 0.0))
            if amt_out <= 0:
                continue

            ch_key = (o.get("chain_id") or "eth").strip().lower() or "eth"

            c, ts_buy = _consume_lots(
                ledger[(ch_key, tok)],
                amt_out
            )
            cost += c
            if oldest_ts is None and ts_buy:
                oldest_ts = ts_buy
            if DEBUG_VAULT_EXIT:
                print(
                    "[LEDGER]",
                    "chain=", ch_key,
                    "token=", tok,
                    "remaining_lots=", len(ledger[(ch_key, tok)]),
                )
            if DEBUG_VAULT_EXIT and c <= 0 and amt_out > 0:
                # zeigt dir, dass Historie für diesen Token fehlt
                print(
                    "[POSITION_EXIT][MISSING_LOTS]",
                    "tx=", txh[:10],
                    "token=", tok,
                    "amount_out=", round(amt_out, 6),
                )

        # Hold days (minimal): ältestes Lot
        ts_sell = _ts(ins_base[0].get("dt_iso") or outs_pos[0].get("dt_iso"))
        hold_days = None
        if oldest_ts and ts_sell and ts_sell >= oldest_ts:
            hold_days = int((ts_sell - oldest_ts) / 86400)

        pnl = proceeds - cost
        net_pnl = pnl - fees
        
        # ⬇️ GENAU HIER
        if cost <= 0 and proceeds > 0:
            if DEBUG_VAULT_EXIT:
                print(
                    "[POSITION_EXIT][UNRESOLVED_COST]",
                    "tx=", txh[:10],
                    "proceeds=", round(proceeds, 2),
                    "fees=", round(fees, 2),
                )
            continue
        dt_iso = ins_base[0].get("dt_iso") or outs_pos[0].get("dt_iso")

        extra.append({
            "tx_hash": txh,
            "category": "position_exit",
            "subtype": subtype,
            "dt_iso": dt_iso,
            "token": "MULTI",
            "proceeds_eur": round(proceeds, 2),
            "cost_basis_eur": round(cost, 2),
            "pnl_eur": round(pnl, 2),
            "fees_eur": round(fees, 2),
            "net_pnl_eur": round(net_pnl, 2),
            "taxable": True,
            "hold_days": hold_days,
            "rows": len(rows),
        })
        if DEBUG_VAULT_EXIT:
            print("[LEDGER_ADD]", ch_key, tok, "lots=", len(ledger[(ch_key, tok)]))

        if DEBUG_VAULT_EXIT:
            print(
                "[POSITION_EXIT][OK]",
                "tx=", txh[:10],
                "subtype=", subtype,
                "proceeds=", round(proceeds, 2),
                "cost=", round(cost, 2),
                "fees=", round(fees, 2),
                "net=", round(net_pnl, 2),
            )

    return list(economic_gains or []) + extra
