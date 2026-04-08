# taxtrack/rules/evaluate.py
# ZenTaxCore Klassifikator v5.0 – DeFi / EVM intelligent (mit Swap-Resolver v1)

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List, Optional, Dict, Any
from collections import defaultdict  # 🔹 NEU: für tx_hash-Gruppierung

from taxtrack.utils.wallet import is_self_transfer
from taxtrack.utils.contract_labeler import label_address
from taxtrack.utils.debug_log import log
from taxtrack.utils.direction import derive_direction
from taxtrack.rules.taxlogic import TaxLogic


# -------------------------------------------------------------
# ClassifiedItem
# -------------------------------------------------------------

@dataclass
class ClassifiedItem:
    tx_hash: str
    dt_iso: str
    token: str
    amount: float
    eur_value: float
    from_addr: str
    to_addr: str
    direction: str
    category: str

    method: str = ""
    source: str = ""
    fee_token: Optional[str] = None
    fee_amount: float = 0.0
    taxable: bool = False
    reason: str = ""
    counterparty: Optional[str] = None
    note: str = ""
    chain_id: str = ""   # Canonical chain (eth, arb, op, etc.) for price resolution
    meta: Optional[Dict[str, Any]] = None

    def to_dict(self):
        d = asdict(self)
        d["from"] = d.pop("from_addr")
        d["to"] = d.pop("to_addr")
        return d

# -------------------------------------------------------------
# 🧱 Klassifikations-Prioritäten (höher = wichtiger)
# -------------------------------------------------------------

CATEGORY_PRIORITY = {
    "lp_remove": 100,
    "pendle_redeem": 95,
    "restake_out": 90,

    "swap": 80,
    "sell": 80,

    "reward": 70,
    "staking_reward": 70,

    "withdraw": 60,

    "lp_add": 50,
    "pendle_deposit": 50,
    "restake_in": 50,

    "bridge_out": 45,
    "bridge_in": 45,

    "transfer": 10,
    "receive": 10,
    "native_transfer_in": 10,
    "native_transfer_out": 10,
    "internal_transfer": 5,

    "unknown": 0,
}

def can_override(old: str, new: str) -> bool:
    """
    Entscheidet, ob eine bestehende Kategorie überschrieben werden darf.
    """
    old_p = CATEGORY_PRIORITY.get(old, 0)
    new_p = CATEGORY_PRIORITY.get(new, 0)
    return new_p > old_p


# -------------------------------------------------------------
# Hilfslogik
# -------------------------------------------------------------


def _basic_category(method: str, direction: str) -> str:
    m = (method or "").lower()

    if "swap" in m or "trade" in m:
        return "swap"
    if "sell" in m:
        return "sell"
    if "buy" in m:
        return "buy"
    if "deposit" in m:
        return "deposit"
    if "withdraw" in m or "redeem" in m:
        return "withdraw"
    if any(k in m for k in ["reward", "claim", "harvest"]):
        return "reward"

    if "transfer" in m:
        return "receive" if direction == "in" else "withdraw"

    return "unknown"


# -------------------------------------------------------------
# Counterparty Resolver
# -------------------------------------------------------------

def _resolve_counterparty(wallet: str, from_addr: str, to_addr: str, direction: str):
    """
    Gibt Adresse & Metadaten zurück:
    {
      "label": "pendle_router",
      "protocol": "pendle",
      "type": "router",
      "tags": ["defi","pendle"]
    }
    """
    fw = (from_addr or "").lower()
    tw = (to_addr or "").lower()
    wl = wallet.lower()

    # Bestimme Counterparty-Adresse
    if direction == "out":
        cp = to_addr
    elif direction == "in":
        cp = from_addr
    else:
        # Fallback
        if fw == wl and tw != wl:
            cp = to_addr
        elif tw == wl and fw != wl:
            cp = from_addr
        else:
            cp = to_addr or from_addr

    # NOTE: chain-specific labels are resolved in evaluate_batch where chain_id is known.
    info = label_address(cp) or {}
    return cp, info


# -------------------------------------------------------------
# Kategorie-Logik (DeFi Engine)
# -------------------------------------------------------------

def _refine_category(base_category,
                     raw_category,
                     method,
                     direction,
                     wallet,
                     from_addr,
                     to_addr,
                     cp_info):
    m = (method or "").lower()
    base = (base_category or "").lower()
    raw = (raw_category or "").lower()

    label = (cp_info.get("label") or "").lower()
    proto = (cp_info.get("protocol") or "").lower()
    ctype = (cp_info.get("type") or "").lower()
    tags  = [t.lower() for t in (cp_info.get("tags") or [])]

    joined = " ".join([label, proto, ctype] + tags)

    # 0) Self-transfer oder intern
    if is_self_transfer(wallet, from_addr, to_addr) or direction == "internal":
        return "internal_transfer"

    # 1) Restake FIRST — must never lose to raw "swap" / router / method-based swap
    if "restake" in joined or proto == "restake":
        return "restake_in" if direction == "out" else "restake_out"

    # 2) RAW Kategorie Vorrang (wenn NICHT transfer/unknown)
    # NOTE (Phase 2): raw native/erc20 transfer labels are often too low-level; do not let
    # them override method/protocol-based classification.
    if raw and raw not in (
        "erc20_transfer",
        "native_transfer",
        "native_transfer_in",
        "native_transfer_out",
        "transfer",
        "unknown",
    ):
        return raw

    # 3) Bridge
    if "bridge" in joined:
        return "bridge_out" if direction == "out" else "bridge_in"

    # 4) Pendle
    if "pendle" in joined:
        if "withdraw" in m or "redeem" in m:
            return "pendle_redeem"
        if direction == "out":
            return "pendle_deposit"
        if direction == "in":
            return "pendle_reward"
        return "pendle_unknown"

    # 5) DEX Swaps
    if "router" in joined or proto == "dex":
        if "swap" in m or direction == "out":
            return "swap"

    # 6) Lending
    if proto == "lending":
        if "repay" in m:
            return "lend_repay"
        return "lend_deposit" if direction == "out" else "lend_withdraw"

    # 7) Rewards (method-driven, protocol-agnostic)
    if any(k in m for k in ["reward", "claim", "harvest"]):
        return "reward"

    # 8) Protocol-driven reclassification to kill fake transfers (Phase 2.5)
    # These rules only trigger when we still look like a generic transfer.
    if raw in ("erc20_transfer", "native_transfer", "native_transfer_in", "native_transfer_out", "transfer", "unknown"):
        # Bridge via method patterns (even if protocol label is missing)
        if any(x in m for x in ["bridge", "relay", "gas zip", "gaszip", "across"]):
            return "bridge"

        # Protocol families for deterministic transfer reclassification.
        proto_deposit_like = {
            "beefy", "aave", "pendle", "curve", "balancer",
            "vault", "generic_vault",
        }
        proto_lp_like = {"uniswap", "curve", "balancer"}
        proto_reward_like = {
            "beefy", "pendle", "aave",
            "vault", "generic_vault",
        }

        # LP operations (method-driven, keep lp_add/lp_remove only when protocol confirms)
        if proto in proto_lp_like:
            if any(x in m for x in ["addliquidity", "add liquidity", "mint", "increase", "deposit"]):
                return "lp_add"
            if any(x in m for x in ["removeliquidity", "remove liquidity", "burn", "decrease", "collect", "withdraw"]):
                return "lp_remove"

        # Reward/claim/harvest (protocol-driven + method-driven)
        if direction == "in" and proto in proto_reward_like and any(x in m for x in ["claim", "harvest", "reward"]):
            return "reward"

        # Deposit/withdraw driven by protocol + direction
        if proto in proto_deposit_like:
            if direction == "out":
                return "deposit"
            if direction == "in":
                return "withdraw"

    # 9) Method + direction based refinement for remaining UNKNOWN / generic transfers
    #    (high-confidence, no new categories)
    if raw in ("unknown", "", None) or base == "unknown":
        # Swap-like
        if "swap" in m or "trade" in m:
            return "swap"
        # Explicit deposit / stake
        if any(x in m for x in ["deposit", "stake", "supply"]):
            return "deposit" if direction == "out" else "withdraw"
        # Explicit withdraw / redeem / unstake
        if any(x in m for x in ["withdraw", "redeem", "unstake"]):
            return "withdraw"
        # Bridge without protocol label but clear method hint
        if any(x in m for x in ["bridge", "relay"]):
            return "bridge"
        # Plain transfer-like methods
        if "transfer" in m:
            return "transfer"

    # 10) Fallback: basic category
    if base and base != "unknown":
        return base

    # 11) Transfer fallback (STRICT category set in Phase 2)
    # Raw EVM transfers without protocol context should stay plain 'transfer'.
    if raw in ("erc20_transfer", "native_transfer", "transfer"):
        return "transfer"

    return "transfer"


ALLOWED_CATEGORIES = {
    "swap",
    "sell",
    "buy",
    "deposit",
    "withdraw",
    "reward",
    "lp_add",
    "lp_remove",
    "pendle_deposit",
    "pendle_redeem",
    "restake",
    "restake_in",
    "restake_out",
    "bridge",
    "transfer",
    "internal_transfer",
    "unknown",
}


def _normalize_category(cat: str) -> str:
    """
    Map legacy/inconsistent categories into the strict allowed set.
    No new categories are introduced here.
    """
    c = (cat or "").lower().strip()
    if not c:
        return "unknown"

    # Legacy / inconsistent
    if c in ("native_transfer_in", "native_transfer_out", "erc20_transfer", "receive"):
        return "transfer"
    if c in ("bridge_in", "bridge_out"):
        return "bridge"
    if c in ("staking_reward", "pendle_reward"):
        return "reward"

    # Keep known
    if c in ALLOWED_CATEGORIES:
        return c

    # Collapse unknown legacy categories into 'unknown'
    return "unknown"


# -------------------------------------------------------------
# Multi-Owner: jede Zeile kann aus einer anderen Wallet-Quelle stammen (run_customer).
# -----------------------------------------------------------------------------

def _row_owner_wallet(tx: dict, fallback_wallet: str) -> str:
    m = tx.get("meta") if isinstance(tx.get("meta"), dict) else {}
    ow = (m.get("owner_wallet") or "").strip().lower()
    if ow:
        return ow
    return (fallback_wallet or "").strip().lower()


def _meta_owner_wallet(meta: Any, fallback: str) -> str:
    if isinstance(meta, dict):
        ow = (meta.get("owner_wallet") or "").strip().lower()
        if ow:
            return ow
    return (fallback or "").strip().lower()


# -------------------------------------------------------------
# Swap-Resolver v1 (Postprocessing pro tx_hash)
# -------------------------------------------------------------

def _group_swaps(items: List[ClassifiedItem], wallet: str) -> List[ClassifiedItem]:
    """
    Swap V2: aggregate ALL legs per tx_hash into ONE swap event.

    For EACH tx_hash:
    - Collect ALL token movements (direction in/out), ignoring:
        * internal transfers
        * zero/negative amounts
        * dust (negligible)
    - A tx is a swap if:
        * >=1 outgoing token
        * >=1 incoming token
        * and no dominant higher-level category exists in that tx:
          lp_*, pendle_*, restake_*, bridge_*, reward*
    - Remove ALL individual swap legs (no leftover receive/withdraw for those tokens)
    - Emit ONE unified swap event:
        category='swap', direction='swap', taxable=True
        meta includes full structure:
          {type:'swap', tokens_out:[...], tokens_in:[...], swap_tx_hash:...}
    """

    wl_fallback = (wallet or "").lower()
    by_tx: Dict[str, List[ClassifiedItem]] = defaultdict(list)
    for ci in items:
        by_tx[ci.tx_hash or ""].append(ci)

    DUST_EUR = 0.01
    DUST_AMOUNT = 1e-12

    def _is_dust(ci: ClassifiedItem) -> bool:
        eur = float(ci.eur_value or 0.0)
        amt = float(ci.amount or 0.0)
        if eur > 0:
            return eur < DUST_EUR
        return abs(amt) < DUST_AMOUNT

    def _is_swap_candidate(ci: ClassifiedItem) -> bool:
        if not ci:
            return False
        if (ci.amount or 0.0) <= 0:
            return False
        # ignore internal transfers
        if (ci.direction or "").lower() == "internal":
            return False
        if (ci.category or "").lower() in ("internal_transfer", "self_transfer"):
            return False
        if _is_dust(ci):
            return False
        # focus on transfer legs (most swaps show up as ERC20_TRANSFER)
        m = (ci.method or "").upper()
        if m == "ERC20_TRANSFER":
            return True
        # also allow generic legs, but still require in/out directions
        if (ci.category or "").lower() in ("receive", "withdraw", "transfer", "unknown", "swap"):
            return True
        return False

    def _score(ci: ClassifiedItem) -> float:
        # Prefer larger legs (eur_value first, then amount)
        eur = float(ci.eur_value or 0.0)
        amt = float(ci.amount or 0.0)
        return eur if eur > 0 else amt

    out_items: List[ClassifiedItem] = []
    # Debug summary metrics
    swap_txs = 0
    swap_legs_total = 0
    residual_legs = 0

    for txh, rows in by_tx.items():
        if not txh:
            out_items.extend(rows)
            continue

        # If this tx already has higher-level semantics, don't group into swap.
        cats = {(r.category or "").lower() for r in rows}
        if cats.intersection({
            "lp_add", "lp_remove",
            "pendle_deposit", "pendle_reward", "pendle_redeem",
            "restake_in", "restake_out",
            "bridge_in", "bridge_out",
            "reward", "staking_reward",
            "vault_exit",
        }):
            out_items.extend(rows)
            continue

        def _row_meta_protocol_restake(r: ClassifiedItem) -> bool:
            m = r.meta if isinstance(r.meta, dict) else {}
            if (m.get("cp_protocol") or "").lower() == "restake":
                return True
            return any(str(t).lower() == "restake" for t in (m.get("cp_tags") or []))

        if any(_row_meta_protocol_restake(r) for r in rows):
            print(f"[FIX_APPLIED] tx={txh} reason=restake_not_swap")
            out_items.extend(rows)
            continue

        outs = [r for r in rows if (r.direction or "").lower() == "out" and _is_swap_candidate(r)]
        ins = [r for r in rows if (r.direction or "").lower() == "in" and _is_swap_candidate(r)]

        if not outs or not ins:
            out_items.extend(rows)
            continue

        wl_tx = wl_fallback
        for r in rows:
            m = r.meta if isinstance(r.meta, dict) else {}
            ow = (m.get("owner_wallet") or "").strip().lower()
            if ow:
                wl_tx = ow
                break

        # Ensure this isn't a trivial self-transfer pattern (any leg)
        if any(is_self_transfer(wl_tx, r.from_addr, r.to_addr) for r in outs + ins):
            out_items.extend(rows)
            continue

        tokens_out = []
        tokens_in = []

        for r in outs:
            tok = (r.token or "").upper()
            if not tok:
                continue
            tokens_out.append({
                "token": tok,
                "amount": float(r.amount or 0.0),
                "eur_value": float(r.eur_value or 0.0),
            })
        for r in ins:
            tok = (r.token or "").upper()
            if not tok:
                continue
            tokens_in.append({
                "token": tok,
                "amount": float(r.amount or 0.0),
                "eur_value": float(r.eur_value or 0.0),
            })

        if not tokens_out or not tokens_in:
            out_items.extend(rows)
            continue

        swap_txs += 1
        swap_legs_total += (len(outs) + len(ins))

        total_out_value_eur = sum(float(x.get("eur_value") or 0.0) for x in tokens_out)
        total_in_value_eur = sum(float(x.get("eur_value") or 0.0) for x in tokens_in)

        # Remove ALL individual swap legs; keep everything else
        leg_set = set(id(x) for x in outs + ins)
        remaining = [r for r in rows if id(r) not in leg_set]

        # Choose representative fields for the single swap item
        out_leg = max(outs, key=_score)
        in_leg = max(ins, key=_score)

        swap_meta: Dict[str, Any] = dict(out_leg.meta or {})
        swap_meta.setdefault("owner_wallet", _meta_owner_wallet(out_leg.meta, wallet))
        swap_meta.update({
            "type": "swap",
            "tokens_out": tokens_out,
            "tokens_in": tokens_in,
            "total_out_value_eur": float(total_out_value_eur or 0.0),
            "total_in_value_eur": float(total_in_value_eur or 0.0),
            "swap_tx_hash": txh,
        })

        swap_item = ClassifiedItem(
            tx_hash=txh,
            dt_iso=out_leg.dt_iso or in_leg.dt_iso,
            token=(out_leg.token or "").upper() or "SWAP",
            amount=float(out_leg.amount or 0.0),
            eur_value=float(total_out_value_eur or out_leg.eur_value or 0.0),
            from_addr=out_leg.from_addr,
            to_addr=out_leg.to_addr,
            direction="swap",
            category="swap",
            method=out_leg.method or in_leg.method,
            source=out_leg.source,
            fee_token=out_leg.fee_token,
            fee_amount=out_leg.fee_amount,
            taxable=True,
            reason="Tausch (Swap) als ein wirtschaftliches Ereignis (alle Legs) gruppiert.",
            counterparty=out_leg.counterparty,
            note="",
            chain_id=out_leg.chain_id or in_leg.chain_id,
            meta=swap_meta,
        )

        # Residual legs inside swap tx (debug): any remaining generic ERC20 receive/withdraw
        for r in remaining:
            if (r.method or "").upper() == "ERC20_TRANSFER" and (r.category or "").lower() in ("receive", "withdraw"):
                residual_legs += 1

        out_items.extend(remaining)
        out_items.append(swap_item)

    avg_legs = (swap_legs_total / swap_txs) if swap_txs else 0.0
    print(f"[SWAP_V2] swap_txs={swap_txs} avg_legs_per_swap={avg_legs:.2f} residual_legs={residual_legs}")
    return out_items


# -------------------------------------------------------------
# evaluate_batch (final v5.0)
# -------------------------------------------------------------

def evaluate_batch(txs: List[dict], wallet: str):
    print(">>> EVALUATE BATCH REALLY RUNNING <<<")

    taxlogic = TaxLogic("de")
    result: List[ClassifiedItem] = []
    debug_info: Dict[str, Any] = {}

    wl_default = wallet.lower()

    for tx in txs:
        txh   = tx.get("tx_hash") or tx.get("hash") or ""
        dt    = tx.get("dt_iso") or tx.get("datetime") or ""
        token = (tx.get("token") or "").upper()
        amount = float(tx.get("amount") or 0.0)
        eur    = float(tx.get("eur_value") or 0.0)

        method = (
            tx.get("method")
            or tx.get("function_name")
            or tx.get("functionName")
            or tx.get("action")
            or tx.get("input_function")
            or tx.get("inputFunction")
            or tx.get("raw_method")
            or ""
        )

        source     = tx.get("source") or ""
        from_addr  = tx.get("from_addr") or tx.get("from") or ""
        to_addr    = tx.get("to_addr")   or tx.get("to")   or ""
        fee_token  = tx.get("fee_token")
        fee_amount = float(tx.get("fee_amount") or 0.0)
        meta       = tx.get("meta") if isinstance(tx.get("meta"), dict) else None
        chain_id   = tx.get("chain_id") or (meta or {}).get("chain_id", "") or ""

        wl_row = _row_owner_wallet(tx, wallet)

        if dt and "T" not in dt:
            dt = dt + "T00:00:00"

        dirn = (tx.get("direction") or "").lower()
        if not dirn or dirn == "unknown":
            dirn = derive_direction(wl_row, from_addr, to_addr)

        raw_cat  = (tx.get("category") or "").lower()
        base_cat = _basic_category(method, dirn)

        # Counterparty
        cp_addr, cp_info = _resolve_counterparty(wl_row, from_addr, to_addr, dirn)
        # Re-resolve with chain_id if available (improves labeling on L2s)
        if chain_id:
            cp_info = label_address(cp_addr, chain=chain_id) or cp_info or {}
        cp_label = cp_info.get("label")
        cp_type  = cp_info.get("type")

        # Final category
        category = _refine_category(
            base_category=base_cat,
            raw_category=raw_cat,
            method=method,
            direction=dirn,
            wallet=wl_row,
            from_addr=from_addr,
            to_addr=to_addr,
            cp_info=cp_info,
        )
        category = _normalize_category(category)

        cp_protocol = (cp_info.get("protocol") or "").strip().lower()
        if cp_protocol == "restake" and category == "swap":
            raise RuntimeError("Invalid classification: restake as swap")

        # Steuerlogik
        rule    = taxlogic.get_rule(category)
        taxable = bool(rule.get("taxable", False))
        reason  = taxlogic.describe(category)

        # Ensure meta carries counterparty info for analysis/backlog
        if meta is None:
            meta = {}
        if isinstance(meta, dict):
            meta.setdefault("owner_wallet", wl_row)
            meta.setdefault("cp_addr", cp_addr)
            meta.setdefault("cp_label", cp_label or "")
            meta.setdefault("cp_protocol", cp_info.get("protocol") or "")
            meta.setdefault("cp_type", cp_info.get("type") or "")
            meta.setdefault("cp_tags", cp_info.get("tags") or [])
            # Preserve token contract identity from raw loader rows for downstream valuation.
            token_contract = (tx.get("contract_addr") or "").strip().lower()
            if token_contract:
                meta.setdefault("token_contract", token_contract)

        ci = ClassifiedItem(
            tx_hash=txh,
            dt_iso=dt,
            token=token,
            amount=amount,
            eur_value=eur,
            from_addr=from_addr,
            to_addr=to_addr,
            direction=dirn,
            category=category,
            method=method,
            source=source,
            fee_token=fee_token,
            fee_amount=fee_amount,
            taxable=taxable,
            reason=reason,
            counterparty=cp_label or cp_type,
            note="",
            chain_id=chain_id,
            meta=meta,
        )

        result.append(ci)
        print("[DEBUG CLASSIFY]", method, "->", category)

    # 🔹 LP detection (strict): must run BEFORE swap grouping to avoid turning LP adds/removes into swaps.
    _postprocess_lp(result)

    # 🔹 Swap grouping: collapse in+out legs into ONE swap event per tx_hash
    result = _group_swaps(result, wl_default)
    _postprocess_pendle(result)   # 🔥 NEU: Pendle v1

    log(f"[CLASSIFY] {len(result)} Items klassifiziert.")
    return result, debug_info
    
# -------------------------------------------------------------
# LP-Resolver v1 (pro tx_hash)
# -------------------------------------------------------------

def _postprocess_lp(items: List[ClassifiedItem]) -> None:
    """
    Strict LP Add/Remove detection (tx_hash grouped).

    LP candidate signals:
    1) Method contains one of:
       - addLiquidity / addLiquidityETH / addLiquidityNATIVE
       - removeLiquidity / removeLiquidityETH / removeLiquidityNATIVE
    OR
    2) Token pattern within tx_hash:
       - LP add: >=2 distinct OUT tokens + >=1 IN token, and IN token(s) are not among OUT tokens
       - LP remove: >=1 OUT token + >=2 distinct IN tokens, and IN tokens differ from OUT token(s)

    Safety rules:
    - Do not classify if only 1 token involved overall
    - Do not classify if no method signal AND no clear token pattern
    - Do not classify if any involved token is UNKNOWN/empty
    - Do not classify if it looks like a normal swap (typically 1 OUT + 1 IN)

    We only upgrade categories in the "grey zone":
    transfer / withdraw / deposit / receive / unknown / erc20_transfer
    """
    by_tx: Dict[str, List[ClassifiedItem]] = defaultdict(list)
    for ci in items:
        by_tx[ci.tx_hash or ""].append(ci)

    for txh, rows in by_tx.items():
        if not txh:
            continue

        # Do not interfere with already-special tx semantics.
        existing_cats = {(r.category or "").lower() for r in rows}
        if existing_cats.intersection({
            "swap", "sell",
            "pendle_deposit", "pendle_reward", "pendle_redeem",
            "restake_in", "restake_out",
            "bridge_in", "bridge_out",
            "reward", "staking_reward",
        }):
            continue

        # Gather method signals (any leg).
        methods_join = " ".join((r.method or "").lower() for r in rows)
        has_add_method = any(x in methods_join for x in (
            "addliquidity", "addliquidityeth", "addliquiditynative",
            "add liquidity", "add liquidity eth", "add liquidity native",
        ))
        has_remove_method = any(x in methods_join for x in (
            "removeliquidity", "removeliquidityeth", "removeliquiditynative",
            "remove liquidity", "remove liquidity eth", "remove liquidity native",
        ))

        # Identify in/out token movements (ignore internal/zero/dusty unknown tokens).
        outs = [r for r in rows if (r.direction or "").lower() == "out" and float(r.amount or 0.0) > 0]
        ins = [r for r in rows if (r.direction or "").lower() == "in" and float(r.amount or 0.0) > 0]

        out_tokens = [(r.token or "").strip().upper() for r in outs]
        in_tokens = [(r.token or "").strip().upper() for r in ins]

        def _is_unknown(tok: str) -> bool:
            t = (tok or "").strip().upper()
            return (not t) or t in {"UNKNOWN", "<EMPTY>"}

        involved = [t for t in (out_tokens + in_tokens) if t]
        involved_set = {t for t in involved if not _is_unknown(t)}
        if len(involved_set) <= 1:
            continue
        if any(_is_unknown(t) for t in involved):
            # Deterministic rule: do not classify LP if tokens are unknown/empty
            continue

        out_set = {t for t in out_tokens if t}
        in_set = {t for t in in_tokens if t}

        def _looks_like_lp_token(sym: str) -> bool:
            s = (sym or "").upper()
            # Deterministic symbol heuristics for LP/vault receipt tokens (no on-chain guessing).
            return any(x in s for x in ("LP", "LPT", "UNI-V2", "SLP", "MOO", "BEEFY", "GMX"))

        def _looks_like_lp_receipt(sym: str) -> bool:
            """
            High-confidence LP receipt token pattern (spec).
            Reject ambiguous names (e.g. exactly 'PAIR').
            """
            s = (sym or "").strip().upper()
            if not s or s in {"UNKNOWN", "<EMPTY>"}:
                return False
            if s == "PAIR":
                return False
            # strong receipt patterns
            if "UNI-V2" in s or "SLP" in s or "CAKE-LP" in s:
                return True
            if "MOO" in s:
                return True
            # Generic LP / pair substrings: require non-trivial symbol length to reduce noise
            if "LP" in s and len(s) >= 4:
                return True
            if "PAIR" in s and len(s) >= 7:
                return True
            return False

        # Safety: reject normal swap-like patterns (1 OUT + 1 IN) ONLY if no LP receipt/burn evidence exists.
        if len(out_set) == 1 and len(in_set) == 1:
            out_tok_1 = next(iter(out_set))
            in_tok_1 = next(iter(in_set))
            has_lp_evidence = (
                (has_add_method and _looks_like_lp_receipt(in_tok_1) and in_tok_1 != out_tok_1)
                or (has_remove_method and _looks_like_lp_receipt(out_tok_1) and in_tok_1 != out_tok_1)
            )
            if not has_lp_evidence:
                continue

        # Pattern rules (conservative but practical):
        # LP add: >=2 distinct OUT tokens + >=1 IN token that is NOT among OUT tokens (LP receipt).
        # Allow extra IN tokens that overlap OUT tokens (refunds), but require at least one new IN token.
        in_new = (in_set - out_set)
        pattern_add = (len(out_set) >= 2) and (len(in_set) >= 1) and (len(in_new) >= 1)

        # LP remove: >=1 OUT token (LP token) + >=2 distinct IN tokens (underlyings), where IN tokens differ from OUT token(s).
        # Allow extra legs, but require at least two distinct IN tokens not equal to the LP token symbol.
        in_underlyings = (in_set - out_set)
        pattern_remove = (len(out_set) >= 1) and (len(in_set) >= 2) and (len(in_underlyings) >= 1)

        # No guessing: pattern-only classification requires an LP-like receipt/burn token symbol present on the expected side.
        has_lp_like_in = any(_looks_like_lp_token(t) for t in in_new)
        has_lp_like_out = any(_looks_like_lp_token(t) for t in out_set)

        is_lp_add = pattern_add and (has_add_method or (has_lp_like_in and not has_remove_method))
        is_lp_remove = pattern_remove and (has_remove_method or (has_lp_like_out and not has_add_method))

        # If both match, abort (ambiguous / could be complex routing).
        if is_lp_add and is_lp_remove:
            continue

        if not (is_lp_add or is_lp_remove):
            # No method signal AND no clear token pattern.
            continue

        target_cat = "lp_add" if is_lp_add else "lp_remove"

        # Apply only to grey-zone categories.
        for r in rows:
            cat = (r.category or "").lower()
            if cat in ("transfer", "withdraw", "receive", "deposit", "unknown", "erc20_transfer"):
                if can_override(r.category, target_cat):
                    r.category = target_cat

        # --------------------------
        # Single-sided LP detection (HIGH confidence only)
        # --------------------------
        # Recompute sets after potential modifications above (tokens/directions unchanged).
        out_set2 = out_set
        in_set2 = in_set

        # Single-sided LP add
        if (len(out_set2) == 1) and (len(in_set2) >= 1) and has_add_method and (not has_remove_method):
            out_tok = next(iter(out_set2))
            lp_in = [t for t in in_set2 if (t != out_tok) and _looks_like_lp_receipt(t)]
            if lp_in:
                lp_tok = sorted(lp_in)[0]
                print("[LP DETECTED]")
                print(f"tx={txh}")
                print("type=lp_add")
                # choose representative method
                mrep = ""
                for r in rows:
                    if "addliquidity" in (r.method or "").lower():
                        mrep = r.method
                        break
                print(f"method={mrep or (rows[0].method if rows else '')}")
                print(f"out_token={out_tok}")
                print(f"lp_token={lp_tok}")
                for r in rows:
                    cat = (r.category or "").lower()
                    if cat in ("transfer", "withdraw", "receive", "deposit", "unknown", "erc20_transfer"):
                        if can_override(r.category, "lp_add"):
                            r.category = "lp_add"
                continue

        # Single-sided LP remove
        if (len(out_set2) == 1) and (len(in_set2) >= 1) and has_remove_method and (not has_add_method):
            out_tok = next(iter(out_set2))
            if _looks_like_lp_receipt(out_tok):
                in_ok = [t for t in in_set2 if t != out_tok and not _is_unknown(t)]
                if in_ok:
                    lp_tok = out_tok
                    print("[LP DETECTED]")
                    print(f"tx={txh}")
                    print("type=lp_remove")
                    mrep = ""
                    for r in rows:
                        if "removeliquidity" in (r.method or "").lower():
                            mrep = r.method
                            break
                    print(f"method={mrep or (rows[0].method if rows else '')}")
                    print(f"out_token={out_tok}")
                    print(f"lp_token={lp_tok}")
                    for r in rows:
                        cat = (r.category or "").lower()
                        if cat in ("transfer", "withdraw", "receive", "deposit", "unknown", "erc20_transfer"):
                            if can_override(r.category, "lp_remove"):
                                r.category = "lp_remove"

# -------------------------------------------------------------
# Pendle-Resolver v1 (PENDLE-LPT Redeems)
# -------------------------------------------------------------

def _postprocess_pendle(items: List[ClassifiedItem]) -> None:
    """
    Erkennung von Pendle-Redeems auf Basis von PENDLE-LPT-Token-Bewegungen.
    Strategie v1 (konservativ):

    - Wenn token == "PENDLE-LPT" UND direction == "out"
      UND Kategorie nur generisch ist (transfer / withdraw / swap / unknown / erc20_transfer),
      dann → category = "pendle_redeem".

    Wir fassen NICHT an:
    - pendle_deposit
    - pendle_reward
    - restake / bridge / lp / reward
    """

    for r in items:
        token = (r.token or "").upper()
        if token not in {"PENDLE-LPT", "PENDLE_LPT"}:
            continue

        if r.direction != "out":
            continue

        cat = (r.category or "").lower()
        m   = (r.method or "").lower()

        # Bereits spezielle Kategorien -> nicht anfassen
        if cat in (
            "pendle_deposit", "pendle_reward", "pendle_redeem",
            "lp_add", "lp_remove",
            "restake_in", "restake_out",
            "bridge_in", "bridge_out",
            "reward", "staking_reward",
            "swap", "sell"
        ):
            continue

        # Nur "graue" Kategorien anfassen
        if cat in ("transfer", "withdraw", "receive", "deposit", "unknown", "erc20_transfer"):
            if can_override(r.category, "pendle_redeem"):
                r.category = "pendle_redeem"
            # Optionales Debug-Log:
            # print(f"[PENDLE-RESOLVER] {r.tx_hash}: {cat} -> pendle_redeem ({r.token}, {r.amount})")
