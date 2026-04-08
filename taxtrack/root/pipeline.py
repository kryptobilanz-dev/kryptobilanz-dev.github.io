# taxtrack/root/pipeline.py
"""
Unified tax pipeline. All runners (run_reference, run_wallet, run_customer) call run_pipeline().

Steps:
  1. Load transactions (from wallet_data)
  2. Normalize rows (to dicts, year filter)
  3. Classify transactions (evaluate_batch)
  4. Resolve prices (fee_eur, then eur_value where needed)
  5. Compute eur_value (base tokens, LP/vault mint)
  6. FIFO gain calculation (compute_gains)
  7. Economic grouping (group_gains_economic)
  8. Resolve vault exits (apply_vault_exits, §23 cleanup)
  9. Apply tax logic (fees_eur, net_pnl_eur, reward eur_value, USD fallback)
  10. Compute rewards (group_rewards)
  11. Generate report (PDF + audit CSV if config specifies output)
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from taxtrack.loaders.auto_detect import load_auto
from taxtrack.rules.evaluate import evaluate_batch
from taxtrack.validation.raw_row import validate_raw_rows
from taxtrack.validation.harvest_consistency import validate_consistency_lists
from taxtrack.analyze.gains import compute_gains
from taxtrack.analyze.gain_grouping import group_gains_economic
from taxtrack.analyze.reward_grouping import group_rewards
from taxtrack.analyze.vault_exit_resolver import apply_vault_exits
from taxtrack.prices import PriceQuery, resolve_prices_batch
from taxtrack.prices.token_mapper import TOKEN_MAP, map_token
from taxtrack.pdf.pdf_report import build_pdf
from taxtrack.pdf.audit_export import write_audit_json
from taxtrack.pdf.audit_validation import (
    confidence_distribution,
    top_problem_tokens,
    unresolved_tx_hashes,
    validate_tax_ready_audit,
)
from taxtrack.tax.jurisdictions import get_jurisdiction


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

# Each item: wallet (address), chain_id, base_dir (path to folder containing normal.csv, erc20.csv, internal.csv)
WalletDataItem = Dict[str, Any]

PipelineConfig = Dict[str, Any]
# Expected keys: output_dir (Path, optional), report_label (str, optional), primary_wallet (str, optional), debug (bool)

PipelineResult = Dict[str, Any]
# Keys: economic_gains, classified_dicts, gains, totals, reward_summary, debug_info


def _row_timestamp(r: Any) -> int:
    if hasattr(r, "timestamp"):
        return int(getattr(r, "timestamp") or 0)
    if isinstance(r, dict):
        return int(r.get("timestamp", 0) or 0)
    return 0


def _row_to_dict(r: Any) -> Dict[str, Any]:
    if hasattr(r, "to_dict"):
        return r.to_dict()
    if isinstance(r, dict):
        return r
    return {}


def _tag_owner_wallet(row: Any, wallet_lower: str) -> None:
    """Jede Rohzeile kennt ihre Quell-Wallet (Multi-Wallet / run_customer)."""
    if not wallet_lower:
        return
    if not hasattr(row, "meta"):
        return
    m = row.meta
    if not isinstance(m, dict):
        m = {}
    m = dict(m)
    m.setdefault("owner_wallet", wallet_lower)
    row.meta = m


def _row_chain_id(r: Any) -> str:
    """Extract chain_id from a raw row (RawRow or dict) for ingestion stats."""
    if hasattr(r, "chain_id"):
        return (getattr(r, "chain_id") or "") or ""
    if isinstance(r, dict):
        return (r.get("chain_id") or "") or ""
    return ""


def _load_maps_to_index() -> Dict[Tuple[str, str], str]:
    """
    Build (chain_id, cp_addr) -> maps_to index from address_map.json.
    Applies safety: maps_to is used only when confidence >= 0.7 (if provided).
    """
    out: Dict[Tuple[str, str], str] = {}
    p = Path(__file__).resolve().parents[1] / "data" / "config" / "address_map.json"
    if not p.exists():
        return out
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return out
    if not isinstance(data, dict):
        return out
    for chain_id, items in data.items():
        if not isinstance(items, dict):
            continue
        ch = (chain_id or "").strip().lower()
        for addr, meta in items.items():
            if not isinstance(addr, str) or not isinstance(meta, dict):
                continue
            maps_to = (meta.get("maps_to") or "").strip().upper()
            if not maps_to:
                continue
            conf = meta.get("confidence")
            if conf is not None:
                try:
                    if float(conf) < 0.7:
                        continue
                except Exception:
                    continue
            out[(ch, addr.strip().lower())] = maps_to
    return out


def _print_ingestion_report(
    primary_wallet: str,
    tax_year: int,
    wallet_data: List[WalletDataItem],
    per_chain_loaded: Dict[str, int],
    per_chain_filtered: Dict[str, int],
    total_filtered: int,
    chain_csv_source: Dict[str, str],
    rows_validated: Optional[int] = None,
) -> None:
    """Print a lightweight wallet ingestion status report to stdout."""
    chain_ids = [item.get("chain_id") or "" for item in wallet_data if item.get("chain_id")]
    print("\n--- Wallet analysis summary ---")
    print(f"Wallet: {primary_wallet}")
    print(f"Year: {tax_year}")
    print()
    for chain_id in chain_ids:
        loaded = per_chain_loaded.get(chain_id, 0)
        filtered = per_chain_filtered.get(chain_id, 0)
        source = chain_csv_source.get(chain_id) or "existing files"
        print(f"Chain: {chain_id}")
        print(f"  CSV source: {source}")
        print(f"  rows loaded: {loaded:,}")
        print(f"  rows after year filter: {filtered:,}")
        print()
    print(f"Total rows entering classification: {total_filtered:,}")
    if rows_validated is not None:
        print(f"Rows validated: {rows_validated:,}")
    print("--------------------------------\n")


# ---------------------------------------------------------------------------
# 1. Load transactions
# ---------------------------------------------------------------------------

def _load_transactions(wallet_data: List[WalletDataItem], tax_year: int) -> Tuple[List[Any], List[Dict[str, Any]]]:
    """Load from wallet_data. Each item: wallet, chain_id, and either base_dir (folder with normal/erc20/internal.csv)
    or files (list of paths). Apply year filter, return raw_rows and filtered dicts."""
    raw_rows: List[Any] = []
    for item in wallet_data:
        wallet = item.get("wallet") or ""
        wnorm = (wallet or "").strip().lower()
        chain_id = item.get("chain_id") or "eth"
        files = item.get("files")
        if files is not None:
            for path in files:
                path = Path(path)
                if not path.exists():
                    continue
                try:
                    rows = load_auto(path, wallet=wallet, chain_id=chain_id)
                    for row in rows:
                        _tag_owner_wallet(row, wnorm)
                    raw_rows.extend(rows)
                except Exception as e:
                    print(f"[PIPELINE] Load failed {path}: {e}")
        else:
            base_dir = item.get("base_dir")
            if not base_dir:
                continue
            base_dir = Path(base_dir)
            for name in ("normal.csv", "erc20.csv", "internal.csv"):
                path = base_dir / name
                if not path.exists():
                    continue
                try:
                    rows = load_auto(path, wallet=wallet, chain_id=chain_id)
                    for row in rows:
                        _tag_owner_wallet(row, wnorm)
                    raw_rows.extend(rows)
                except Exception as e:
                    print(f"[PIPELINE] Load failed {path}: {e}")

    ts_from = int(datetime(tax_year, 1, 1).timestamp())
    ts_to = int(datetime(tax_year + 1, 1, 1).timestamp())
    filtered = [r for r in raw_rows if ts_from <= _row_timestamp(r) < ts_to]
    filtered_dicts = [_row_to_dict(r) for r in filtered]
    # Ensure top-level chain_id for every row (loaders may put it only in meta)
    for r in filtered_dicts:
        if not r.get("chain_id") and isinstance(r.get("meta"), dict):
            r["chain_id"] = r["meta"].get("chain_id", "") or ""
        r.setdefault("chain_id", "")
    return raw_rows, filtered_dicts


# ---------------------------------------------------------------------------
# 2. Normalize rows (ensure dicts; year filter already done in load)
# ---------------------------------------------------------------------------
# Handled inside _load_transactions. No extra step.

# ---------------------------------------------------------------------------
# 3. Classify + 4. Resolve prices (fee_eur) + 5. eur_value (base + LP/vault)
# ---------------------------------------------------------------------------

PRICE_TOKENS = {"ETH", "WETH", "USDC", "USDT", "DAI", "ARB", "OP", "ZRO"}
REWARD_CATEGORIES = {
    "reward", "staking_reward", "vault_reward", "pendle_reward",
    "restake_reward", "airdrop", "learning_reward", "earn_reward",
}
# Tokens that must never be queried via direct pricing providers.
# They are valued only through flow/swap derivation paths.
NO_DIRECT_PRICE_TOKENS = {"RENZO"}

PRICE_CONFIDENCE_COLOR = {
    "high": "GREEN",
    "medium": "YELLOW",
    "low": "RED",
}


def _dt_iso_to_ts(dt_iso: str) -> int:
    if not dt_iso:
        return 0
    try:
        return int(datetime.fromisoformat(dt_iso.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def _collect_price_queries_from_classified(classified: List[Any]) -> List[PriceQuery]:
    """Collect unique (symbol, ts, chain) as PriceQuery for batch resolution."""
    maps_to_index = _load_maps_to_index()

    def resolve_effective_token(row_like: Any, token_symbol: str) -> Tuple[str, str]:
        """
        Centralized token resolution for valuation.
        1) cp_addr + chain_id maps_to (confidence-gated) when safe
        2) fallback to original token
        3) normalize through existing map_token
        Returns: (effective_token, source)
        """
        raw = (token_symbol or "").strip().upper()
        chain = (getattr(row_like, "chain_id", None) or "").strip().lower()
        meta = getattr(row_like, "meta", None)
        cp_addr = ""
        token_contract = ""
        if isinstance(meta, dict):
            cp_addr = (meta.get("cp_addr") or "").strip().lower()
            token_contract = (
                meta.get("token_contract")
                or meta.get("contract_addr")
                or meta.get("contract_address")
                or meta.get("token_address")
                or ""
            )
            token_contract = str(token_contract).strip().lower()

        # Prefer token contract identity over counterparty address.
        mapped_from_contract = None
        if token_contract:
            mapped_from_contract = maps_to_index.get((chain, token_contract))
        if not mapped_from_contract and cp_addr:
            mapped_from_contract = maps_to_index.get((chain, cp_addr))
        if mapped_from_contract:
            # Safety: avoid overriding explicit known symbols to conflicting assets.
            # If token is explicit/known and conflicts, keep token fallback.
            if raw in TOKEN_MAP or raw in PRICE_TOKENS:
                canonical_raw = map_token(raw)
                canonical_contract = map_token(mapped_from_contract)
                if canonical_raw != canonical_contract:
                    return map_token(raw), "fallback"
            return map_token(mapped_from_contract), "maps_to"

        return map_token(raw), "fallback"

    seen: set = set()
    queries: List[PriceQuery] = []
    for it in classified:
        chain = getattr(it, "chain_id", None) or ""
        dt_iso = getattr(it, "dt_iso", None) or ""
        ts = _dt_iso_to_ts(dt_iso)
        tx_hash = getattr(it, "tx_hash", "") or ""
        meta = getattr(it, "meta", None)
        if not isinstance(meta, dict):
            meta = {}
            try:
                setattr(it, "meta", meta)
            except Exception:
                pass
        # Base token eur_value
        tok = (getattr(it, "token", None) or "").upper()
        tok_eff, tok_src = resolve_effective_token(it, tok)
        if tok_eff != tok:
            print(f"[TOKEN RESOLVED] tx={tx_hash} original={tok} resolved={tok_eff} source={tok_src}")
        if isinstance(meta, dict):
            meta["effective_token"] = tok_eff
            meta["effective_token_source"] = tok_src
        eur = float(getattr(it, "eur_value", 0.0) or 0.0)
        amt = abs(float(getattr(it, "amount", 0.0) or 0.0))

        # Swap legs need their own price keys: the row token is often only the out-leg
        # (e.g. vault receipt), while the in-leg (EZETH, WETH, …) must still be quoted
        # or total_in stays 0 and derivation never runs.
        cat = (getattr(it, "category", "") or "").lower()
        dirn = (getattr(it, "direction", "") or "").lower()
        if cat == "swap" and dirn == "swap" and isinstance(meta, dict):
            for side in ("tokens_in", "tokens_out"):
                legs = meta.get(side) or []
                if not isinstance(legs, list):
                    continue
                for leg in legs:
                    if not isinstance(leg, dict):
                        continue
                    lt = (leg.get("token") or "").strip().upper()
                    if not lt or lt in ("UNKNOWN", "ERC-20"):
                        continue
                    tok_eff_leg, _ = resolve_effective_token(it, lt)
                    if tok_eff_leg in NO_DIRECT_PRICE_TOKENS or lt in NO_DIRECT_PRICE_TOKENS:
                        continue
                    la = abs(float(leg.get("amount") or 0.0))
                    if la > 0 and ts > 0 and tok_eff_leg and tok_eff_leg not in {"", "UNKNOWN"}:
                        key = ("swap_leg", tok_eff_leg, ts, chain)
                        if key not in seen:
                            seen.add(key)
                            queries.append(PriceQuery(symbol=tok_eff_leg, ts=ts, chain=chain))

        # Hard price control: selected tokens must not use direct price lookup.
        if tok in NO_DIRECT_PRICE_TOKENS or tok_eff in NO_DIRECT_PRICE_TOKENS:
            if isinstance(meta, dict):
                meta["direct_price_blocked"] = True
                meta["direct_price_blocked_reason"] = "no_direct_price_token"
            continue

        # Query broadly for any deterministically identified token.
        # Keep UNKNOWN / empty out of the provider path.
        if eur <= 0 and amt > 0 and ts > 0 and tok_eff not in {"", "UNKNOWN", "ERC-20"}:
            key = ("base", tok_eff, ts, chain)
            if key not in seen:
                seen.add(key)
                queries.append(PriceQuery(symbol=tok_eff, ts=ts, chain=chain))
        # Fee
        fee_tok = (getattr(it, "fee_token", None) or "").upper()
        fee_amt = float(getattr(it, "fee_amount", 0.0) or 0.0)
        if fee_amt > 0 and fee_tok and ts > 0:
            key = ("fee", fee_tok, ts, chain)
            if key not in seen:
                seen.add(key)
                queries.append(PriceQuery(symbol=fee_tok, ts=ts, chain=chain))
        # Reward (category in REWARD_CATEGORIES, need eur_value)
        if cat in REWARD_CATEGORIES and amt > 0 and tok_eff and ts > 0 and eur <= 0:
            key = ("reward", tok_eff, ts, chain)
            if key not in seen:
                seen.add(key)
                queries.append(PriceQuery(symbol=tok_eff, ts=ts, chain=chain))
    return queries


def _build_price_map(queries: List[PriceQuery], results: List[Dict[str, Any]]) -> Dict[Tuple[str, int], float]:
    """Map (normalized_symbol, ts) -> price from batch results. Missing prices are omitted."""
    price_map: Dict[Tuple[str, int], float] = {}
    for q, res in zip(queries, results):
        if not res or "price" not in res:
            continue
        sym = map_token(q.symbol)
        raw_price = res.get("price")
        if raw_price is None:
            print(f"[PRICE WARN] missing price for {sym} ts={q.ts} chain={getattr(q, 'chain', '')}")
            continue
        price = float(raw_price)
        if price <= 0:
            # treat non-positive as missing in the valuation layer
            print(f"[PRICE WARN] non-positive price for {sym} ts={q.ts} -> {price} (treated as missing)")
            continue
        key = (sym, q.ts)
        if key not in price_map:
            price_map[key] = price
    return price_map


def _fee_eur_on_classified_dicts(rows: List[Dict[str, Any]], price_map: Dict[Tuple[str, int], float]) -> None:
    for tx in rows:
        fee_amt = float(tx.get("fee_amount") or 0.0)
        fee_tok = (tx.get("fee_token") or "").upper()
        dt_iso = tx.get("dt_iso") or ""
        chain = tx.get("chain_id") or ""
        if fee_amt > 0 and fee_tok and dt_iso:
            ts = _dt_iso_to_ts(dt_iso)
            price = price_map.get((map_token(fee_tok), ts))
            if price is None:
                print(f"[PRICE WARN] missing fee price {fee_tok} ts={ts} chain={chain}")
                tx["fee_eur"] = 0.0
            else:
                tx["fee_eur"] = fee_amt * float(price)
        else:
            tx["fee_eur"] = 0.0


def _classified_items_tx_valuation_missing(classified: List[Any], tx_hash: str) -> bool:
    """True if any ClassifiedItem for this tx has meta.valuation_missing (skip derived leg pricing / recovery)."""
    k = (tx_hash or "").strip().lower()
    if not k:
        return False
    for it in classified:
        if (getattr(it, "tx_hash", "") or "").strip().lower() != k:
            continue
        m = getattr(it, "meta", None)
        if isinstance(m, dict) and m.get("valuation_missing"):
            return True
    return False


def _fill_base_token_eur_value(classified: List[Any], price_map: Dict[Tuple[str, int], float]) -> None:
    def _ensure_meta(it: Any) -> Dict[str, Any]:
        meta = getattr(it, "meta", None)
        if not isinstance(meta, dict):
            meta = {}
            try:
                setattr(it, "meta", meta)
            except Exception:
                pass
        return meta

    def _set_confidence(meta: Dict[str, Any], level: str) -> None:
        meta["price_confidence"] = level
        meta["price_confidence_color"] = PRICE_CONFIDENCE_COLOR[level]

    for it in classified:
        try:
            meta = _ensure_meta(it)
            if meta.get("valuation_missing"):
                continue
            if _classified_items_tx_valuation_missing(classified, getattr(it, "tx_hash", "")):
                continue

            tok = (it.token or "").upper()
            amt = abs(float(it.amount or 0.0))
            eur = float(it.eur_value or 0.0)
            ts = _dt_iso_to_ts(it.dt_iso or "")
            chain = getattr(it, "chain_id", None) or ""
            cat = (getattr(it, "category", "") or "").lower()
            dirn = (getattr(it, "direction", "") or "").lower()
            effective_tok = (meta.get("effective_token") or "").upper() or map_token(tok)

            # USD normalization anchor:
            # Must bypass price tokens whitelist and always resolve to 1.0 EUR-equivalent.
            if effective_tok == "USD":
                if amt > 0:
                    print("[PRICE BASE] USD base currency applied (1.0)")
                    it.eur_value = float(amt) * 1.0
                    _set_confidence(meta, "high")
                else:
                    _set_confidence(meta, "low")
                continue

            # Swap V2 valuation: compute per-leg eur_value when possible.
            if cat == "swap" and dirn == "swap" and isinstance(getattr(it, "meta", None), dict):
                meta = _ensure_meta(it)
                tx_hash = getattr(it, "tx_hash", "")
                tokens_out = meta.get("tokens_out") or []
                tokens_in = meta.get("tokens_in") or []

                def _leg_value(leg: dict) -> float | None:
                    t = (leg.get("token") or "").upper()
                    a = abs(float(leg.get("amount") or 0.0))
                    if not t or a <= 0:
                        return None
                    t_eff = map_token(t)
                    # If the swap leg token equals the row token, prefer pre-resolved row identity.
                    if t == tok and effective_tok:
                        t_eff = effective_tok
                    # USD is a normalization anchor (fixed 1.0 EUR-equivalent).
                    # Must bypass price_map and fallback logic.
                    if t_eff == "USD":
                        print("[PRICE BASE] USD base currency applied (1.0)")
                        return a * 1.0
                    p = price_map.get((t_eff, ts))
                    if p is None:
                        print(f"[PRICE WARN] missing price for {t} in swap tx={getattr(it,'tx_hash','')} ts={ts} chain={chain}")
                        return None
                    return a * float(p)

                def _derive_missing_leg_values(legs: List[dict], target_total_eur: float, tx_hash: str, side: str) -> int:
                    if target_total_eur <= 0:
                        return 0
                    candidates: List[dict] = []
                    amount_sum = 0.0
                    for leg in legs:
                        try:
                            a = abs(float(leg.get("amount") or 0.0))
                        except Exception:
                            a = 0.0
                        if a <= 0:
                            continue
                        cur = leg.get("eur_value")
                        try:
                            cur_eur = float(cur) if cur is not None else 0.0
                        except Exception:
                            cur_eur = 0.0
                        # Never override existing valid leg valuation.
                        if cur_eur > 0:
                            continue
                        candidates.append(leg)
                        amount_sum += a
                    if amount_sum <= 0:
                        return 0
                    derived = 0
                    for leg in candidates:
                        a = abs(float(leg.get("amount") or 0.0))
                        v = target_total_eur * (a / amount_sum)
                        leg["eur_value"] = float(v)
                        leg["price_confidence"] = "medium"
                        leg["price_confidence_color"] = PRICE_CONFIDENCE_COLOR["medium"]
                        print(
                            f"[PRICE DERIVED] tx={tx_hash} token={(leg.get('token') or '').upper()} "
                            f"value={float(v):.10f} source=swap side={side}"
                        )
                        derived += 1
                    return derived

                out_vals = []
                for leg in tokens_out if isinstance(tokens_out, list) else []:
                    v = _leg_value(leg)
                    if v is not None and v > 0:
                        leg["eur_value"] = float(v)
                        leg["price_confidence"] = "high"
                        leg["price_confidence_color"] = PRICE_CONFIDENCE_COLOR["high"]
                        out_vals.append(float(v))
                    else:
                        leg["eur_value"] = None
                        leg["price_confidence"] = "low"
                        leg["price_confidence_color"] = PRICE_CONFIDENCE_COLOR["low"]

                in_vals = []
                for leg in tokens_in if isinstance(tokens_in, list) else []:
                    v = _leg_value(leg)
                    if v is not None and v > 0:
                        leg["eur_value"] = float(v)
                        leg["price_confidence"] = "high"
                        leg["price_confidence_color"] = PRICE_CONFIDENCE_COLOR["high"]
                        in_vals.append(float(v))
                    else:
                        leg["eur_value"] = None
                        leg["price_confidence"] = "low"
                        leg["price_confidence_color"] = PRICE_CONFIDENCE_COLOR["low"]

                total_out = sum(out_vals)
                total_in = sum(in_vals)

                # CASE A/B: leg derivation from opposite side (never when tx flagged valuation_missing).
                if not _classified_items_tx_valuation_missing(classified, tx_hash):
                    # CASE A: token_out missing, token_in priced -> derive out leg values.
                    if total_out <= 0 and total_in > 0 and isinstance(tokens_out, list):
                        _derive_missing_leg_values(tokens_out, total_in, tx_hash, "out")
                        total_out = sum(float((leg.get("eur_value") or 0.0)) for leg in tokens_out)
                    # CASE B: token_in missing, token_out priced -> derive in leg values.
                    if total_in <= 0 and total_out > 0 and isinstance(tokens_in, list):
                        _derive_missing_leg_values(tokens_in, total_out, tx_hash, "in")
                        total_in = sum(float((leg.get("eur_value") or 0.0)) for leg in tokens_in)

                    meta["total_out_value_eur"] = float(total_out) if total_out > 0 else None
                    meta["total_in_value_eur"] = float(total_in) if total_in > 0 else None

                    # If one side is valued, derive the other side total for neutrality.
                    if (meta.get("total_out_value_eur") is None) and (meta.get("total_in_value_eur") is not None):
                        meta["total_out_value_eur"] = meta["total_in_value_eur"]
                        print(f"[PRICE WARN] derived swap out total from in total tx={getattr(it,'tx_hash','')}: {meta['total_out_value_eur']}")
                    if (meta.get("total_in_value_eur") is None) and (meta.get("total_out_value_eur") is not None):
                        meta["total_in_value_eur"] = meta["total_out_value_eur"]
                        print(f"[PRICE WARN] derived swap in total from out total tx={getattr(it,'tx_hash','')}: {meta['total_in_value_eur']}")

                    # Consistency check when both sides exist
                    if meta.get("total_out_value_eur") is not None and meta.get("total_in_value_eur") is not None:
                        try:
                            outv = float(meta["total_out_value_eur"])
                            inv = float(meta["total_in_value_eur"])
                            denom = max(outv, inv, 1e-9)
                            diff = abs(outv - inv) / denom
                            if diff > 0.10:
                                print(f"[PRICE CHECK] swap imbalance tx={getattr(it,'tx_hash','')} out={outv:.2f} in={inv:.2f} diff={diff:.2%}")
                        except Exception:
                            pass

                    # Set swap item's eur_value to total_out if known (so proceeds basis is available)
                    if eur <= 0 and meta.get("total_out_value_eur") is not None:
                        it.eur_value = float(meta["total_out_value_eur"])
                    # Mark valuation_missing if neither side could be valued
                    if meta.get("total_out_value_eur") is None and meta.get("total_in_value_eur") is None:
                        meta["valuation_missing"] = True
                        _set_confidence(meta, "low")
                        print(f"[PRICE ERROR] full valuation missing for swap tx={getattr(it,'tx_hash','')}")
                    else:
                        # Medium if any leg was derived, else high for fully direct-valued swaps.
                        any_medium = any(
                            (leg.get("price_confidence") == "medium")
                            for leg in (tokens_out if isinstance(tokens_out, list) else [])
                        ) or any(
                            (leg.get("price_confidence") == "medium")
                            for leg in (tokens_in if isinstance(tokens_in, list) else [])
                        )
                        _set_confidence(meta, "medium" if any_medium else "high")
                continue

            # Non-swap valuation for supported price tokens
            if eur > 0 or amt <= 0 or effective_tok in {"", "UNKNOWN", "ERC-20"}:
                if eur > 0:
                    _set_confidence(meta, "high")
                elif amt > 0:
                    _set_confidence(meta, "low")
                continue
            # USD is a normalization anchor: never go through fallback/provider/cache.
            if effective_tok == "USD":
                print("[PRICE BASE] USD base currency applied (1.0)")
                it.eur_value = float(amt) * 1.0
                _set_confidence(meta, "high")
                continue
            price = price_map.get((effective_tok, ts))
            print(f"[VALUE CALC] {tok} amount={amt} timestamp={ts} chain={chain}")
            if price is None:
                print(f"[PRICE WARN] missing price for {tok} ts={ts} chain={chain}")
                _set_confidence(meta, "low")
                continue
            it.eur_value = amt * float(price)
            _set_confidence(meta, "high")
        except Exception:
            pass


def _lp_vault_mint_eur_value(classified: List[Any]) -> None:
    def is_vault_like(tok: str) -> bool:
        t = (tok or "").upper()
        return any(x in t for x in ("MOO", "BEEFY", "RCOW", "CAMELOT", "RAMSES", "LP", "LPT"))

    def is_base_like(tok: str) -> bool:
        return (tok or "").upper() in {"ETH", "WETH", "USDC", "USDT", "DAI", "ARB", "OP", "ZRO"}

    by_tx = defaultdict(list)
    for it in classified:
        if it.tx_hash:
            by_tx[it.tx_hash].append(it)

    for txh, rows in by_tx.items():
        funding_out = [
            r for r in rows
            if r.direction == "out"
            and is_base_like((r.token or "").upper())
            and float(r.eur_value or 0.0) > 0
        ]
        if not funding_out:
            continue
        funding_eur = sum(float(r.eur_value) for r in funding_out)
        vault_ins = [
            r for r in rows
            if r.direction == "in" and is_vault_like((r.token or "").upper())
        ]
        if not vault_ins:
            continue
        total_amt = sum(abs(float(r.amount or 0.0)) for r in vault_ins)
        if total_amt <= 0:
            continue
        for r in vault_ins:
            amt = abs(float(r.amount or 0.0))
            if amt <= 0:
                continue
            r.eur_value = funding_eur * (amt / total_amt)


def _reward_eur_value(classified_dicts: List[Dict[str, Any]], price_map: Dict[Tuple[str, int], float]) -> None:
    for r in classified_dicts:
        cat = (r.get("category") or "").lower()
        if cat not in REWARD_CATEGORIES:
            continue
        amount = float(r.get("amount") or 0.0)
        token = (r.get("token") or "").upper()
        dt_iso = r.get("dt_iso") or ""
        ts = _dt_iso_to_ts(dt_iso) if dt_iso else int(r.get("timestamp") or 0)
        if amount <= 0 or not token or ts <= 0 or r.get("eur_value"):
            continue
        chain = r.get("chain_id") or ""
        price = price_map.get((map_token(token), ts), 0.0)
        print(f"[VALUE CALC] {token} amount={amount} timestamp={ts} chain={chain}")
        r["eur_value"] = amount * price


def _swap_recovery_metrics(classified: List[Any]) -> Dict[str, float]:
    eligible = 0
    priced = 0
    for it in classified:
        cat = (getattr(it, "category", "") or "").lower()
        amt = abs(float(getattr(it, "amount", 0.0) or 0.0))
        if cat != "swap" or amt <= 0:
            continue
        eligible += 1
        eur = float(getattr(it, "eur_value", 0.0) or 0.0)
        if eur > 0:
            priced += 1
    coverage_pct = (priced / eligible * 100.0) if eligible else 100.0
    return {
        "eligible_rows": float(eligible),
        "priced_rows": float(priced),
        "coverage_pct": float(coverage_pct),
    }


def _recover_swap_missing_values(classified: List[Any]) -> Dict[str, float]:
    """
    Recover missing swap-side EUR values from the opposite side (no external pricing).

    Rules:
      CASE A: out has EUR, in missing -> set in total EUR = out total EUR; distribute by amount
      CASE B: in has EUR, out missing -> set out total EUR = in total EUR; distribute by amount
      CASE C: both missing -> do nothing

    Safety:
      - never overwrite existing eur_value > 0
      - apply only to swap rows
    """
    by_tx: Dict[str, List[Any]] = defaultdict(list)
    for it in classified:
        txh = (getattr(it, "tx_hash", "") or "").strip()
        if txh:
            by_tx[txh].append(it)

    recovered_rows = 0
    recovered_eur_volume = 0.0

    for txh, rows in by_tx.items():
        if _classified_items_tx_valuation_missing(classified, txh):
            continue

        swap_rows = [r for r in rows if (getattr(r, "category", "") or "").lower() == "swap"]
        if not swap_rows:
            continue

        ins = [r for r in swap_rows if (getattr(r, "direction", "") or "").lower() in {"in", "buy"}]
        outs = [r for r in swap_rows if (getattr(r, "direction", "") or "").lower() in {"out", "sell"}]
        if not ins or not outs:
            continue

        in_known = sum(float(getattr(r, "eur_value", 0.0) or 0.0) for r in ins if float(getattr(r, "eur_value", 0.0) or 0.0) > 0)
        out_known = sum(float(getattr(r, "eur_value", 0.0) or 0.0) for r in outs if float(getattr(r, "eur_value", 0.0) or 0.0) > 0)

        in_missing = [r for r in ins if float(getattr(r, "eur_value", 0.0) or 0.0) <= 0 and abs(float(getattr(r, "amount", 0.0) or 0.0)) > 0]
        out_missing = [r for r in outs if float(getattr(r, "eur_value", 0.0) or 0.0) <= 0 and abs(float(getattr(r, "amount", 0.0) or 0.0)) > 0]

        # CASE A: out known, in missing
        if out_known > 0 and in_missing:
            amt_sum = sum(abs(float(getattr(r, "amount", 0.0) or 0.0)) for r in in_missing)
            if amt_sum > 0:
                for r in in_missing:
                    share = abs(float(getattr(r, "amount", 0.0) or 0.0)) / amt_sum
                    rec = float(out_known) * share
                    if rec <= 0:
                        continue
                    r.eur_value = rec
                    meta = getattr(r, "meta", None)
                    if not isinstance(meta, dict):
                        meta = {}
                        try:
                            setattr(r, "meta", meta)
                        except Exception:
                            pass
                    if isinstance(meta, dict):
                        meta["price_source"] = "recovered"
                        meta["price_confidence"] = "medium"
                        meta["price_confidence_color"] = PRICE_CONFIDENCE_COLOR["medium"]
                    recovered_rows += 1
                    recovered_eur_volume += rec

        # CASE B: in known, out missing
        if in_known > 0 and out_missing:
            amt_sum = sum(abs(float(getattr(r, "amount", 0.0) or 0.0)) for r in out_missing)
            if amt_sum > 0:
                for r in out_missing:
                    share = abs(float(getattr(r, "amount", 0.0) or 0.0)) / amt_sum
                    rec = float(in_known) * share
                    if rec <= 0:
                        continue
                    r.eur_value = rec
                    meta = getattr(r, "meta", None)
                    if not isinstance(meta, dict):
                        meta = {}
                        try:
                            setattr(r, "meta", meta)
                        except Exception:
                            pass
                    if isinstance(meta, dict):
                        meta["price_source"] = "recovered"
                        meta["price_confidence"] = "medium"
                        meta["price_confidence_color"] = PRICE_CONFIDENCE_COLOR["medium"]
                    recovered_rows += 1
                    recovered_eur_volume += rec

    return {
        "recovered_rows": float(recovered_rows),
        "recovered_eur_volume": float(recovered_eur_volume),
    }


def _usd_fallback_eur_value(classified_dicts: List[Dict[str, Any]]) -> None:
    for r in classified_dicts:
        if r.get("eur_value"):
            continue
        usd = r.get("usd_value") or r.get("USDValueDayOfTx")
        if not usd:
            continue
        try:
            usd_val = float(str(usd).replace("$", "").strip())
            r["eur_value"] = usd_val
        except Exception:
            pass


def _dedupe_swap_when_position_exit(economic_gains: List[Dict[str, Any]]) -> None:
    """
    apply_vault_exits emits position_exit from the vault ledger for the same tx that
    FIFO+swap grouping may have already summarized as swap. Keep a single economic
    exit: drop swap rows when position_exit exists for that tx_hash.
    """
    by_tx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in economic_gains:
        txh = (e.get("tx_hash") or "").strip()
        by_tx[txh].append(e)
    merged: List[Dict[str, Any]] = []
    for txh, rows in by_tx.items():
        if not txh:
            merged.extend(rows)
            continue
        has_position_exit = any(
            (r.get("category") or "").lower() == "position_exit" for r in rows
        )
        if has_position_exit:
            merged.extend(r for r in rows if (r.get("category") or "").lower() != "swap")
        else:
            merged.extend(rows)
    economic_gains.clear()
    economic_gains.extend(merged)


def _enforce_single_realization_per_tx(economic_gains: List[Dict[str, Any]]) -> None:
    """
    Hard guard: max 1 realization row per tx_hash.
    Realization categories are swap/sell/position_exit/lp_remove.

    Rule:
    - If position_exit exists in tx: keep only position_exit among realization rows,
      and remove swap/sell realization rows.
    - If multiple realization rows remain, keep exactly one deterministic winner
      (category priority + highest absolute pnl) and keep non-realization rows unchanged.
    - Never crash the run; emit warning logs for visibility.
    """
    realization_cats = {"swap", "sell", "position_exit", "lp_remove"}
    by_tx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for e in economic_gains:
        txh = (e.get("tx_hash") or "").strip()
        by_tx[txh].append(e)

    guarded: List[Dict[str, Any]] = []
    for txh, rows in by_tx.items():
        if not txh:
            guarded.extend(rows)
            continue
        has_position_exit = any((r.get("category") or "").lower() == "position_exit" for r in rows)
        if has_position_exit:
            for r in rows:
                cat = (r.get("category") or "").lower()
                if cat in {"swap", "sell"}:
                    continue
                guarded.append(r)
            continue
        guarded.extend(rows)

    economic_gains.clear()
    economic_gains.extend(guarded)

    by_tx_after: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in economic_gains:
        txh = (r.get("tx_hash") or "").strip()
        by_tx_after[txh].append(r)

    def _row_pnl_abs(row: Dict[str, Any]) -> float:
        try:
            return abs(float(row.get("pnl_eur") or 0.0))
        except Exception:
            return 0.0

    # Higher number = higher keep priority
    cat_priority = {
        "position_exit": 4,
        "lp_remove": 3,
        "sell": 2,
        "swap": 1,
    }

    normalized: List[Dict[str, Any]] = []
    dedup_count = 0
    for txh, rows in by_tx_after.items():
        if not txh:
            normalized.extend(rows)
            continue
        realization_rows = [r for r in rows if (r.get("category") or "").lower() in realization_cats]
        if len(realization_rows) <= 1:
            normalized.extend(rows)
            continue

        winner = max(
            realization_rows,
            key=lambda r: (
                cat_priority.get((r.get("category") or "").lower(), 0),
                _row_pnl_abs(r),
            ),
        )
        winner_cat = (winner.get("category") or "").lower()
        print(
            f"[PIPELINE][REALIZATION_DEDUP] tx={txh} realizations={len(realization_rows)} "
            f"keep={winner_cat}"
        )
        dedup_count += len(realization_rows) - 1

        for r in rows:
            cat = (r.get("category") or "").lower()
            if cat in realization_cats:
                if r is winner:
                    normalized.append(r)
                continue
            normalized.append(r)

    if dedup_count > 0:
        print(f"[PIPELINE][REALIZATION_DEDUP] removed_rows={dedup_count}")

    economic_gains.clear()
    economic_gains.extend(normalized)


def _cleanup_vault_exit_per_tx(economic_gains: List[Dict[str, Any]]) -> None:
    by_tx = defaultdict(list)
    for r in economic_gains:
        by_tx[r.get("tx_hash", "")].append(r)
    cleaned = []
    for txh, rows in by_tx.items():
        has_vault_exit = any((r.get("category") or "").lower() == "vault_exit" for r in rows)
        if has_vault_exit:
            cleaned.extend([r for r in rows if (r.get("category") or "").lower() == "vault_exit"])
        else:
            cleaned.extend(rows)
    economic_gains.clear()
    economic_gains.extend(cleaned)


def _apply_fees_net_pnl(classified_dicts: List[Dict[str, Any]], economic_gains: List[Dict[str, Any]]) -> None:
    fees_by_tx = defaultdict(float)
    for r in classified_dicts:
        txh = r.get("tx_hash") or ""
        fees_by_tx[txh] += float(r.get("fee_eur") or 0.0)
    for e in economic_gains:
        txh = e.get("tx_hash") or ""
        fee = float(fees_by_tx.get(txh, 0.0))
        pnl = float(e.get("pnl_eur") or 0.0)
        e["fees_eur"] = round(fee, 2)
        e["net_pnl_eur"] = round(pnl - fee, 2)


def _write_audit_csv_tax_ready(
    tax_ready_rows: List[Dict[str, Any]],
    gains: List[Any],
    output_path: Path,
    tax_year: int,
) -> None:
    """Audit CSV from tax_interpreter output (§23 buckets, net gain)."""
    amount_by_tx = defaultdict(float)
    for g in gains:
        txh = getattr(g, "tx_hash", "") or ""
        if not txh:
            continue
        try:
            amt = float(getattr(g, "amount_out", 0.0) or 0.0)
        except Exception:
            amt = 0.0
        amount_by_tx[txh] += amt

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "tx_hash",
            "date",
            "category",
            "token",
            "amount",
            "proceeds_eur",
            "cost_basis_eur",
            "pnl_gross_eur",
            "fees_eur",
            "gain_net_eur",
            "speculative_1y_eur",
            "long_term_eur",
            "taxable_flag",
            "hold_days_min",
            "hold_days_max",
        ])
        for e in tax_ready_rows:
            txh = e.get("tx_hash") or ""
            dt_iso = e.get("dt_iso") or ""
            date = dt_iso.split("T")[0] if "T" in str(dt_iso) else dt_iso
            writer.writerow(
                [
                    txh,
                    date,
                    e.get("category") or "",
                    e.get("token") or "",
                    amount_by_tx.get(txh, ""),
                    e.get("proceeds"),
                    e.get("cost_basis"),
                    e.get("pnl_gross_eur"),
                    e.get("fees_eur"),
                    e.get("gain"),
                    e.get("speculative_bucket_net_eur"),
                    e.get("long_term_bucket_net_eur"),
                    "1" if e.get("taxable") else "0",
                    e.get("holding_period_days_min"),
                    e.get("holding_period_days_max"),
                ]
            )


def _persist_tax_ready_outputs(
    wallet: str,
    tax_year: int,
    economic_gains_tax_ready: List[Dict[str, Any]],
    tax_summary: Dict[str, Any],
) -> None:
    wallet_norm = (wallet or "").strip().lower()
    if not wallet_norm:
        raise ValueError("primary wallet missing; cannot persist tax-ready outputs")

    harvest_dir = Path(__file__).resolve().parents[1] / "data" / "harvest" / wallet_norm / str(tax_year)
    harvest_dir.mkdir(parents=True, exist_ok=True)

    tax_ready_path = harvest_dir / "economic_gains_tax_ready.json"
    summary_path = harvest_dir / "tax_summary.json"

    tax_ready_path.write_text(
        json.dumps(economic_gains_tax_ready, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    summary_path.write_text(
        json.dumps(tax_summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _persist_identity_resolver_report(
    wallet: str,
    tax_year: int,
    classified_rows: List[Dict[str, Any]],
) -> None:
    """
    Persist an identity-focused diagnostics report for unresolved valuations.
    This report is read-only guidance for deterministic contract/token mapping work.
    """
    wallet_norm = (wallet or "").strip().lower()
    if not wallet_norm:
        raise ValueError("primary wallet missing; cannot persist identity resolver report")

    harvest_dir = Path(__file__).resolve().parents[1] / "data" / "harvest" / wallet_norm / str(tax_year)
    harvest_dir.mkdir(parents=True, exist_ok=True)
    out_path = harvest_dir / "identity_resolver_report.json"

    unresolved: List[Dict[str, Any]] = []
    unresolved_economic: List[Dict[str, Any]] = []
    by_tx: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    economic_dirs = {"out", "swap"}
    skip_internal_cats = {"internal_transfer", "self_transfer"}
    for r in classified_rows:
        txh = str(r.get("tx_hash") or "").lower().strip()
        if txh:
            by_tx[txh].append(r)
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        conf = (meta.get("price_confidence") or "").lower()
        try:
            amt = abs(float(r.get("amount") or 0.0))
        except Exception:
            amt = 0.0
        ev = r.get("eur_value")
        try:
            eur = float(ev) if ev is not None else None
        except Exception:
            eur = None
        if bool(meta.get("valuation_missing")) or (amt > 0 and (eur is None or eur <= 0)) or conf == "low":
            unresolved.append(r)
            d = (r.get("direction") or "").lower()
            c = (r.get("category") or "").lower()
            if d in economic_dirs and c not in skip_internal_cats:
                unresolved_economic.append(r)

    by_contract: Dict[str, Dict[str, Any]] = {}
    for r in unresolved_economic:
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        chain = (r.get("chain_id") or "").lower().strip()
        token_contract = (
            meta.get("token_contract")
            or meta.get("contract_addr")
            or meta.get("contract_address")
            or meta.get("token_address")
            or ""
        )
        token_contract = str(token_contract).lower().strip()
        cp_addr = str(meta.get("cp_addr") or "").lower().strip()
        key_addr = token_contract or cp_addr or "<missing>"
        key = f"{chain}:{key_addr}"

        bucket = by_contract.setdefault(
            key,
            {
                "chain_id": chain,
                "key_address": key_addr,
                "token_contract": token_contract or None,
                "cp_addr": cp_addr or None,
                "rows": 0,
                "tokens": defaultdict(int),
                "methods": defaultdict(int),
                "directions": defaultdict(int),
                "categories": defaultdict(int),
                "tx_hashes": set(),
            },
        )
        bucket["rows"] += 1
        bucket["tokens"][(r.get("token") or "").upper()] += 1
        bucket["methods"][str(r.get("method") or "")] += 1
        bucket["directions"][(r.get("direction") or "").lower()] += 1
        bucket["categories"][(r.get("category") or "").lower()] += 1
        txh = str(r.get("tx_hash") or "").lower().strip()
        if txh:
            bucket["tx_hashes"].add(txh)

    candidates: List[Dict[str, Any]] = []
    for key, info in by_contract.items():
        txs = sorted(info["tx_hashes"])
        counterpart = defaultdict(int)
        for txh in txs:
            for rr in by_tx.get(txh, []):
                tok = (rr.get("token") or "").upper().strip()
                if not tok or tok in ("UNKNOWN", "ERC-20"):
                    continue
                try:
                    eur = float(rr.get("eur_value") or 0.0)
                except Exception:
                    eur = 0.0
                if eur > 0:
                    counterpart[map_token(tok)] += 1
        top_counterpart = sorted(counterpart.items(), key=lambda x: x[1], reverse=True)
        if len(txs) >= 3 and top_counterpart:
            best_tok, best_cnt = top_counterpart[0]
            coverage = best_cnt / max(1, len(txs))
            if coverage >= 0.8:
                candidates.append(
                    {
                        "contract_key": key,
                        "chain_id": info["chain_id"],
                        "key_address": info["key_address"],
                        "observed_rows": info["rows"],
                        "observed_txs": len(txs),
                        "suggested_maps_to": best_tok,
                        "support_txs": best_cnt,
                        "support_ratio": round(coverage, 4),
                        "reason": "deterministic_tx_counterpart_consistency",
                    }
                )

    contracts_sorted = sorted(
        (
            {
                "contract_key": k,
                "chain_id": v["chain_id"],
                "key_address": v["key_address"],
                "token_contract": v["token_contract"],
                "cp_addr": v["cp_addr"],
                "rows": v["rows"],
                "tx_count": len(v["tx_hashes"]),
                "tokens": dict(sorted(v["tokens"].items(), key=lambda x: x[1], reverse=True)),
                "methods": dict(sorted(v["methods"].items(), key=lambda x: x[1], reverse=True)),
                "directions": dict(sorted(v["directions"].items(), key=lambda x: x[1], reverse=True)),
                "categories": dict(sorted(v["categories"].items(), key=lambda x: x[1], reverse=True)),
            }
            for k, v in by_contract.items()
        ),
        key=lambda x: x["rows"],
        reverse=True,
    )

    # Actionable focus: reduce noise from one-off addresses and pure transfer artifacts.
    actionable_contracts = [
        c
        for c in contracts_sorted
        if c["rows"] >= 3
    ]

    cause_breakdown = defaultdict(int)
    for r in unresolved:
        tok = (r.get("token") or "").upper().strip()
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        if tok in {"UNKNOWN", "ERC-20"}:
            cause_breakdown["generic_symbol_identity_gap"] += 1
            continue
        if (meta.get("effective_token_source") or "").lower() == "maps_to":
            cause_breakdown["mapped_identity_but_no_price"] += 1
            continue
        cause_breakdown["known_or_mapped_symbol_no_price"] += 1

    unresolved_by_token = defaultdict(int)
    for r in unresolved:
        unresolved_by_token[(r.get("token") or "").upper().strip()] += 1

    unresolved_economic_by_token = defaultdict(int)
    for r in unresolved_economic:
        unresolved_economic_by_token[(r.get("token") or "").upper().strip()] += 1

    report = {
        "wallet": wallet_norm,
        "year": tax_year,
        "unresolved_rows": len(unresolved),
        "unresolved_rows_economic_only": len(unresolved_economic),
        "contracts_analyzed": len(contracts_sorted),
        "contracts_analyzed_economic_only": len(contracts_sorted),
        "actionable_contracts_count": len(actionable_contracts),
        "cause_breakdown": dict(cause_breakdown),
        "top_unresolved_tokens": sorted(
            ({"token": k, "count": v} for k, v in unresolved_by_token.items() if k),
            key=lambda x: x["count"],
            reverse=True,
        )[:25],
        "top_unresolved_tokens_economic_only": sorted(
            ({"token": k, "count": v} for k, v in unresolved_economic_by_token.items() if k),
            key=lambda x: x["count"],
            reverse=True,
        )[:25],
        "top_unresolved_contracts": contracts_sorted[:100],
        "top_actionable_contracts": actionable_contracts[:30],
        "deterministic_candidates": sorted(candidates, key=lambda x: x["observed_rows"], reverse=True),
    }

    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def _norm_tx_pipeline(txh: Optional[str]) -> str:
    return str(txh or "").strip().lower()


def _classified_tx_valuation_missing(txh: str, classified_dicts: List[Dict[str, Any]]) -> bool:
    k = _norm_tx_pipeline(txh)
    if not k:
        return False
    for r in classified_dicts or []:
        if _norm_tx_pipeline(r.get("tx_hash")) != k:
            continue
        m = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        if m.get("valuation_missing") is True:
            return True
    return False


def _classified_tx_has_swap(txh: str, classified_dicts: List[Dict[str, Any]]) -> bool:
    k = _norm_tx_pipeline(txh)
    if not k:
        return False
    for r in classified_dicts or []:
        if _norm_tx_pipeline(r.get("tx_hash")) != k:
            continue
        if str(r.get("category") or "").strip().lower() == "swap":
            return True
    return False


def _apply_valuation_missing_zero_economic(
    economic_gains: List[Dict[str, Any]],
    classified_dicts: List[Dict[str, Any]],
) -> None:
    """Any classified row with meta.valuation_missing → economic realization EUR fields zeroed (gains.json SSOT)."""
    logged_tx: set[str] = set()
    for e in economic_gains or []:
        txh = str(e.get("tx_hash") or "")
        if not _classified_tx_valuation_missing(txh, classified_dicts):
            continue
        e["pnl_eur"] = 0.0
        e["net_pnl_eur"] = 0.0
        e["proceeds_eur"] = 0.0
        e["cost_basis_eur"] = 0.0
        e["fees_eur"] = 0.0
        e["valuation_missing"] = True
        k = _norm_tx_pipeline(txh)
        if k and k not in logged_tx:
            logged_tx.add(k)
            print(f"[FIX_APPLIED] tx={txh} reason=valuation_missing")



def _reconcile_false_swap_economic(
    economic_gains: List[Dict[str, Any]],
    classified_dicts: List[Dict[str, Any]],
) -> None:
    """
    If FIFO/grouping produced swap but classified has no swap row, report as sell so
    gains.json and tax_ready stay aligned with classification (FIFO legs still match via _fifo_category).
    """
    for e in economic_gains or []:
        if str(e.get("category") or "").strip().lower() != "swap":
            continue
        txh = str(e.get("tx_hash") or "")
        if _classified_tx_has_swap(txh, classified_dicts):
            continue
        e["_fifo_category"] = "swap"
        e["category"] = "sell"


def _validate_pipeline_consistency_or_raise(
    classified_dicts: List[Dict[str, Any]],
    economic_gains: List[Dict[str, Any]],
    tax_ready: List[Dict[str, Any]],
    *,
    wallet_id: str,
    tax_year: int,
) -> None:
    fails = validate_consistency_lists(
        classified_dicts,
        economic_gains,
        tax_ready,
        wallet_id=wallet_id,
        year=tax_year,
    )
    if not fails:
        return
    for line in fails:
        print(f"[PIPELINE][CONSISTENCY] {line}")
    raise RuntimeError(
        f"pipeline consistency validation failed ({len(fails)} issue(s)); first: {fails[0]}"
    )


# ---------------------------------------------------------------------------
# run_pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    wallet_data: List[WalletDataItem],
    tax_year: int,
    config: Optional[PipelineConfig] = None,
) -> PipelineResult:
    """
    Single entry point for the tax pipeline.

    wallet_data: List of {wallet: str, chain_id: str, base_dir: Path}. Each base_dir
                 should contain normal.csv, erc20.csv, internal.csv (any that exist).
    tax_year: Year for filtering and report (e.g. 2025).
    config: Optional dict with:
        - output_dir: Path for PDF and audit CSV
        - report_label: str for PDF filename (e.g. "stefan_2025" or "0x123_2025")
        - primary_wallet: str for evaluate_batch (default: first wallet in wallet_data)
        - debug: bool for extra logs (e.g. position audit)
        - debug_info: dict merged into PDF debug_info
        - validate_raw_rows: bool – if True, validate each loaded row (tx_hash, timestamp, dt_iso, token, amount, direction, from_addr, to_addr, method, chain_id); raise on first invalid row (default False).
        - require_chain_id: bool – when validate_raw_rows is True, require non-empty chain_id (default False, so Coinbase/generic rows pass).
        - chain_csv_source: dict – optional {chain_id: "auto-fetched"|"existing files"} for ingestion report.
        - skip_pipeline_consistency_check: bool – if True, skip validate_consistency after tax_ready (default False).

    Returns dict with: economic_gains, classified_dicts, gains, totals, reward_summary, debug_info.
    """
    config = config or {}
    output_dir = config.get("output_dir")
    report_label = config.get("report_label") or f"report_{tax_year}"
    primary_wallet = config.get("primary_wallet")
    debug = config.get("debug", False)
    extra_debug_info = config.get("debug_info") or {}

    if not wallet_data:
        raise ValueError("wallet_data is empty")

    if primary_wallet is None:
        primary_wallet = (wallet_data[0].get("wallet") or "").lower()

    # 1. Load transactions
    raw_rows, filtered_dicts = _load_transactions(wallet_data, tax_year)

    # Per-chain counts for ingestion report
    per_chain_loaded: Dict[str, int] = defaultdict(int)
    for r in raw_rows:
        cid = _row_chain_id(r)
        per_chain_loaded[cid] += 1
    per_chain_filtered: Dict[str, int] = defaultdict(int)
    for r in filtered_dicts:
        cid = (r.get("chain_id") or "") or ""
        per_chain_filtered[cid] += 1
    chain_csv_source: Dict[str, str] = config.get("chain_csv_source") or {}

    _print_ingestion_report(
        primary_wallet=primary_wallet,
        tax_year=tax_year,
        wallet_data=wallet_data,
        per_chain_loaded=dict(per_chain_loaded),
        per_chain_filtered=dict(per_chain_filtered),
        total_filtered=len(filtered_dicts),
        chain_csv_source=chain_csv_source,
        rows_validated=len(filtered_dicts) if config.get("validate_raw_rows") else None,
    )

    if config.get("validate_raw_rows"):
        validate_raw_rows(
            filtered_dicts,
            require_chain_id=config.get("require_chain_id", False),
            raise_on_first=True,
        )

    if not filtered_dicts:
        return {
            "economic_gains": [],
            "classified_dicts": [],
            "gains": [],
            "totals": {},
            "reward_summary": {},
            "debug_info": {"wallet": primary_wallet, "tax_year": tax_year, **extra_debug_info},
        }

    # 2. Normalize: filtered_dicts already have top-level chain_id (set in _load_transactions)
    tx_to_chain = {r.get("tx_hash"): r.get("chain_id") or "" for r in filtered_dicts if r.get("tx_hash")}

    # 3. Classify (evaluate_batch copies chain_id from tx into ClassifiedItem)
    classified, debug_info = evaluate_batch(filtered_dicts, primary_wallet)
    print(f"[PIPELINE] Classified {len(classified)} items")

    # 4. Resolve prices in one batch (unique symbol+date+chain)
    price_queries = _collect_price_queries_from_classified(classified)
    if price_queries:
        price_results = resolve_prices_batch(price_queries)
        price_map = _build_price_map(price_queries, price_results)
        print(f"[PIPELINE] Resolved {len(price_queries)} unique price queries -> {len(price_map)} prices")
    else:
        price_map = {}

    # 5. Compute eur_value and fee_eur from price map
    swap_cov_before = _swap_recovery_metrics(classified)
    _fill_base_token_eur_value(classified, price_map)
    swap_recovery = _recover_swap_missing_values(classified)
    swap_cov_after = _swap_recovery_metrics(classified)
    print(
        "[PIPELINE][SWAP_RECOVERY] "
        f"coverage_before={swap_cov_before['coverage_pct']:.2f}% "
        f"coverage_after={swap_cov_after['coverage_pct']:.2f}% "
        f"recovered_rows={int(swap_recovery['recovered_rows'])} "
        f"recovered_eur_volume={swap_recovery['recovered_eur_volume']:.2f}"
    )
    classified_dicts = [c.to_dict() if hasattr(c, "to_dict") else c for c in classified]
    for d in classified_dicts:
        d.setdefault("chain_id", tx_to_chain.get(d.get("tx_hash", ""), ""))

    _fee_eur_on_classified_dicts(classified_dicts, price_map)
    _lp_vault_mint_eur_value(classified)

    # Update eur_value from classified (LP/vault mint); do NOT rebuild dicts or we lose fee_eur
    for i, c in enumerate(classified):
        if i < len(classified_dicts):
            classified_dicts[i]["eur_value"] = float(getattr(c, "eur_value", 0.0) or 0.0)
            classified_dicts[i].setdefault("chain_id", tx_to_chain.get(getattr(c, "tx_hash", ""), ""))

    # 6. FIFO gain calculation
    gains, totals = compute_gains(classified)
    print(f"[PIPELINE] Gains: {len(gains)}")

    # 7. Economic grouping
    economic_gains = group_gains_economic([g.to_dict() for g in gains])
    print(f"[PIPELINE] Economic gains: {len(economic_gains)}")

    # 8. Resolve vault exits
    economic_gains = apply_vault_exits(
        economic_gains,
        classified_dicts,
        [g.to_dict() for g in gains],
    )
    _cleanup_vault_exit_per_tx(economic_gains)
    _dedupe_swap_when_position_exit(economic_gains)
    _enforce_single_realization_per_tx(economic_gains)

    # 9. Apply tax logic (fees, net_pnl, reward eur_value, USD fallback)
    _apply_fees_net_pnl(classified_dicts, economic_gains)
    _apply_valuation_missing_zero_economic(economic_gains, classified_dicts)
    _reconcile_false_swap_economic(economic_gains, classified_dicts)
    _reward_eur_value(classified_dicts, price_map)
    _usd_fallback_eur_value(classified_dicts)
    _persist_identity_resolver_report(
        wallet=primary_wallet,
        tax_year=tax_year,
        classified_rows=classified_dicts,
    )

    # 9b. Jurisdiction projection (DE default; US later)
    jurisdiction_code = (config.get("jurisdiction") or "DE")
    jurisdiction = get_jurisdiction(jurisdiction_code)
    fifo_gain_dicts = [g.to_dict() for g in gains]
    tax_result = jurisdiction.process(
        list(economic_gains or []),
        context={
            "jurisdiction": getattr(jurisdiction, "code", str(jurisdiction_code).upper()),
            "tax_year": tax_year,
            "wallet": primary_wallet,
            "fifo_gain_rows": fifo_gain_dicts,
            "classified_dicts": classified_dicts,
        },
    )
    economic_gains_tax_ready = list(tax_result.tax_ready_events or [])
    tax_summary = dict(tax_result.tax_summary or {})
    reward_income = (tax_result.meta or {}).get("reward_income") if isinstance(tax_result.meta, dict) else None
    reward_income_summary = (tax_result.meta or {}).get("reward_income_summary") if isinstance(tax_result.meta, dict) else None

    # Consistency + audit validation are currently DE tax-ready specific. Gate by jurisdiction code.
    if str(getattr(jurisdiction, "code", "")).upper() == "DE":
        if not config.get("skip_pipeline_consistency_check"):
            _validate_pipeline_consistency_or_raise(
                classified_dicts,
                economic_gains,
                economic_gains_tax_ready,
                wallet_id=primary_wallet,
                tax_year=tax_year,
            )
        _audit_validation = validate_tax_ready_audit(
            economic_gains_tax_ready,
            tax_summary,
            classified_dicts,
        )
        _conf_dist = confidence_distribution(economic_gains_tax_ready)
        _vm_count = sum(
            1
            for r in classified_dicts
            if isinstance(r.get("meta"), dict) and (r.get("meta") or {}).get("valuation_missing")
        )
        audit_report_payload = {
            "validation": _audit_validation,
            "confidence_distribution": _conf_dist,
            "problematic_tokens": top_problem_tokens(classified_dicts),
            "unresolved_tx_hashes": unresolved_tx_hashes(classified_dicts),
            "valuation_missing_count": _vm_count,
        }
        _persist_tax_ready_outputs(
            wallet=primary_wallet,
            tax_year=tax_year,
            economic_gains_tax_ready=economic_gains_tax_ready,
            tax_summary=tax_summary,
        )
    else:
        _audit_validation: Dict[str, Any] = {}
        _conf_dist: Dict[str, Any] = {}
        audit_report_payload = {
            "problematic_tokens": [],
            "unresolved_tx_hashes": [],
            "valuation_missing_count": 0,
        }
        _persist_tax_ready_outputs(
            wallet=primary_wallet,
            tax_year=tax_year,
            economic_gains_tax_ready=economic_gains_tax_ready,
            tax_summary=tax_summary,
        )

    # 10. Compute rewards
    reward_summary = group_rewards(classified_dicts)
    print(f"[PIPELINE] Rewards: {len(reward_summary)} tokens")

    debug_info_out = {
        "wallet": primary_wallet,
        "tax_year": tax_year,
        "from": f"{tax_year}-01-01",
        "to": f"{tax_year}-12-31",
        **(debug_info or {}),
        **extra_debug_info,
        "audit_report": audit_report_payload,
    }

    try:
        audit_json_path = write_audit_json(
            primary_wallet,
            tax_year,
            economic_gains_tax_ready,
            tax_summary,
            _audit_validation,
            _conf_dist,
            audit_report_payload["problematic_tokens"],
            audit_report_payload["unresolved_tx_hashes"],
        )
        print(f"[PIPELINE] Audit JSON: {audit_json_path}")
    except Exception as e:
        print(f"[PIPELINE] Audit JSON write failed: {e}")

    # 11. Generate report
    if output_dir:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        audit_file = output_path / f"tax_audit_{tax_year}.csv"
        pdf_file = output_path / f"tax_report_{tax_year}.pdf"
        if config.get("audit_filename"):
            audit_file = output_path / config["audit_filename"]
        if config.get("pdf_filename"):
            pdf_file = output_path / config["pdf_filename"]

        _write_audit_csv_tax_ready(economic_gains_tax_ready, gains, audit_file, tax_year)
        print(f"[PIPELINE] Audit CSV: {audit_file}")

        build_pdf(
            economic_records=economic_gains_tax_ready,
            reward_records=classified_dicts,
            summary=totals,
            debug_info=debug_info_out,
            tax_summary=tax_summary,
            outpath=str(pdf_file),
        )
        print(f"[PIPELINE] PDF: {pdf_file}")

    return {
        "economic_gains": economic_gains,
        "economic_gains_tax_ready": economic_gains_tax_ready,
        "tax_summary": tax_summary,
        "reward_income": reward_income,
        "reward_income_summary": reward_income_summary,
        "classified_dicts": classified_dicts,
        "gains": gains,
        "totals": totals,
        "reward_summary": reward_summary,
        "debug_info": debug_info_out,
    }
