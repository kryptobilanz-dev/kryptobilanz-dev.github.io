from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests

from taxtrack.data.config.chain_config import CHAIN_CONFIG

REWARD_CATEGORIES = {
    "reward",
    "staking_reward",
    "vault_reward",
    "pendle_reward",
    "restake_reward",
    "airdrop",
    "learning_reward",
    "earn_reward",
}
_DYNAMIC_CONFIG_WRITES = (
    os.getenv("TAXTRACK_ALLOW_DYNAMIC_CONFIG_WRITES", "").strip().lower() in {"1", "true", "yes", "on"}
)


def _norm(s: Any) -> str:
    return str(s or "").strip()


def _up(s: Any) -> str:
    return _norm(s).upper()


def _classify_unknown_reason(row: Dict[str, Any]) -> Tuple[str, str]:
    """
    Deterministic UNKNOWN classification without pricing/mapping/guessing.
    """
    meta = row.get("meta") if isinstance(row.get("meta"), dict) else {}
    token = _up(row.get("token"))
    raw_token = _up(meta.get("raw_token"))
    category = _norm(row.get("category")).lower()
    cp_protocol = _norm(meta.get("cp_protocol")).lower()

    observed_tokens: List[str] = []
    if token:
        observed_tokens.append(token)
    if raw_token:
        observed_tokens.append(raw_token)
    for leg_key in ("tokens_in", "tokens_out"):
        legs = meta.get(leg_key) if isinstance(meta.get(leg_key), list) else []
        for leg in legs:
            observed_tokens.append(_up(leg.get("token")))

    joined = " ".join(t for t in observed_tokens if t)

    # A) LP / pool receipts
    if any(k in joined for k in ("_LPT", " LPT", "PENDLE_LPT", " LP")):
        return (
            "lp_token",
            "Token ist ein LP/Pool-Receipt. Kein direkter Spot-Preis; Bewertung erfolgt implizit ueber Ein-/Ausstiegsflows.",
        )

    # A) Vault-like receipts
    if any(k in joined for k in ("MOO", "BEEFY", "VAULT", "RCOW")) or cp_protocol == "vault":
        return (
            "vault_token",
            "Token ist ein Vault/Receipt-Token. Kein direkter Spot-Preis; Bewertung erfolgt implizit ueber Positions- und Exit-Flows.",
        )

    # B) Reward with no market price
    if category in REWARD_CATEGORIES:
        return (
            "reward_no_price",
            "Reward-Token ohne belastbaren Marktpreis zum Tx-Zeitpunkt; Bewertung bleibt offen.",
        )

    # C) Spam/fake marker only on explicit indicators
    if "HTTP" in joined or "WWW" in joined or "CLAIM" in joined:
        return (
            "spam",
            "Expliziter Spam/Fake-Indikator im Token-Metadateninhalt (Claim/URL-Marker).",
        )

    # D) unresolved real token candidate
    return (
        "unresolved",
        "Unaufgeloester Tokenfall ohne belastbare Preis-/Identitaetsbasis; bewusst nicht gemappt.",
    )


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _economic_volume_by_tx(econ_rows: List[Dict[str, Any]]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for r in econ_rows:
        txh = _norm(r.get("tx_hash")).lower()
        if not txh:
            continue
        vol = abs(float(r.get("proceeds") or 0.0)) + abs(float(r.get("cost_basis") or 0.0))
        if vol > out.get(txh, 0.0):
            out[txh] = vol
    return out


def _safe_float(v: Any) -> float:
    try:
        return float(v or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _sum_amounts(legs: List[Dict[str, Any]]) -> float:
    return sum(_safe_float(leg.get("amount")) for leg in legs if isinstance(leg, dict))


def _has_known_eur(legs: List[Dict[str, Any]]) -> bool:
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        eur = leg.get("eur_value")
        if eur is None:
            continue
        if _safe_float(eur) > 0.0:
            return True
    return False


def _missing_side(tokens_in: List[Dict[str, Any]], tokens_out: List[Dict[str, Any]]) -> str:
    in_known = _has_known_eur(tokens_in)
    out_known = _has_known_eur(tokens_out)
    if in_known and not out_known:
        return "out"
    if out_known and not in_known:
        return "in"
    return "both"


def _has_chain_contract_code(chain: str, contract_address: str, cache: Dict[Tuple[str, str], bool]) -> bool | None:
    """
    Returns:
      - True  -> contract code exists
      - False -> empty code ("0x")
      - None  -> unknown (rpc missing/error)
    """
    chain_key = _norm(chain).lower()
    addr = _norm(contract_address).lower()
    if not chain_key or not addr:
        return None
    cache_key = (chain_key, addr)
    if cache_key in cache:
        return cache[cache_key]

    rpc_url = ((CHAIN_CONFIG.get(chain_key) or {}).get("rpc") or "").strip()
    if not rpc_url:
        return None
    try:
        response = requests.post(
            rpc_url,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_getCode", "params": [addr, "latest"]},
            timeout=8,
        )
        response.raise_for_status()
        code = ((response.json() or {}).get("result") or "").strip().lower()
        has_code = code not in {"", "0x"}
        cache[cache_key] = has_code
        return has_code
    except Exception:
        return None


def _fetch_tx_receipt(chain: str, tx_hash: str, cache: Dict[Tuple[str, str], Dict[str, Any] | None]) -> Dict[str, Any] | None:
    chain_key = _norm(chain).lower()
    txh = _norm(tx_hash).lower()
    if not chain_key or not txh:
        return None
    cache_key = (chain_key, txh)
    if cache_key in cache:
        return cache[cache_key]

    rpc_url = ((CHAIN_CONFIG.get(chain_key) or {}).get("rpc") or "").strip()
    if not rpc_url:
        cache[cache_key] = None
        return None
    try:
        response = requests.post(
            rpc_url,
            json={"jsonrpc": "2.0", "id": 1, "method": "eth_getTransactionReceipt", "params": [txh]},
            timeout=8,
        )
        response.raise_for_status()
        receipt = (response.json() or {}).get("result")
        cache[cache_key] = receipt if isinstance(receipt, dict) else None
        return cache[cache_key]
    except Exception:
        cache[cache_key] = None
        return None


def _classify_contract_behavior(
    chain: str,
    tx_hash: str,
    contract_address: str,
    receipt_cache: Dict[Tuple[str, str], Dict[str, Any] | None],
) -> Tuple[str, str]:
    transfer_sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
    approval_sig = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
    zero_addr = "0x0000000000000000000000000000000000000000"

    receipt = _fetch_tx_receipt(chain, tx_hash, receipt_cache)
    if not receipt:
        return "unknown_behavior", "low"

    addr = _norm(contract_address).lower()
    transfer_count = 0
    approval_count = 0
    mint_count = 0
    burn_count = 0

    for log in receipt.get("logs", []) or []:
        if _norm(log.get("address")).lower() != addr:
            continue
        topics = log.get("topics") if isinstance(log.get("topics"), list) else []
        if not topics:
            continue
        sig = _norm(topics[0]).lower()
        if sig == transfer_sig:
            transfer_count += 1
            if len(topics) >= 3:
                from_addr = "0x" + _norm(topics[1])[-40:].lower()
                to_addr = "0x" + _norm(topics[2])[-40:].lower()
                if from_addr == zero_addr:
                    mint_count += 1
                if to_addr == zero_addr:
                    burn_count += 1
        elif sig == approval_sig:
            approval_count += 1

    if transfer_count > 0 and burn_count > 0:
        return "vault_exit", "high"
    if transfer_count > 0 and mint_count > 0:
        return "deposit", "high"
    if transfer_count > 0 and approval_count == 0:
        return "standard_transfer", "medium"
    if approval_count > transfer_count and approval_count > 0:
        return "protocol_interaction", "medium"
    return "unknown_behavior", "low"


def _resolution_key(chain: str, contract: str) -> str:
    return f"{_norm(chain).lower()}:{_norm(contract).lower()}"


def _load_contract_resolution_map(path: Path) -> Dict[str, Dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _persist_contract_resolution_map(path: Path, mapping: Dict[str, Dict[str, Any]]) -> None:
    path.write_text(json.dumps(mapping, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _static_resolution_to_meta(
    entry: Dict[str, Any],
) -> Tuple[str, str, str]:
    entry_type = _norm(entry.get("type")).upper()
    decision = _norm(entry.get("decision")).upper()
    protocol = _norm(entry.get("protocol")).lower() or "unresolved"
    reason = _norm(entry.get("reason"))
    explanation = f"Static contract resolution: {entry_type}/{decision} ({protocol}). {reason}".strip()

    if entry_type in {"LP", "VAULT"} or decision == "DERIVE":
        return "vault_token", explanation, "high"
    if entry_type == "SPAM" or decision == "IGNORE":
        return "spam", explanation, "high"
    if entry_type == "UNRESOLVED":
        return "unresolved", explanation, "high"
    return "unresolved", explanation, "medium"


def _infer_resolution_entry(
    chain: str,
    contract: str,
    cp_protocol: str,
    unknown_reason: str,
    behavior_type: str,
    behavior_confidence: str,
) -> Dict[str, Any] | None:
    # Only persist deterministic high-confidence outcomes.
    if behavior_confidence != "high":
        return None

    proto = _norm(cp_protocol).lower() or None
    chain_key = _norm(chain).lower()
    contract_key = _norm(contract).lower()
    if not chain_key or not contract_key:
        return None

    if unknown_reason == "chain_contract_mismatch":
        return {
            "type": "UNRESOLVED",
            "decision": "IGNORE",
            "protocol": proto,
            "confidence": "high",
            "reason": "eth_getCode returned 0x on the specified chain (chain-contract mismatch).",
        }
    if behavior_type == "vault_exit" or (proto in {"restake", "vault"}):
        return {
            "type": "VAULT",
            "decision": "DERIVE",
            "protocol": proto,
            "confidence": "high",
            "reason": "Vault/restake execution pattern detected (transfer+burn / protocol flow).",
        }
    if behavior_type == "deposit":
        return {
            "type": "VAULT",
            "decision": "DERIVE",
            "protocol": proto,
            "confidence": "high",
            "reason": "Deposit-like behavior detected (transfer+mint), no direct spot token mapping.",
        }
    return None


def _behavior_to_decision(
    behavior_type: str,
    behavior_confidence: str,
    unknown_reason: str,
    static_resolution: Dict[str, Any] | None,
) -> Tuple[str, str]:
    # Static map has highest precedence in audit output.
    if isinstance(static_resolution, dict):
        decision = _norm(static_resolution.get("decision")).upper() or "UNRESOLVED"
        conf = _norm(static_resolution.get("confidence")).lower() or "medium"
        return decision, conf

    if unknown_reason == "chain_contract_mismatch":
        return "UNRESOLVED", "high"
    if behavior_type in {"vault_exit", "deposit"}:
        return "DERIVE", behavior_confidence if behavior_confidence in {"high", "medium", "low"} else "medium"
    if behavior_type == "protocol_interaction":
        return "IGNORE", behavior_confidence if behavior_confidence in {"high", "medium", "low"} else "medium"
    if behavior_type == "standard_transfer":
        # Audit-only decision marker; no automatic mapping execution.
        return "MAP", behavior_confidence if behavior_confidence in {"high", "medium", "low"} else "medium"
    return "UNRESOLVED", "low"


def run() -> None:
    root = Path(__file__).resolve().parents[1]
    harvest_root = root / "data" / "harvest"
    out_audit_dir = root / "data" / "out" / "audit"
    out_audit_dir.mkdir(parents=True, exist_ok=True)
    resolution_map_path = root / "data" / "config" / "contract_resolution_map.json"
    contract_resolution_map = _load_contract_resolution_map(resolution_map_path)
    appended_resolution_entries = 0
    if not _DYNAMIC_CONFIG_WRITES:
        print("[DETERMINISTIC_MODE] dynamic writes disabled")

    summary_total_missing_eur = 0.0
    breakdown_eur: Dict[str, float] = defaultdict(float)
    top_contracts = Counter()
    top_protocols = Counter()
    top_contracts_eur: Dict[str, float] = defaultdict(float)
    top_protocols_eur: Dict[str, float] = defaultdict(float)
    breakdown_count = Counter()
    enriched_rows = 0
    cases: List[Dict[str, Any]] = []
    code_cache: Dict[Tuple[str, str], bool] = {}
    receipt_cache: Dict[Tuple[str, str], Dict[str, Any] | None] = {}
    contract_behavior_rollup: Dict[str, List[str]] = defaultdict(list)
    contract_context: Dict[str, Dict[str, Any]] = {}

    wallet_summaries: List[Dict[str, Any]] = []

    for wallet_dir in sorted(harvest_root.glob("*/2025")):
        cls_path = wallet_dir / "classified.json"
        econ_path = wallet_dir / "economic_gains_tax_ready.json"
        if not cls_path.exists() or not econ_path.exists():
            continue

        classified = _load_json(cls_path)
        economic = _load_json(econ_path)
        if not isinstance(classified, list) or not isinstance(economic, list):
            continue

        econ_vol_by_tx = _economic_volume_by_tx(economic)
        tx_reason: Dict[str, str] = {}
        tx_expl: Dict[str, str] = {}

        wallet_missing_eur = 0.0
        wallet_breakdown: Dict[str, float] = defaultdict(float)

        # Enrich classified missing rows
        for r in classified:
            if not isinstance(r, dict):
                continue
            meta = r.get("meta")
            if not isinstance(meta, dict):
                continue
            if not bool(meta.get("valuation_missing")):
                continue

            reason, explanation = _classify_unknown_reason(r)

            # Audit-only chain/contract validation to prevent cross-chain misinterpretation.
            chain_value = _norm(r.get("chain_id") or meta.get("chain_id")).lower()
            contract_value = _norm(meta.get("token_contract") or meta.get("cp_addr")).lower()
            cp_addr_value = _norm(meta.get("cp_addr")).lower()
            cp_protocol_value = _norm(meta.get("cp_protocol")).lower()
            resolution_lookup_contract = cp_addr_value or contract_value
            resolution_key = _resolution_key(chain_value, resolution_lookup_contract) if chain_value and resolution_lookup_contract else ""
            static_resolution = None

            if resolution_key and resolution_key in contract_resolution_map:
                static_resolution = contract_resolution_map[resolution_key]
                reason, explanation, static_conf = _static_resolution_to_meta(static_resolution)
                meta["confidence"] = static_conf
                meta["contract_resolution_source"] = "static_map"
                meta["contract_resolution"] = static_resolution

            if chain_value and contract_value:
                has_code = _has_chain_contract_code(chain_value, contract_value, code_cache)
                if has_code is False:
                    reason = "chain_contract_mismatch"
                    explanation = (
                        "Adresse hat auf dieser Chain keinen Contract-Code (EOA/leer). "
                        "Keine sichere Token-Identitaet moeglich."
                    )
                    meta["confidence"] = "high"

            behavior_type = "unknown_behavior"
            behavior_confidence = "low"
            txh_behavior = _norm(r.get("tx_hash")).lower()
            if chain_value and contract_value and txh_behavior:
                has_code = _has_chain_contract_code(chain_value, contract_value, code_cache)
                if has_code is True:
                    behavior_type, behavior_confidence = _classify_contract_behavior(
                        chain_value, txh_behavior, contract_value, receipt_cache
                    )

            resolution_decision, resolution_confidence = _behavior_to_decision(
                behavior_type, behavior_confidence, reason, static_resolution
            )
            meta["resolution_decision"] = resolution_decision
            meta["resolution_confidence"] = resolution_confidence
            if resolution_key:
                if not static_resolution:
                    inferred_entry = _infer_resolution_entry(
                        chain_value,
                        resolution_lookup_contract,
                        cp_protocol_value,
                        reason,
                        behavior_type,
                        behavior_confidence,
                    )
                    if (
                        _DYNAMIC_CONFIG_WRITES
                        and inferred_entry
                        and resolution_key not in contract_resolution_map
                    ):
                        contract_resolution_map[resolution_key] = inferred_entry
                        appended_resolution_entries += 1
                        static_resolution = inferred_entry
                        meta["contract_resolution_source"] = "inferred_append"
                        meta["contract_resolution"] = inferred_entry
                contract_behavior_rollup[resolution_key].append(behavior_type)
                if resolution_key not in contract_context:
                    contract_context[resolution_key] = {
                        "chain_id": chain_value,
                        "contract": resolution_lookup_contract,
                        "protocol": cp_protocol_value or None,
                    }

            meta["unknown_reason"] = reason
            meta["explanation"] = explanation
            meta["contract_behavior_type"] = behavior_type
            meta["behavior_confidence"] = behavior_confidence
            enriched_rows += 1
            breakdown_count[reason] += 1

            txh = _norm(r.get("tx_hash")).lower()
            tokens_in = meta.get("tokens_in") if isinstance(meta.get("tokens_in"), list) else []
            tokens_out = meta.get("tokens_out") if isinstance(meta.get("tokens_out"), list) else []

            amount_in_total = _sum_amounts(tokens_in)
            amount_out_total = _sum_amounts(tokens_out)
            in_known = _has_known_eur(tokens_in)
            out_known = _has_known_eur(tokens_out)
            known_eur_side = "none"
            if in_known and not out_known:
                known_eur_side = "in"
            elif out_known and not in_known:
                known_eur_side = "out"

            missing_side = _missing_side(tokens_in, tokens_out)
            can_be_derived = known_eur_side in {"in", "out"}
            swap_recovery_possible = (
                _norm(meta.get("type")).lower() == "swap"
                and can_be_derived
                and missing_side in {"in", "out"}
            )

            vol = econ_vol_by_tx.get(txh, 0.0) if txh else 0.0

            case = {
                "tx_hash": txh,
                "chain_id": _norm(r.get("chain_id") or meta.get("chain_id")),
                "protocol": _norm(meta.get("cp_protocol")),
                "contract": _norm(meta.get("cp_addr") or meta.get("token_contract")),
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "amount_in_total": round(amount_in_total, 12),
                "amount_out_total": round(amount_out_total, 12),
                "known_eur_side": known_eur_side,
                "unknown_reason": reason,
                "explanation": explanation,
                "confidence": "high",
                "can_be_derived": can_be_derived,
                "missing_side": missing_side,
                "swap_recovery_possible": swap_recovery_possible,
                "missing_eur": round(vol, 2),
                "contract_behavior_type": behavior_type,
                "behavior_confidence": behavior_confidence,
                "resolution_decision": resolution_decision,
                "resolution_confidence": resolution_confidence,
                "contract_resolution_source": _norm(meta.get("contract_resolution_source")) or None,
                "contract_resolution": meta.get("contract_resolution"),
            }
            cases.append(case)

            if txh:
                tx_reason.setdefault(txh, reason)
                tx_expl.setdefault(txh, explanation)
                if vol > 0:
                    wallet_missing_eur += vol
                    wallet_breakdown[reason] += vol
                    breakdown_eur[reason] += vol

            cp_addr = _norm(meta.get("cp_addr")).lower()
            cp_protocol = _norm(meta.get("cp_protocol")).lower()
            if cp_addr:
                top_contracts[cp_addr] += 1
                top_contracts_eur[cp_addr] += vol
            if cp_protocol:
                top_protocols[cp_protocol] += 1
                top_protocols_eur[cp_protocol] += vol

        # Enrich economic rows at tx-level for audit readability
        for e in economic:
            if not isinstance(e, dict):
                continue
            txh = _norm(e.get("tx_hash")).lower()
            if not txh:
                continue
            if txh in tx_reason:
                e["unknown_reason"] = tx_reason[txh]
                e["explanation"] = tx_expl[txh]

        _write_json(cls_path, classified)
        _write_json(econ_path, economic)

        summary_total_missing_eur += wallet_missing_eur

        wallet_summaries.append(
            {
                "wallet": wallet_dir.parent.name,
                "missing_eur": round(wallet_missing_eur, 2),
                "breakdown": {k: round(v, 2) for k, v in sorted(wallet_breakdown.items(), key=lambda x: x[1], reverse=True)},
            }
        )

    contract_resolution_suggestions: List[Dict[str, Any]] = []
    for key, behaviors in contract_behavior_rollup.items():
        total = len(behaviors)
        if total < 3:
            continue
        common_behavior, common_count = Counter(behaviors).most_common(1)[0]
        if common_count != total:
            continue
        ctx = contract_context.get(key, {})
        # Suggestion-only, no auto-write.
        suggested_entry = _infer_resolution_entry(
            _norm(ctx.get("chain_id")),
            _norm(ctx.get("contract")),
            _norm(ctx.get("protocol")),
            "unresolved",
            common_behavior,
            "high",
        )
        if not suggested_entry:
            continue
        contract_resolution_suggestions.append(
            {
                "key": key,
                "case_count": total,
                "consistent_behavior": common_behavior,
                "suggested_entry": suggested_entry,
                "note": "suggestion_only_no_auto_write",
            }
        )

    summary = {
        "total_missing_eur": round(summary_total_missing_eur, 2),
        "cases": cases,
        "breakdown": {
            k: {"count": breakdown_count[k], "missing_eur": round(breakdown_eur.get(k, 0.0), 2)}
            for k in sorted(breakdown_count.keys(), key=lambda key: breakdown_eur.get(key, 0.0), reverse=True)
        },
        "top_contracts": [
            {"contract": k, "count": v, "missing_eur": round(top_contracts_eur.get(k, 0.0), 2)}
            for k, v in top_contracts.most_common(20)
        ],
        "top_protocols": [
            {"protocol": k, "count": v, "missing_eur": round(top_protocols_eur.get(k, 0.0), 2)}
            for k, v in top_protocols.most_common(20)
        ],
        "enriched_rows": enriched_rows,
        "wallet_summaries": wallet_summaries,
        "contract_resolution_map_appended": appended_resolution_entries,
        "contract_resolution_suggestions": contract_resolution_suggestions,
    }

    out_path = out_audit_dir / "unknown_valuation_missing_summary.json"
    _write_json(out_path, summary)
    if _DYNAMIC_CONFIG_WRITES:
        _persist_contract_resolution_map(resolution_map_path, contract_resolution_map)
    print(f"[UNKNOWN_EXPLAIN] wrote {out_path}")
    print(f"[UNKNOWN_EXPLAIN] contract_resolution_map_appended={appended_resolution_entries}")
    print(f"[UNKNOWN_EXPLAIN] contract_resolution_suggestions={len(contract_resolution_suggestions)}")
    print(
        f"[UNKNOWN_EXPLAIN] enriched_rows={enriched_rows} "
        f"total_missing_eur={summary['total_missing_eur']}"
    )


if __name__ == "__main__":
    run()
