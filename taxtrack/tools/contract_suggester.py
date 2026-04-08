"""
Contract Label Auto-Suggester (with confidence scoring).

Reads:
  - taxtrack/data/registry/unknown_registry.json:
      * ambiguous_transfers.by_contract (top N)
      * unlabeled_contracts
  - harvested classified data:
      taxtrack/data/harvest/<wallet>/<year>/classified.json

Writes:
  - taxtrack/data/registry/contract_suggestions.json

Optional interactive mode:
  - prompts user to accept/edit and appends to taxtrack/data/config/address_map.json

Constraints:
  - Does NOT modify classification logic, swap logic, pricing, or pipeline behavior.
  - No external APIs, no ML; deterministic heuristics only.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from collections import Counter, defaultdict


ALLOWED_CHAINS = {"eth", "arb", "base", "op", "avax", "matic", "bnb", "ftm"}
STABLECOINS = {"USDC", "USDT", "DAI", "TUSD", "USDE", "USD", "USDC.E", "USDCE", "USDT.E", "USDT0"}


def _repo_taxtrack_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _registry_path() -> Path:
    return _repo_taxtrack_root() / "data" / "registry" / "unknown_registry.json"


def _suggestions_path() -> Path:
    return _repo_taxtrack_root() / "data" / "registry" / "contract_suggestions.json"


def _harvest_root() -> Path:
    return _repo_taxtrack_root() / "data" / "harvest"


def _address_map_path() -> Path:
    return _repo_taxtrack_root() / "data" / "config" / "address_map.json"


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def _is_addr(s: str) -> bool:
    return isinstance(s, str) and s.lower().startswith("0x") and len(s.strip()) >= 10


def _meta(d: Dict[str, Any]) -> Dict[str, Any]:
    return d.get("meta") if isinstance(d.get("meta"), dict) else {}


def _cp_addr(d: Dict[str, Any]) -> str:
    m = _meta(d)
    a = (m.get("cp_addr") or m.get("counterparty_addr") or "").strip().lower()
    return a if _is_addr(a) else ""


def _method(d: Dict[str, Any]) -> str:
    m = (d.get("method") or "").strip()
    if m:
        return m
    mm = _meta(d).get("method") or ""
    return str(mm).strip()


def _token(d: Dict[str, Any]) -> str:
    return (d.get("token") or "").strip().upper()


def _direction(d: Dict[str, Any]) -> str:
    return (d.get("direction") or "").strip().lower()


def _chain(d: Dict[str, Any]) -> str:
    c = (d.get("chain_id") or "").strip().lower()
    return c if c in ALLOWED_CHAINS else ""


def _tx_hash(d: Dict[str, Any]) -> str:
    return (d.get("tx_hash") or "").strip().lower()


def _dt_iso(d: Dict[str, Any]) -> str:
    return (d.get("dt_iso") or "").strip()


def _ts(d: Dict[str, Any]) -> int:
    dt = _dt_iso(d)
    if not dt:
        return 0
    try:
        return int(datetime.fromisoformat(dt.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def iter_harvested_classified(year: int) -> Iterable[Dict[str, Any]]:
    root = _harvest_root()
    if not root.exists():
        return
    for wallet_dir in root.iterdir():
        if not wallet_dir.is_dir():
            continue
        year_dir = wallet_dir / str(year)
        if not year_dir.is_dir():
            continue
        p = year_dir / "classified.json"
        if not p.exists():
            continue
        try:
            rows = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(rows, list):
            continue
        for d in rows:
            if isinstance(d, dict):
                yield d


def _top_n_keys(counter_map: Dict[str, int], n: int) -> List[str]:
    return [k for k, _v in sorted(counter_map.items(), key=lambda kv: (-int(kv[1]), kv[0]))[:n]]


def load_target_contracts(top_n: int) -> List[str]:
    reg = _load_json(_registry_path(), {}) or {}
    unlabeled = reg.get("unlabeled_contracts") or {}
    amb = (reg.get("ambiguous_transfers") or {}).get("by_contract") or {}

    score = Counter()
    for k, v in unlabeled.items():
        if isinstance(k, str) and _is_addr(k):
            try:
                score[k.lower()] += int(v)
            except Exception:
                pass
    for k, v in amb.items():
        if isinstance(k, str) and _is_addr(k):
            try:
                score[k.lower()] += int(v)
            except Exception:
                pass

    return [k for k, _v in score.most_common(top_n)]


@dataclass
class ContractFeatures:
    address: str
    total_interactions: int
    unique_methods: List[str]
    method_frequency: List[Tuple[str, int]]
    tokens_used: List[str]
    direction_ratio: Dict[str, float]
    chains: Dict[str, int]
    avg_value_eur: Optional[float]
    mixed_in_out_tx_ratio: float
    token_cross_chain: bool
    out_then_in_later: bool
    small_incoming_repeat: bool


def build_features(addr: str, rows: Iterable[Dict[str, Any]]) -> ContractFeatures:
    addr_l = addr.lower()
    total = 0
    methods = Counter()
    tokens = Counter()
    dirs = Counter()
    chains = Counter()
    eur_vals: List[float] = []
    by_tx_dirs: Dict[str, set] = defaultdict(set)
    token_chains: Dict[str, set] = defaultdict(set)
    token_out_ts: Dict[str, int] = {}
    token_in_after_out = False
    small_in = 0

    for d in rows:
        if _cp_addr(d) != addr_l:
            continue
        total += 1
        m = _method(d) or "<empty>"
        methods[m] += 1
        t = _token(d) or "<empty>"
        tokens[t] += 1
        di = _direction(d) or "<none>"
        dirs[di] += 1
        ch = _chain(d)
        if ch:
            chains[ch] += 1
            token_chains[t].add(ch)
        txh = _tx_hash(d)
        if txh:
            by_tx_dirs[txh].add(di)

        # avg eur value (optional)
        try:
            eur = float(d.get("eur_value") or 0.0)
            if eur > 0:
                eur_vals.append(eur)
        except Exception:
            pass

        # bridge-ish out->in later signal per token
        ts = _ts(d)
        if ts > 0 and di == "out":
            prev = token_out_ts.get(t)
            if prev is None or ts < prev:
                token_out_ts[t] = ts
        if ts > 0 and di == "in":
            out_ts = token_out_ts.get(t)
            if out_ts is not None and ts > out_ts:
                token_in_after_out = True

        # small repeated incoming (reward-ish)
        if di == "in":
            try:
                eur = float(d.get("eur_value") or 0.0)
            except Exception:
                eur = 0.0
            if 0 < eur < 10:
                small_in += 1

    in_n = int(dirs.get("in", 0))
    out_n = int(dirs.get("out", 0))
    denom = max(1, in_n + out_n)
    in_ratio = in_n / denom
    out_ratio = out_n / denom

    mixed_tx = 0
    for _tx, s in by_tx_dirs.items():
        if "in" in s and "out" in s:
            mixed_tx += 1
    mixed_ratio = (mixed_tx / max(1, len(by_tx_dirs))) if by_tx_dirs else 0.0

    token_cross_chain = any(len(chs) >= 2 for chs in token_chains.values() if chs)
    avg_value = (sum(eur_vals) / len(eur_vals)) if eur_vals else None
    small_incoming_repeat = (small_in >= 5 and in_ratio >= 0.7)

    return ContractFeatures(
        address=addr_l,
        total_interactions=total,
        unique_methods=sorted([m for m, _ in methods.items() if m and m != "<empty>"]),
        method_frequency=methods.most_common(5),
        tokens_used=sorted([t for t, _ in tokens.items() if t and t != "<empty>"]),
        direction_ratio={"out": out_ratio, "in": in_ratio},
        chains=dict(chains),
        avg_value_eur=avg_value,
        mixed_in_out_tx_ratio=mixed_ratio,
        token_cross_chain=bool(token_cross_chain),
        out_then_in_later=bool(token_in_after_out),
        small_incoming_repeat=bool(small_incoming_repeat),
    )


def _has_method(f: ContractFeatures, needles: Iterable[str]) -> bool:
    hay = " ".join([m for m, _ in f.method_frequency]).lower()
    return any(n.lower() in hay for n in needles)


def _has_token_family(f: ContractFeatures) -> bool:
    toks = set(f.tokens_used)
    if toks.intersection(STABLECOINS):
        return True
    # LP / vault style tokens
    for t in toks:
        tu = t.upper()
        if any(x in tu for x in ("LP", "LPT", "MOO", "BEEFY", "RCOW")):
            return True
    return False


def score_vault(f: ContractFeatures) -> Tuple[float, str]:
    score = 0.0
    if _has_method(f, ["deposit", "withdraw", "stake"]):
        score += 0.4
    if f.direction_ratio["out"] > 0.6:
        score += 0.2
    if _has_token_family(f):
        score += 0.2
    if f.total_interactions >= 5:
        score += 0.2

    proto = "generic_vault"
    tjoin = " ".join(f.tokens_used).lower()
    mjoin = " ".join([m for m, _ in f.method_frequency]).lower()
    if "moo" in tjoin or "beefy" in tjoin:
        proto = "beefy"
    elif "aave" in mjoin or any(x in mjoin for x in ["borrow", "repay", "supply"]):
        proto = "aave"
    return min(1.0, score), proto


def score_router(f: ContractFeatures) -> Tuple[float, str]:
    score = 0.0
    if _has_method(f, ["swap", "multicall", "execute", "exactinput"]):
        score += 0.4
    if f.mixed_in_out_tx_ratio >= 0.3:
        score += 0.3
    if len(f.tokens_used) >= 5:
        score += 0.2

    proto = "aggregator"
    mjoin = " ".join([m for m, _ in f.method_frequency]).lower()
    if "exactinput" in mjoin or "exactoutput" in mjoin:
        proto = "uniswap"
    return min(1.0, score), proto


def score_bridge(f: ContractFeatures) -> Tuple[float, str]:
    score = 0.0
    if _has_method(f, ["bridge", "relay", "across", "gaszip", "gas zip"]):
        score += 0.5
    if f.token_cross_chain:
        score += 0.3
    if f.out_then_in_later:
        score += 0.2
    return min(1.0, score), "bridge"


def score_reward(f: ContractFeatures) -> Tuple[float, str]:
    score = 0.0
    if _has_method(f, ["claim", "harvest", "reward"]):
        score += 0.4
    if f.direction_ratio["in"] > 0.7:
        score += 0.3
    if f.small_incoming_repeat:
        score += 0.2
    return min(1.0, score), "reward"


def suggest_for_contract(f: ContractFeatures) -> Dict[str, Any]:
    vault_score, vault_proto = score_vault(f)
    router_score, router_proto = score_router(f)
    bridge_score, bridge_proto = score_bridge(f)
    reward_score, reward_proto = score_reward(f)

    candidates = [
        ("vault", vault_score, vault_proto),
        ("router", router_score, router_proto),
        ("bridge", bridge_score, bridge_proto),
        ("reward", reward_score, reward_proto),
    ]
    candidates.sort(key=lambda x: (-x[1], x[0]))
    best_type, best_score, best_proto = candidates[0]

    suggestion: Dict[str, Any] = {
        "suggested_protocol": best_proto,
        "type": best_type if best_score >= 0.5 else "uncertain",
        "confidence": float(best_score),
        "signals": {
            "methods": [m for m, _ in f.method_frequency],
            "method_frequency": f.method_frequency,
            "tokens": f.tokens_used[:20],
            "direction_ratio": f.direction_ratio,
            "total_interactions": f.total_interactions,
            "mixed_in_out_tx_ratio": f.mixed_in_out_tx_ratio,
            "chains": f.chains,
        },
        "scores": {
            "vault": vault_score,
            "router": router_score,
            "bridge": bridge_score,
            "reward": reward_score,
        },
    }
    return suggestion


def build_suggestions(year: int, top_n: int) -> Tuple[Dict[str, ContractFeatures], Dict[str, Any]]:
    targets = load_target_contracts(top_n)
    rows = list(iter_harvested_classified(year))
    features: Dict[str, ContractFeatures] = {}
    suggestions: Dict[str, Any] = {}
    for addr in targets:
        f = build_features(addr, rows)
        features[addr] = f
        suggestions[addr] = suggest_for_contract(f)
    return features, suggestions


def _addr_exists_anywhere(address_map: Dict[str, Any], addr: str) -> bool:
    a = addr.lower()
    for _chain, items in (address_map or {}).items():
        if isinstance(items, dict) and a in {k.lower(): True for k in items.keys()}.keys():
            return True
    return False


def append_address_map(
    *,
    chain: str,
    addr: str,
    label: str,
    protocol: str,
    type_: str,
    address_map_path: Path,
) -> bool:
    data = _load_json(address_map_path, {}) or {}
    c = (chain or "eth").lower()
    a = addr.lower()
    if _addr_exists_anywhere(data, a):
        return False
    if c not in data or not isinstance(data.get(c), dict):
        data[c] = {}
    data[c][a] = {"label": label, "protocol": protocol, "type": type_, "tags": []}
    _save_json(address_map_path, data)
    return True


def _choose_chain(f: ContractFeatures) -> str:
    if not f.chains:
        return "eth"
    return sorted(f.chains.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def main() -> None:
    p = argparse.ArgumentParser(description="Suggest contract labels with confidence scoring.")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--interactive", action="store_true")
    p.add_argument("--min-confidence", type=float, default=0.5)
    args = p.parse_args()

    features, suggestions = build_suggestions(int(args.year), int(args.top))
    _save_json(_suggestions_path(), suggestions)
    print(f"[SUGGESTIONS] wrote {_suggestions_path()}")

    # Print suggestions, only confident ones by default
    for addr, sug in suggestions.items():
        conf = float(sug.get("confidence") or 0.0)
        if conf < float(args.min_confidence):
            continue
        f = features.get(addr)
        print()
        print(f"Address: {addr}")
        print(f"Suggested: {sug.get('suggested_protocol')} ({sug.get('type')})")
        print(f"Confidence: {conf:.2f}")
        sig = sug.get("signals") or {}
        print("Evidence:")
        print(f"  total_interactions: {sig.get('total_interactions')}")
        print(f"  methods: {sig.get('method_frequency')}")
        print(f"  tokens: {sig.get('tokens')}")
        print(f"  direction: {sig.get('direction_ratio')}")
        print(f"  chains: {sig.get('chains')}")
        print(f"  mixed_in_out_tx_ratio: {sig.get('mixed_in_out_tx_ratio')}")

        if not args.interactive or not f:
            continue

        chain = _choose_chain(f)
        protocol = (sug.get("suggested_protocol") or "").strip().lower() or "unknown"
        type_ = (sug.get("type") or "").strip().lower()
        label = f"{protocol}_{type_}".strip("_")

        while True:
            ans = input("Accept? (y/n/edit) ").strip().lower()
            if ans in ("n", "no", ""):
                break
            if ans in ("y", "yes"):
                ok = append_address_map(
                    chain=chain,
                    addr=addr,
                    label=label,
                    protocol=protocol,
                    type_=type_,
                    address_map_path=_address_map_path(),
                )
                if ok:
                    print(f"[ACCEPTED] added to address_map.json under chain={chain}")
                else:
                    print("[SKIP] address already labeled (not overwritten)")
                break
            if ans.startswith("e"):
                chain = input(f"chain [{chain}]: ").strip().lower() or chain
                protocol = input(f"protocol [{protocol}]: ").strip().lower() or protocol
                type_ = input(f"type [{type_}]: ").strip().lower() or type_
                label = input(f"label [{label}]: ").strip() or label
                continue
            print("Please enter y/n/edit.")


if __name__ == "__main__":
    main()

