"""
Contract Profiler + Semi-automatic Label Suggestions (human-in-the-loop).

Reads:
  - taxtrack/data/registry/unknown_registry.json (unlabeled_contracts top N)
  - taxtrack/data/harvest/<wallet>/<year>/classified.json (harvested classified_dicts)

Writes:
  - taxtrack/data/registry/contract_suggestions.json
  - (optional, on approval) appends entries to taxtrack/data/config/address_map.json

Constraints:
  - Does NOT modify pipeline behavior, classification rules, swaps, prices, or gains.
  - No external APIs.
  - Deterministic heuristics only.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from collections import Counter, defaultdict


ALLOWED_CHAINS = {"eth", "arb", "base", "op", "avax", "matic", "bnb", "ftm"}


def _repo_taxtrack_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _registry_dir() -> Path:
    return _repo_taxtrack_root() / "data" / "registry"


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


@dataclass
class ContractProfile:
    address: str
    count: int
    chains: Dict[str, int]
    methods: List[Tuple[str, int]]
    tokens: List[Tuple[str, int]]
    direction_pattern: Dict[str, int]


def load_unlabeled_contracts_from_registry(registry_path: Path) -> Dict[str, int]:
    data = _load_json(registry_path, {}) or {}
    unlabeled = data.get("unlabeled_contracts") or {}
    out: Dict[str, int] = {}
    for k, v in unlabeled.items():
        if not isinstance(k, str):
            continue
        addr = k.strip().lower()
        if addr.startswith("0x") and len(addr) >= 10:
            try:
                out[addr] = int(v)
            except Exception:
                out[addr] = 0
    return out


def iter_harvested_classified(
    harvest_root: Path,
    year: int,
    *,
    wallet_filter: Optional[set[str]] = None,
) -> Iterable[Dict[str, Any]]:
    if not harvest_root.exists():
        return
    for wallet_dir in harvest_root.iterdir():
        if not wallet_dir.is_dir():
            continue
        wallet = wallet_dir.name.lower()
        if wallet_filter and wallet not in wallet_filter:
            continue
        year_dir = wallet_dir / str(year)
        if not year_dir.is_dir():
            continue
        classified_path = year_dir / "classified.json"
        if not classified_path.exists():
            continue
        try:
            rows = json.loads(classified_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(rows, list):
            continue
        for d in rows:
            if isinstance(d, dict):
                yield d


def _get_meta(d: Dict[str, Any]) -> Dict[str, Any]:
    return d.get("meta") if isinstance(d.get("meta"), dict) else {}


def _method_name(d: Dict[str, Any]) -> str:
    m = (d.get("method") or "").strip()
    if m:
        return m
    meta = _get_meta(d)
    return (meta.get("method") or meta.get("function") or "").strip()


def _token(d: Dict[str, Any]) -> str:
    return (d.get("token") or "").strip().upper()


def _direction(d: Dict[str, Any]) -> str:
    return (d.get("direction") or "").strip().lower()


def _chain_id(d: Dict[str, Any]) -> str:
    c = (d.get("chain_id") or "").strip().lower()
    return c if c in ALLOWED_CHAINS else ""


def _cp_addr(d: Dict[str, Any]) -> str:
    meta = _get_meta(d)
    a = (meta.get("cp_addr") or meta.get("counterparty_addr") or "").strip().lower()
    if a.startswith("0x") and len(a) >= 10:
        return a
    return ""


def profile_contract(address: str, rows: Iterable[Dict[str, Any]]) -> ContractProfile:
    addr = address.strip().lower()
    count = 0
    methods = Counter()
    tokens = Counter()
    dirs = Counter()
    chains = Counter()

    for d in rows:
        if _cp_addr(d) != addr:
            continue
        count += 1
        m = _method_name(d) or "<empty>"
        methods[m] += 1
        t = _token(d) or "<empty>"
        tokens[t] += 1
        dirs[_direction(d) or "<none>"] += 1
        ch = _chain_id(d)
        if ch:
            chains[ch] += 1

    return ContractProfile(
        address=addr,
        count=count,
        chains=dict(chains),
        methods=methods.most_common(10),
        tokens=tokens.most_common(10),
        direction_pattern={"in": int(dirs.get("in", 0)), "out": int(dirs.get("out", 0))},
    )


def _contains_any(haystack: str, needles: Iterable[str]) -> bool:
    h = haystack.lower()
    return any(n.lower() in h for n in needles)


def suggest_label(p: ContractProfile) -> Dict[str, Any]:
    """
    Deterministic heuristics (no ML).
    Returns suggestion dict with:
      suggested_protocol, type, confidence, evidence
    """
    methods = [m for m, _ in p.methods]
    tokens = [t for t, _ in p.tokens]
    m_join = " ".join(methods).lower()
    t_join = " ".join(tokens).lower()

    in_n = int(p.direction_pattern.get("in", 0))
    out_n = int(p.direction_pattern.get("out", 0))
    total = max(1, in_n + out_n)
    out_ratio = out_n / total
    in_ratio = in_n / total

    suggested_type = "unknown"
    suggested_protocol = ""
    confidence = 0.2
    evidence: Dict[str, Any] = {
        "count": p.count,
        "methods": p.methods,
        "tokens": p.tokens,
        "direction_pattern": p.direction_pattern,
        "chains": p.chains,
    }

    # 1) Bridge detection
    if _contains_any(m_join, ["bridge", "relay", "across", "gas zip", "gaszip"]):
        suggested_type = "bridge"
        suggested_protocol = "bridge"
        confidence = 0.85
        return {
            "suggested_protocol": suggested_protocol,
            "type": suggested_type,
            "confidence": confidence,
            "evidence": evidence,
        }

    # 2) Router detection
    if _contains_any(m_join, ["swap", "multicall", "execute", "exactinput", "exactoutput", "executeorder"]):
        suggested_type = "router"
        if _contains_any(m_join, ["exactinput", "exactoutput", "uniswap"]):
            suggested_protocol = "uniswap"
            confidence = 0.8
        elif _contains_any(m_join, ["executeorder", "aggregator", "1inch", "paraswap", "0x"]):
            suggested_protocol = "aggregator"
            confidence = 0.75
        else:
            suggested_protocol = "aggregator"
            confidence = 0.65
        return {
            "suggested_protocol": suggested_protocol,
            "type": suggested_type,
            "confidence": confidence,
            "evidence": evidence,
        }

    # 3) Reward contract detection
    if in_ratio >= 0.8 and _contains_any(m_join, ["claim", "harvest", "reward"]):
        suggested_type = "reward"
        if _contains_any(t_join, ["moo", "beefy"]):
            suggested_protocol = "beefy"
            confidence = 0.8
        else:
            suggested_protocol = ""
            confidence = 0.65
        return {
            "suggested_protocol": suggested_protocol,
            "type": suggested_type,
            "confidence": confidence,
            "evidence": evidence,
        }

    # 4) Vault detection
    if _contains_any(m_join, ["deposit", "withdraw", "stake", "unstake", "redeem", "supply"]):
        if out_ratio >= 0.6:
            suggested_type = "vault"
            if _contains_any(t_join, ["moo", "beefy"]):
                suggested_protocol = "beefy"
                confidence = 0.8
            elif _contains_any(m_join, ["borrow", "repay", "supply", "aave"]):
                suggested_protocol = "aave"
                confidence = 0.75
            else:
                suggested_protocol = ""
                confidence = 0.6
        else:
            suggested_type = "vault"
            suggested_protocol = ""
            confidence = 0.55
        return {
            "suggested_protocol": suggested_protocol,
            "type": suggested_type,
            "confidence": confidence,
            "evidence": evidence,
        }

    return {
        "suggested_protocol": suggested_protocol,
        "type": suggested_type,
        "confidence": confidence,
        "evidence": evidence,
    }


def build_suggestions(
    *,
    year: int,
    top_n: int,
    registry_path: Path,
    harvest_root: Path,
    wallet_filter: Optional[set[str]] = None,
) -> Tuple[List[ContractProfile], Dict[str, Any]]:
    unlabeled = load_unlabeled_contracts_from_registry(registry_path)
    top_addrs = [a for a, _ in sorted(unlabeled.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]]
    rows = list(iter_harvested_classified(harvest_root, year, wallet_filter=wallet_filter))

    profiles: List[ContractProfile] = []
    suggestions: Dict[str, Any] = {}
    for addr in top_addrs:
        prof = profile_contract(addr, rows)
        profiles.append(prof)
        suggestions[addr] = suggest_label(prof)
    return profiles, suggestions


def _choose_chain(p: ContractProfile) -> str:
    if not p.chains:
        return "eth"
    return sorted(p.chains.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


def append_to_address_map(
    *,
    chain: str,
    addr: str,
    label: str,
    protocol: str,
    type_: str,
    tags: Optional[List[str]] = None,
    address_map_path: Path,
) -> bool:
    chain = (chain or "eth").lower()
    addr = addr.lower()
    data = _load_json(address_map_path, {}) or {}
    if chain not in data or not isinstance(data.get(chain), dict):
        data[chain] = {}
    if addr in data[chain]:
        return False
    data[chain][addr] = {
        "label": label,
        "protocol": protocol,
        "type": type_,
        "tags": tags or [],
    }
    _save_json(address_map_path, data)
    return True


def main() -> None:
    p = argparse.ArgumentParser(description="Profile top unlabeled contracts and suggest labels.")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--top", type=int, default=20)
    p.add_argument("--registry", default=None, help="Path to unknown_registry.json")
    p.add_argument("--harvest-root", default=None, help="Path to data/harvest")
    p.add_argument("--wallet-list", default=None, help="Optional wallet_list.txt to restrict profiles")
    p.add_argument("--write-suggestions", action="store_true", help="Write contract_suggestions.json")
    p.add_argument("--interactive", action="store_true", help="Prompt to accept/edit and append to address_map.json")
    args = p.parse_args()

    registry_path = Path(args.registry) if args.registry else (_registry_dir() / "unknown_registry.json")
    harvest_root = Path(args.harvest_root) if args.harvest_root else _harvest_root()
    wallet_filter = None
    if args.wallet_list:
        wl = Path(args.wallet_list)
        if wl.exists():
            wallet_filter = {l.strip().lower() for l in wl.read_text(encoding="utf-8", errors="replace").splitlines() if l.strip() and not l.strip().startswith("#")}

    profiles, suggestions = build_suggestions(
        year=int(args.year),
        top_n=int(args.top),
        registry_path=registry_path,
        harvest_root=harvest_root,
        wallet_filter=wallet_filter,
    )

    out_path = _registry_dir() / "contract_suggestions.json"
    if args.write_suggestions:
        _save_json(out_path, suggestions)
        print(f"[SUGGESTIONS] wrote {out_path}")

    print(f"CONTRACT PROFILER (top {len(profiles)})")
    for prof in profiles:
        sug = suggestions.get(prof.address) or {}
        print()
        print(f"Address: {prof.address}")
        print(f"Count: {prof.count}")
        print(f"Chains: {prof.chains}")
        print(f"Suggested protocol: {sug.get('suggested_protocol') or '<none>'}")
        print(f"Suggested type: {sug.get('type')}")
        print(f"Confidence: {sug.get('confidence')}")
        print(f"Top methods: {prof.methods}")
        print(f"Top tokens: {prof.tokens}")
        print(f"Direction: {prof.direction_pattern}")

        if not args.interactive:
            continue

        # Manual review loop
        chain = _choose_chain(prof)
        protocol = (sug.get("suggested_protocol") or "").strip() or "unknown"
        type_ = (sug.get("type") or "").strip() or "unknown"
        label = f"{protocol} {type_}".strip()
        while True:
            ans = input("Accept? (y/n/edit) ").strip().lower()
            if ans in ("n", "no", ""):
                break
            if ans in ("y", "yes"):
                ok = append_to_address_map(
                    chain=chain,
                    addr=prof.address,
                    label=label,
                    protocol=protocol,
                    type_=type_,
                    tags=[],
                    address_map_path=_address_map_path(),
                )
                if ok:
                    print(f"[ACCEPTED] added to address_map.json under chain={chain}")
                else:
                    print(f"[SKIP] already exists in address_map.json (chain={chain})")
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

