"""
File-based Unknown Registry.

Aggregates unknown patterns across many wallets without touching the pipeline.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional


@dataclass
class UnknownRegistry:
    methods: Dict[str, int]
    tokens: Dict[str, int]
    contracts: Dict[str, int]
    missing_price_tokens: Dict[str, int]
    unlabeled_contracts: Dict[str, int]
    ambiguous_transfers: Dict[str, Dict[str, int]]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "methods": self.methods,
            "tokens": self.tokens,
            "contracts": self.contracts,
            "missing_price_tokens": self.missing_price_tokens,
            "unlabeled_contracts": self.unlabeled_contracts,
            "ambiguous_transfers": self.ambiguous_transfers,
        }


def _inc(m: Dict[str, int], key: str, n: int = 1) -> None:
    if not key:
        return
    m[key] = int(m.get(key, 0) or 0) + int(n)


def load_registry(path: Path) -> UnknownRegistry:
    if not path.exists():
        return UnknownRegistry(
            methods={},
            tokens={},
            contracts={},
            missing_price_tokens={},
            unlabeled_contracts={},
            ambiguous_transfers={"by_protocol": {}, "by_method": {}, "by_contract": {}},
        )
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        data = {}
    amb = data.get("ambiguous_transfers") or {}
    by_protocol = dict(amb.get("by_protocol") or {})
    by_method = dict(amb.get("by_method") or {})
    by_contract = dict(amb.get("by_contract") or {})
    return UnknownRegistry(
        methods=dict(data.get("methods") or {}),
        tokens=dict(data.get("tokens") or {}),
        contracts=dict(data.get("contracts") or {}),
        missing_price_tokens=dict(data.get("missing_price_tokens") or {}),
        unlabeled_contracts=dict(data.get("unlabeled_contracts") or {}),
        ambiguous_transfers={"by_protocol": by_protocol, "by_method": by_method, "by_contract": by_contract},
    )


def save_registry(path: Path, reg: UnknownRegistry) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(reg.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def _extract_method(item: Dict[str, Any]) -> str:
    m = (item.get("method") or "").strip()
    if m:
        return m
    meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
    return (meta.get("method") or meta.get("function") or "").strip()


def _extract_contract_addr(item: Dict[str, Any]) -> str:
    meta = item.get("meta") if isinstance(item.get("meta"), dict) else {}
    addr = (meta.get("cp_addr") or meta.get("counterparty_addr") or meta.get("contract") or "").strip().lower()
    if addr.startswith("0x") and len(addr) >= 10:
        return addr
    return ""


def _extract_token(item: Dict[str, Any]) -> str:
    return (item.get("token") or "").strip().upper()


def update_registry_from_classified(
    reg: UnknownRegistry,
    classified_dicts: Iterable[Dict[str, Any]],
    *,
    collect_unknown_only: bool = True,
) -> UnknownRegistry:
    """
    Update registry counts in-place and return reg.

    Extraction rules:
    - If category == 'unknown': collect method/token/contract
    - Also collect:
      * tokens with missing price signal (eur_value missing/<=0 while amount>0)
      * contracts without label (cp_addr exists but cp_label empty)
    """
    for it in classified_dicts:
        if not isinstance(it, dict):
            continue

        cat = (it.get("category") or "").lower().strip()
        meta = it.get("meta") if isinstance(it.get("meta"), dict) else {}

        tok = _extract_token(it)
        method = _extract_method(it)
        caddr = _extract_contract_addr(it)

        if (not collect_unknown_only) or cat == "unknown":
            if method:
                _inc(reg.methods, method)
            if tok:
                _inc(reg.tokens, tok)
            if caddr:
                _inc(reg.contracts, caddr)

        # Missing price signal (without touching price logic): eur_value absent/0 while amount>0 and token present
        try:
            amt = abs(float(it.get("amount") or 0.0))
        except Exception:
            amt = 0.0
        try:
            eur_val = float(it.get("eur_value") or 0.0)
        except Exception:
            eur_val = 0.0
        if tok and amt > 0 and eur_val <= 0:
            _inc(reg.missing_price_tokens, tok)

        # Contracts without label
        cp_addr = (meta.get("cp_addr") or "").strip().lower()
        cp_label = (meta.get("cp_label") or "").strip()
        if cp_addr and not cp_label:
            _inc(reg.unlabeled_contracts, cp_addr)

        # Ambiguous transfers: category==transfer but has protocol/label/method context
        if cat == "transfer":
            cp_protocol = (meta.get("cp_protocol") or "").strip().lower()
            cp_label2 = (meta.get("cp_label") or "").strip()
            cp_addr2 = (meta.get("cp_addr") or "").strip().lower()
            method2 = _extract_method(it)

            if cp_protocol or cp_label2 or (method2 and method2 != "<empty>"):
                amb = reg.ambiguous_transfers
                if not isinstance(amb, dict):
                    reg.ambiguous_transfers = {"by_protocol": {}, "by_method": {}, "by_contract": {}}
                    amb = reg.ambiguous_transfers
                amb.setdefault("by_protocol", {})
                amb.setdefault("by_method", {})
                amb.setdefault("by_contract", {})
                if cp_protocol:
                    _inc(amb["by_protocol"], cp_protocol)
                if method2 and method2 != "<empty>":
                    _inc(amb["by_method"], method2)
                if cp_addr2:
                    _inc(amb["by_contract"], cp_addr2)

    return reg


def print_top(reg: UnknownRegistry, *, top_n: int = 20) -> None:
    def top_items(m: Dict[str, int]):
        return sorted(m.items(), key=lambda kv: (-kv[1], kv[0]))[:top_n]

    print("TOP UNKNOWN METHODS (top %d)" % top_n)
    for k, v in top_items(reg.methods):
        print(f"  {v:>6}  {k}")
    print()

    print("TOP UNKNOWN TOKENS (top %d)" % top_n)
    for k, v in top_items(reg.tokens):
        print(f"  {v:>6}  {k}")
    print()

    print("TOP UNKNOWN CONTRACTS (top %d)" % top_n)
    for k, v in top_items(reg.contracts):
        print(f"  {v:>6}  {k}")
    print()

    print("TOP TOKENS WITH MISSING PRICE SIGNAL (top %d)" % top_n)
    for k, v in top_items(reg.missing_price_tokens):
        print(f"  {v:>6}  {k}")
    print()

    print("TOP UNLABELED CONTRACTS (top %d)" % top_n)
    for k, v in top_items(reg.unlabeled_contracts):
        print(f"  {v:>6}  {k}")
    print()

    amb = reg.ambiguous_transfers if isinstance(reg.ambiguous_transfers, dict) else {}
    by_protocol = amb.get("by_protocol") or {}
    by_method = amb.get("by_method") or {}
    by_contract = amb.get("by_contract") or {}

    print("TOP AMBIGUOUS TRANSFERS — by protocol (top %d)" % top_n)
    for k, v in top_items(by_protocol):
        print(f"  {v:>6}  {k}")
    print()

    print("TOP AMBIGUOUS TRANSFERS — by method (top %d)" % top_n)
    for k, v in top_items(by_method):
        print(f"  {v:>6}  {k}")
    print()

    print("TOP AMBIGUOUS TRANSFERS — by contract (top %d)" % top_n)
    for k, v in top_items(by_contract):
        print(f"  {v:>6}  {k}")
    print()

