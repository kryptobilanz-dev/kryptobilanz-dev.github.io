"""
Value coverage self-healing loop.

Goal:
- Reduce missing price tokens to <= 10
- Or reach price coverage >= 90%
- Or stop when improvement < 1% for 3 iterations

Strict scope:
- token mapping
- contract mapping
- safe normalization
"""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


MAX_ITERATIONS = 50
YEAR = 2025
CHAINS = "eth"
WALLET_LIST = "wallet_list_loop_runtime.txt"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _taxtrack_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _loop_dir() -> Path:
    p = _taxtrack_root() / "data" / "loop"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _harvest_root() -> Path:
    return _taxtrack_root() / "data" / "harvest"


def _registry_path() -> Path:
    return _taxtrack_root() / "data" / "registry" / "unknown_registry.json"


def _address_map_path() -> Path:
    return _taxtrack_root() / "data" / "config" / "address_map.json"


def _auto_token_map_path() -> Path:
    return _taxtrack_root() / "data" / "config" / "auto_token_map.json"


def _token_price_map_path() -> Path:
    return _taxtrack_root() / "data" / "config" / "token_price_mapping.json"


def _log_path() -> Path:
    return _loop_dir() / "value_coverage_log.json"


def _report_path() -> Path:
    return _loop_dir() / "value_coverage_final.md"


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _save_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _run_cmd(cmd: List[str], cwd: Path) -> Tuple[int, str]:
    p = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )
    out = (p.stdout or "") + ("\n" if p.stdout and p.stderr else "") + (p.stderr or "")
    return p.returncode, out


def _is_price_missing(row: dict) -> bool:
    try:
        eur = row.get("eur_value")
    except Exception:
        eur = None
    valuation_missing = bool(row.get("valuation_missing"))
    conf = str(row.get("price_confidence") or "").strip().lower()
    eur_bad = eur is None
    if not eur_bad:
        try:
            eur_bad = float(eur) <= 0
        except Exception:
            eur_bad = True
    return eur_bad or valuation_missing or conf == "low"


def _iter_classified_rows(year: int) -> Iterable[dict]:
    root = _harvest_root()
    if not root.exists():
        return []
    out: List[dict] = []
    for wallet in root.iterdir():
        if not wallet.is_dir():
            continue
        p = wallet / str(year) / "classified.json"
        if not p.exists():
            continue
        rows = _load_json(p, [])
        if isinstance(rows, list):
            for r in rows:
                if isinstance(r, dict):
                    out.append(r)
    return out


@dataclass
class CoverageStats:
    total_rows: int
    resolved_rows: int
    missing_rows: int
    missing_tokens: Dict[str, int]
    price_conf_dist: Dict[str, int]
    token_contracts: Dict[str, Dict[str, int]]


def _collect_coverage_stats(year: int) -> CoverageStats:
    total = 0
    resolved = 0
    missing = 0
    missing_tokens: Dict[str, int] = {}
    conf_dist: Dict[str, int] = {}
    token_contracts: Dict[str, Dict[str, int]] = {}

    for r in _iter_classified_rows(year):
        total += 1
        conf = str(r.get("price_confidence") or "none").strip().lower() or "none"
        conf_dist[conf] = conf_dist.get(conf, 0) + 1

        token = str(r.get("token") or "").strip().upper()
        meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
        contract = str(meta.get("cp_addr") or r.get("to_addr") or "").strip().lower()
        if token:
            token_contracts.setdefault(token, {})
            if contract.startswith("0x") and len(contract) >= 10:
                token_contracts[token][contract] = token_contracts[token].get(contract, 0) + 1

        if _is_price_missing(r):
            missing += 1
            if token:
                missing_tokens[token] = missing_tokens.get(token, 0) + 1
        else:
            resolved += 1

    return CoverageStats(
        total_rows=total,
        resolved_rows=resolved,
        missing_rows=missing,
        missing_tokens=missing_tokens,
        price_conf_dist=conf_dist,
        token_contracts=token_contracts,
    )


def _coverage(stats: CoverageStats) -> float:
    if stats.total_rows <= 0:
        return 0.0
    return float(stats.resolved_rows) / float(stats.total_rows)


def _blocked_symbol(token: str) -> bool:
    t = token.upper()
    if t in {"UNKNOWN", ""}:
        return True
    if "-" in t:
        return True
    if "_LPT" in t or "LP" in t or "VAULT" in t or "POOL" in t:
        return True
    if t.startswith("MOO"):
        return True
    if t in {"GMX", "MOO", "AAVE", "COMP", "CRV", "UNI", "SUSHI", "BAL", "PENDLE", "MORPHO"}:
        return True
    return False


def _load_existing_token_maps() -> Tuple[Dict[str, str], Dict[str, str]]:
    auto = _load_json(_auto_token_map_path(), {"token_map": {}})
    auto_map_raw = auto.get("token_map") if isinstance(auto, dict) else {}
    auto_map: Dict[str, str] = {}
    if isinstance(auto_map_raw, dict):
        for k, v in auto_map_raw.items():
            auto_map[str(k).upper()] = str(v).upper()

    cg = _load_json(_token_price_map_path(), {})
    cg_map: Dict[str, str] = {}
    if isinstance(cg, dict):
        for k, v in cg.items():
            cg_map[str(k).upper()] = str(v).lower()
    return auto_map, cg_map


def _safe_normalization_candidate(symbol: str) -> str | None:
    s = symbol.upper()
    if s in {"WETH", "WETHE", "WETH.E", "ETH.E"}:
        return "ETH"
    if s in {"WAVAX", "WAVAX.E"}:
        return "AVAX"
    if s in {"WBTC", "WBTC.E"}:
        return "BTC"
    if s.endswith(".E"):
        root = s[:-2]
        if root in {"USDC", "USDT", "DAI", "USDCE", "USDT0"}:
            return "USD"
    if s.startswith("W") and len(s) > 1 and s[1:] in {"ETH", "BTC", "AVAX", "BNB", "MATIC", "FTM"}:
        return s[1:]
    return None


def _deterministic_coingecko_id_for(symbol: str) -> str | None:
    # deterministic static map only (no search/guessing)
    fixed = {
        "ETH": "ethereum",
        "BTC": "bitcoin",
        "AVAX": "avalanche-2",
        "ARB": "arbitrum",
        "OP": "optimism",
        "BNB": "binancecoin",
        "MATIC": "matic-network",
        "POL": "matic-network",
        "FTM": "fantom",
        "ATOM": "cosmos",
        "SOL": "solana",
        "INJ": "injective-protocol",
        "TIA": "celestia",
        "SUI": "sui",
        "OSMO": "osmosis",
        "JITO": "jito-governance-token",
        "STRD": "stride",
        "STARS": "stargaze",
    }
    return fixed.get(symbol.upper())


def _load_address_maps_to() -> Dict[str, str]:
    out: Dict[str, str] = {}
    data = _load_json(_address_map_path(), {})
    if not isinstance(data, dict):
        return out
    eth = data.get("eth")
    if not isinstance(eth, dict):
        return out
    for addr, meta in eth.items():
        if isinstance(meta, dict):
            mt = str(meta.get("maps_to") or "").strip().upper()
            if mt:
                out[str(addr).lower()] = mt
    return out


def _append_auto_token_map(new_map: Dict[str, str]) -> List[dict]:
    if not new_map:
        return []
    data = _load_json(_auto_token_map_path(), {"meta": {}, "token_map": {}})
    if not isinstance(data, dict):
        data = {"meta": {}, "token_map": {}}
    token_map = data.get("token_map")
    if not isinstance(token_map, dict):
        token_map = {}
    added: List[dict] = []
    for src, dst in sorted(new_map.items()):
        u = src.upper()
        if u in token_map:
            continue
        token_map[u] = dst.upper()
        added.append({"target": "auto_token_map.json", "source": u, "maps_to": dst.upper(), "confidence": 1.0})
    data["token_map"] = dict(sorted(token_map.items(), key=lambda kv: kv[0]))
    data["meta"] = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    data["meta"]["updated_at_utc"] = _now_utc()
    data["meta"]["updated_by"] = "value_coverage_loop"
    _save_json(_auto_token_map_path(), data)
    return added


def _append_token_price_mapping(new_map: Dict[str, str]) -> List[dict]:
    if not new_map:
        return []
    data = _load_json(_token_price_map_path(), {})
    if not isinstance(data, dict):
        data = {}
    added: List[dict] = []
    for sym, cg_id in sorted(new_map.items()):
        k = sym.lower()
        if k in data:
            continue
        data[k] = cg_id
        added.append({"target": "token_price_mapping.json", "symbol": sym.upper(), "coingecko_id": cg_id, "confidence": 1.0})
    _save_json(_token_price_map_path(), dict(sorted(data.items(), key=lambda kv: kv[0])))
    return added


def _append_address_map_entries(new_contract_map: Dict[str, str]) -> List[dict]:
    if not new_contract_map:
        return []
    data = _load_json(_address_map_path(), {})
    if not isinstance(data, dict):
        data = {}
    eth = data.get("eth")
    if not isinstance(eth, dict):
        eth = {}
        data["eth"] = eth
    added: List[dict] = []
    for addr, maps_to in sorted(new_contract_map.items()):
        a = addr.lower()
        if a in eth:
            continue
        eth[a] = {
            "label": f"auto_price_map_{maps_to.lower()}",
            "protocol": "auto_resolved",
            "type": "token",
            "maps_to": maps_to.upper(),
            "confidence": 1.0,
            "source": "value_coverage_loop",
        }
        added.append({"target": "address_map.json", "contract": a, "maps_to": maps_to.upper(), "confidence": 1.0})
    _save_json(_address_map_path(), data)
    return added


def _reason_unresolved(token: str, freq: int, contracts: Dict[str, int]) -> str:
    if _blocked_symbol(token):
        return "blocked by strict filter (LP/UNKNOWN/protocol/multi-symbol)"
    if freq < 5:
        return "frequency < 5"
    if len(contracts) != 1:
        return "maps to multiple/no contracts"
    if _safe_normalization_candidate(token) is None and _deterministic_coingecko_id_for(token) is None:
        return "no deterministic market mapping"
    return "requires manual review"


def run_loop() -> None:
    repo = _repo_root()
    log_rows: List[dict] = []

    missing_prev: int | None = None
    start_cov: float | None = None
    no_improve_streak = 0
    all_added: List[dict] = []

    print("VALUE COVERAGE LOOP STARTED", flush=True)

    for i in range(1, MAX_ITERATIONS + 1):
        # 1) batch runner
        cmd_a = [
            "python", "-m", "taxtrack.tools.batch_runner",
            "--wallet-list", WALLET_LIST,
            "--year", str(YEAR),
            "--chains", CHAINS,
            "--top", "200",
        ]
        ec_a, out_a = _run_cmd(cmd_a, repo)
        (_loop_dir() / f"value_coverage_iter_{i:02d}_batch_runner.log").write_text(out_a, encoding="utf-8")

        # 2) unknown report
        out_report = _loop_dir() / f"self_healing_iter_{i}.md"
        cmd_b = ["python", "-m", "taxtrack.tools.unknown_classifier_report", "--year", str(YEAR), "--out", str(out_report)]
        ec_b, out_b = _run_cmd(cmd_b, repo)
        (_loop_dir() / f"value_coverage_iter_{i:02d}_unknown_classifier.log").write_text(out_b, encoding="utf-8")

        # 3) parse coverage/missing
        stats = _collect_coverage_stats(YEAR)
        coverage = _coverage(stats)
        missing_count = int(stats.missing_rows)
        if start_cov is None:
            start_cov = coverage

        # 4) apply deterministic improvements
        existing_auto, existing_cg = _load_existing_token_maps()
        addr_maps_to = _load_address_maps_to()

        to_auto_map: Dict[str, str] = {}
        to_cg_map: Dict[str, str] = {}
        to_addr_map: Dict[str, str] = {}

        for tok, freq in sorted(stats.missing_tokens.items(), key=lambda kv: (-kv[1], kv[0])):
            token = tok.upper()
            if _blocked_symbol(token):
                continue
            if freq < 5:
                continue

            contracts = stats.token_contracts.get(token, {})
            if len(contracts) != 1:
                continue
            only_contract = next(iter(contracts.keys()))

            # CASE 3: contract has maps_to -> ensure valuation uses it via auto token map
            if only_contract in addr_maps_to:
                mapped = addr_maps_to[only_contract].upper()
                if token not in existing_auto:
                    to_auto_map[token] = mapped
                continue

            # CASE 2: safe wrapped/variant normalization
            norm = _safe_normalization_candidate(token)
            if norm:
                if token not in existing_auto:
                    to_auto_map[token] = norm
                # also ensure contract map exists for deterministic contract-based reuse
                if only_contract not in addr_maps_to:
                    to_addr_map[only_contract] = norm
                # if normalized target has deterministic cg id, ensure cg mapping too
                cg_id = _deterministic_coingecko_id_for(norm)
                if cg_id and norm not in existing_cg:
                    to_cg_map[norm] = cg_id
                continue

            # CASE 1: known token but missing price -> deterministic Coingecko mapping
            cg_id = _deterministic_coingecko_id_for(token)
            if cg_id and token not in existing_cg:
                to_cg_map[token] = cg_id
                if only_contract not in addr_maps_to:
                    to_addr_map[only_contract] = token

        added = []
        added.extend(_append_auto_token_map(to_auto_map))
        added.extend(_append_token_price_mapping(to_cg_map))
        added.extend(_append_address_map_entries(to_addr_map))
        all_added.extend(added)

        # 6) log row
        improvement_pct = 0.0
        if missing_prev is not None and missing_prev > 0:
            improvement_pct = float(missing_prev - missing_count) / float(missing_prev)
        if missing_prev is not None and improvement_pct < 0.01:
            no_improve_streak += 1
        else:
            no_improve_streak = 0

        row = {
            "iteration": i,
            "timestamp_utc": _now_utc(),
            "batch_runner_exit": ec_a,
            "unknown_report_exit": ec_b,
            "missing_price_count": missing_count,
            "coverage": round(coverage, 6),
            "price_confidence_distribution": stats.price_conf_dist,
            "improvements_applied": added,
            "improvement_pct": round(improvement_pct, 6),
        }
        log_rows.append(row)
        _save_json(_log_path(), log_rows)

        print(f"Iteration {i}: coverage={coverage*100:.2f}%", flush=True)

        # 8) stop conditions
        if missing_count <= 10:
            stop_reason = "missing_price_tokens <= 10"
            break
        if coverage >= 0.90:
            stop_reason = "price_coverage >= 90%"
            break
        if no_improve_streak >= 3:
            stop_reason = "improvement < 1% for 3 iterations"
            break

        missing_prev = missing_count
    else:
        stop_reason = "max_iterations reached"

    final_stats = _collect_coverage_stats(YEAR)
    end_cov = _coverage(final_stats)
    unresolved = sorted(final_stats.missing_tokens.items(), key=lambda kv: (-kv[1], kv[0]))[:25]

    lines: List[str] = []
    lines.append("# Value Coverage Final Report")
    lines.append("")
    lines.append(f"- Start coverage: `{(start_cov or 0.0)*100:.2f}%`")
    lines.append(f"- End coverage: `{end_cov*100:.2f}%`")
    lines.append(f"- Start missing price rows: `{log_rows[0]['missing_price_count'] if log_rows else 0}`")
    lines.append(f"- End missing price rows: `{final_stats.missing_rows}`")
    lines.append(f"- Total deterministic improvements applied: `{len(all_added)}`")
    lines.append(f"- Stop reason: `{stop_reason}`")
    lines.append("")
    lines.append("## Tokens Improved")
    lines.append("")
    if all_added:
        lines.append("| Target File | Mapping |")
        lines.append("|-------------|---------|")
        for item in all_added:
            if item.get("target") == "token_price_mapping.json":
                lines.append(f"| token_price_mapping.json | {item['symbol']} -> {item['coingecko_id']} |")
            elif item.get("target") == "auto_token_map.json":
                lines.append(f"| auto_token_map.json | {item['source']} -> {item['maps_to']} |")
            elif item.get("target") == "address_map.json":
                lines.append(f"| address_map.json | {item['contract']} -> {item['maps_to']} |")
    else:
        lines.append("- No new deterministic mappings were eligible.")
    lines.append("")
    lines.append("## Tokens Still Unresolved")
    lines.append("")
    lines.append("| Token | Count | Reason |")
    lines.append("|-------|-------|--------|")
    for tok, cnt in unresolved:
        reason = _reason_unresolved(tok, int(cnt), final_stats.token_contracts.get(tok, {}))
        lines.append(f"| {tok} | {cnt} | {reason} |")
    if not unresolved:
        lines.append("| *(none)* | 0 | all priced/resolved by deterministic rules |")
    lines.append("")
    lines.append("## Safety")
    lines.append("")
    lines.append("- No fake prices assigned.")
    lines.append("- No guessing-based symbol resolution.")
    lines.append("- Existing mappings were not overwritten.")
    lines.append("- Tax logic was not modified.")
    lines.append("")

    _report_path().write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("STOP CONDITION REACHED", flush=True)


if __name__ == "__main__":
    run_loop()

