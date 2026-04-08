"""
Automated, deterministic self-healing loop for UNKNOWN token resolution.

Scope is strictly limited to:
- token mapping
- contract mapping
- safe normalization

No changes to tax logic are made.
"""

from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


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


def _registry_path() -> Path:
    return _taxtrack_root() / "data" / "registry" / "unknown_registry.json"


def _address_map_path() -> Path:
    return _taxtrack_root() / "data" / "config" / "address_map.json"


def _auto_token_map_path() -> Path:
    return _taxtrack_root() / "data" / "config" / "auto_token_map.json"


def _self_log_path() -> Path:
    return _loop_dir() / "self_healing_log.json"


def _final_report_path() -> Path:
    return _loop_dir() / "self_healing_final_report.md"


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
    proc = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        encoding="utf-8",
        errors="replace",
        capture_output=True,
    )
    out = (proc.stdout or "") + ("\n" if proc.stdout and proc.stderr else "") + (proc.stderr or "")
    return proc.returncode, out


def _parse_unknown_count_from_report(report_path: Path) -> int:
    if not report_path.exists():
        return 0
    text = report_path.read_text(encoding="utf-8", errors="replace")
    m = re.search(r"Rows classified as `unknown`:\s*(\d+)", text)
    if m:
        return int(m.group(1))
    return 0


@dataclass
class UnknownTokenStats:
    count: int
    contracts: set


def _collect_unknown_token_stats(year: int) -> Dict[str, UnknownTokenStats]:
    harvest_root = _taxtrack_root() / "data" / "harvest"
    stats: Dict[str, UnknownTokenStats] = {}
    if not harvest_root.exists():
        return stats

    for wallet_dir in harvest_root.iterdir():
        if not wallet_dir.is_dir():
            continue
        classified = wallet_dir / str(year) / "classified.json"
        if not classified.exists():
            continue
        rows = _load_json(classified, [])
        if not isinstance(rows, list):
            continue

        for r in rows:
            if not isinstance(r, dict):
                continue
            cat = (r.get("category") or "").strip().lower()
            if cat != "unknown":
                continue
            token = (r.get("token") or "").strip().upper()
            if not token:
                continue
            meta = r.get("meta") if isinstance(r.get("meta"), dict) else {}
            contract = (meta.get("cp_addr") or r.get("to_addr") or "").strip().lower()
            if token not in stats:
                stats[token] = UnknownTokenStats(count=0, contracts=set())
            stats[token].count += 1
            if contract.startswith("0x") and len(contract) >= 10:
                stats[token].contracts.add(contract)
    return stats


def _is_lp_or_pool_token(token: str) -> bool:
    t = token.upper()
    return ("_LPT" in t) or ("LP" in t) or ("POOL" in t) or ("VAULT" in t)


def _is_protocol_token(token: str) -> bool:
    blocked = {
        "MOO", "GMX", "AAVE", "COMP", "CRV", "UNI", "SUSHI", "BAL", "PENDLE", "MORPHO"
    }
    t = token.upper()
    if t in blocked:
        return True
    # conservative prefix guard for moo* vault wrappers
    if t.startswith("MOO"):
        return True
    return False


def _safe_symbol_to_canonical(symbol: str) -> str | None:
    s = symbol.upper()
    # deterministic wrappers only
    if s in {"WETH", "WETHE", "WETH.E", "ETH.E"}:
        return "ETH"
    if s in {"WAVAX", "WAVAX.E"}:
        return "AVAX"
    if s in {"WBTC", "WBTC.E"}:
        return "BTC"
    return None


def _extract_contract_maps_to_eth() -> Dict[str, str]:
    out: Dict[str, str] = {}
    address_map = _load_json(_address_map_path(), {})
    eth_map = address_map.get("eth") if isinstance(address_map, dict) else {}
    if not isinstance(eth_map, dict):
        return out
    for addr, meta in eth_map.items():
        if isinstance(meta, dict):
            maps_to = (meta.get("maps_to") or "").strip().upper()
            if maps_to:
                out[str(addr).lower()] = maps_to
    return out


def _append_auto_token_maps(new_maps: Dict[str, str]) -> List[dict]:
    if not new_maps:
        return []
    p = _auto_token_map_path()
    data = _load_json(p, {"meta": {}, "token_map": {}})
    if not isinstance(data, dict):
        data = {"meta": {}, "token_map": {}}
    tm = data.get("token_map")
    if not isinstance(tm, dict):
        tm = {}
    added: List[dict] = []
    for src, dst in sorted(new_maps.items()):
        u = src.upper()
        if u in tm:
            continue  # never overwrite
        tm[u] = dst.upper()
        added.append({"source": u, "maps_to": dst.upper(), "target": "auto_token_map.json", "confidence": 1.0})
    data["token_map"] = dict(sorted(tm.items(), key=lambda kv: kv[0]))
    data["meta"] = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    data["meta"]["updated_at_utc"] = _now_utc()
    data["meta"]["updated_by"] = "unknown_self_healing_loop"
    _save_json(p, data)
    return added


def _append_address_map_entries(new_contract_maps: Dict[str, str]) -> List[dict]:
    if not new_contract_maps:
        return []
    p = _address_map_path()
    data = _load_json(p, {})
    if not isinstance(data, dict):
        data = {}
    eth_map = data.get("eth")
    if not isinstance(eth_map, dict):
        eth_map = {}
        data["eth"] = eth_map

    added: List[dict] = []
    for addr, maps_to in sorted(new_contract_maps.items()):
        a = addr.lower()
        if a in eth_map:
            continue  # never overwrite manual/existing mappings
        eth_map[a] = {
            "label": f"auto_resolved_{maps_to.lower()}",
            "protocol": "auto_resolved",
            "type": "token",
            "maps_to": maps_to.upper(),
            "confidence": 1.0,
            "source": "unknown_self_healing_loop",
        }
        added.append({"contract": a, "maps_to": maps_to.upper(), "target": "address_map.json", "confidence": 1.0})
    _save_json(p, data)
    return added


def _top_remaining(stats: Dict[str, UnknownTokenStats], limit: int = 20) -> List[Tuple[str, int]]:
    rows = [(k, v.count) for k, v in stats.items()]
    rows.sort(key=lambda kv: (-kv[1], kv[0]))
    return rows[:limit]


def _reason_unresolvable(token: str, st: UnknownTokenStats, contract_maps: Dict[str, str]) -> str:
    if st.count < 5:
        return "frequency < 5"
    if len(st.contracts) != 1:
        return "maps to multiple/no contracts"
    if token == "UNKNOWN":
        return "explicit UNKNOWN token"
    if _is_lp_or_pool_token(token):
        return "LP/POOL/VAULT pattern blocked"
    if "-" in token:
        return "multi-token symbol contains '-'"
    if _is_protocol_token(token):
        return "protocol token blocked by safety rules"
    only_contract = next(iter(st.contracts)) if st.contracts else ""
    if only_contract not in contract_maps and _safe_symbol_to_canonical(token) is None:
        return "no deterministic mapping source"
    return "safety rule prevented mapping"


def run_loop() -> None:
    repo = _repo_root()
    loop_log: List[dict] = []
    no_improvement_streak = 0
    unknown_prev: int | None = None
    initial_unknown = 0
    total_mappings_added = 0

    print("SELF-HEALING LOOP STARTED", flush=True)

    for i in range(1, MAX_ITERATIONS + 1):
        # Step A: run pipeline batch
        cmd_a = [
            "python", "-m", "taxtrack.tools.batch_runner",
            "--wallet-list", WALLET_LIST,
            "--year", str(YEAR),
            "--chains", CHAINS,
            "--top", "200",
        ]
        ec_a, out_a = _run_cmd(cmd_a, repo)
        (_loop_dir() / f"self_healing_iter_{i:02d}_batch_runner.log").write_text(out_a, encoding="utf-8")

        # Step B: unknown report
        report_path = _loop_dir() / f"self_healing_iter_{i:02d}.md"
        cmd_b = [
            "python", "-m", "taxtrack.tools.unknown_classifier_report",
            "--year", str(YEAR),
            "--out", str(report_path),
        ]
        ec_b, out_b = _run_cmd(cmd_b, repo)
        (_loop_dir() / f"self_healing_iter_{i:02d}_unknown_classifier.log").write_text(out_b, encoding="utf-8")

        # Step C: parse results
        unknown_count = _parse_unknown_count_from_report(report_path)
        if i == 1:
            initial_unknown = unknown_count
        reg = _load_json(_registry_path(), {})
        missing_price_tokens = reg.get("missing_price_tokens") if isinstance(reg, dict) else {}
        if not isinstance(missing_price_tokens, dict):
            missing_price_tokens = {}
        confidence_levels = {"deterministic": 1.0}

        stats = _collect_unknown_token_stats(YEAR)
        contract_maps = _extract_contract_maps_to_eth()

        # Step D/E: apply safe improvements (deterministic only)
        new_symbol_maps: Dict[str, str] = {}
        new_contract_maps: Dict[str, str] = {}

        for tok, st in stats.items():
            if st.count < 5:
                continue
            if len(st.contracts) != 1:
                continue
            if tok == "UNKNOWN":
                continue
            if _is_lp_or_pool_token(tok):
                continue
            if "-" in tok:
                continue
            if _is_protocol_token(tok):
                continue

            only_contract = next(iter(st.contracts))
            mapped = contract_maps.get(only_contract)
            if not mapped:
                mapped = _safe_symbol_to_canonical(tok)
            if not mapped:
                continue
            new_symbol_maps[tok] = mapped
            if only_contract and only_contract not in contract_maps:
                new_contract_maps[only_contract] = mapped

        added_token_maps = _append_auto_token_maps(new_symbol_maps)
        added_contract_maps = _append_address_map_entries(new_contract_maps)
        newly_added = added_token_maps + added_contract_maps
        total_mappings_added += len(newly_added)

        # Improvement / stop handling
        if unknown_prev is None:
            improvement = 0
        else:
            improvement = unknown_prev - unknown_count

        if unknown_prev is not None and improvement <= 0:
            no_improvement_streak += 1
        else:
            no_improvement_streak = 0

        row = {
            "iteration": i,
            "timestamp_utc": _now_utc(),
            "batch_runner_exit": ec_a,
            "unknown_report_exit": ec_b,
            "unknown_count": unknown_count,
            "unknown_count_previous": unknown_prev,
            "improvement": improvement,
            "missing_price_tokens_top": sorted(
                ((k, int(v)) for k, v in missing_price_tokens.items()),
                key=lambda kv: (-kv[1], kv[0]),
            )[:20],
            "confidence_levels": confidence_levels,
            "new_mappings": newly_added,
        }
        loop_log.append(row)
        _save_json(_self_log_path(), loop_log)

        print(f"Iteration {i} complete: unknown={unknown_count}", flush=True)

        # stop condition A
        if unknown_count <= 10:
            stop_reason = "unknown_count <= 10"
            break
        # stop condition B (strict: improvement < 2)
        if unknown_prev is not None and improvement < 2:
            stop_reason = "improvement < 2"
            break
        # stop condition C (no improvement for 3 iterations)
        if no_improvement_streak >= 3:
            stop_reason = "no improvement for 3 iterations"
            break

        unknown_prev = unknown_count
    else:
        stop_reason = "max_iterations reached"

    # final report
    final_stats = _collect_unknown_token_stats(YEAR)
    top_remaining = _top_remaining(final_stats, 20)
    lines: List[str] = []
    lines.append("# Self-Healing Final Report")
    lines.append("")
    lines.append(f"- Initial unknown count: `{initial_unknown}`")
    lines.append(f"- Final unknown count: `{loop_log[-1]['unknown_count'] if loop_log else 0}`")
    lines.append(f"- Total mappings added: `{total_mappings_added}`")
    lines.append(f"- Stop reason: `{stop_reason}`")
    lines.append("")
    lines.append("## Top Remaining Unknown Tokens")
    lines.append("")
    lines.append("| Token | Count | Reason not auto-resolved |")
    lines.append("|-------|-------|--------------------------|")
    contract_maps = _extract_contract_maps_to_eth()
    for token, cnt in top_remaining:
        st = final_stats[token]
        reason = _reason_unresolvable(token, st, contract_maps)
        lines.append(f"| {token} | {cnt} | {reason} |")
    if not top_remaining:
        lines.append("| *(none)* | 0 | fully resolved by deterministic rules |")
    lines.append("")
    lines.append("## Safety Notes")
    lines.append("")
    lines.append("- No tax logic was modified.")
    lines.append("- Existing mappings were never overwritten.")
    lines.append("- Only deterministic mappings were applied.")
    lines.append("")
    _final_report_path().write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("STOP CONDITION REACHED", flush=True)


if __name__ == "__main__":
    run_loop()

