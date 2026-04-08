"""
Robust ETH-focused unknown discovery loop with pacing and clear reporting.

What it does per iteration:
1) Run batch_runner (harvest + pipeline + unknown registry update)
2) Run unknown_classifier_report
3) Snapshot unknown registry
4) Auto-apply conservative token mappings into data/config/auto_token_map.json
5) Write easy-to-read KPI files

Usage example:
  python -m taxtrack.tools.unknown_loop_v2 --wallet-list wallet_list_loop_runtime.txt --year 2025 --iterations 12
"""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _taxtrack_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _registry_path() -> Path:
    return _taxtrack_root() / "data" / "registry" / "unknown_registry.json"


def _auto_token_map_path() -> Path:
    return _taxtrack_root() / "data" / "config" / "auto_token_map.json"


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _top_n(d: dict, n: int = 10) -> List[Tuple[str, int]]:
    items = [(str(k), int(v)) for k, v in (d or {}).items()]
    items.sort(key=lambda kv: (-kv[1], kv[0]))
    return items[:n]


def _snapshot_kpi(reg: dict) -> dict:
    return {
        "methods_total": sum(int(v) for v in (reg.get("methods") or {}).values()),
        "tokens_total": sum(int(v) for v in (reg.get("tokens") or {}).values()),
        "contracts_total": sum(int(v) for v in (reg.get("contracts") or {}).values()),
        "missing_price_total": sum(int(v) for v in (reg.get("missing_price_tokens") or {}).values()),
        "unlabeled_contracts_total": sum(int(v) for v in (reg.get("unlabeled_contracts") or {}).values()),
        "top_tokens": _top_n(reg.get("tokens") or {}, 12),
        "top_contracts": _top_n(reg.get("contracts") or {}, 12),
        "top_missing_price": _top_n(reg.get("missing_price_tokens") or {}, 12),
    }


def _write_quick_unknown_md(path: Path, reg: dict, *, year: int, chains: str, iteration: int) -> None:
    lines: List[str] = []
    lines.append("# Unknown Quick Report")
    lines.append("")
    lines.append(f"- Iteration: `{iteration:02d}`")
    lines.append(f"- Year: `{year}`")
    lines.append(f"- Chains: `{chains}`")
    lines.append("")
    lines.append("## Top Unknown Tokens")
    lines.append("")
    lines.append("| Token | Count |")
    lines.append("|-------|-------|")
    for k, v in _top_n(reg.get("tokens") or {}, 25):
        lines.append(f"| {k} | {v} |")
    lines.append("")
    lines.append("## Top Unknown Contracts")
    lines.append("")
    lines.append("| Contract | Count |")
    lines.append("|----------|-------|")
    for k, v in _top_n(reg.get("contracts") or {}, 25):
        lines.append(f"| {k} | {v} |")
    lines.append("")
    lines.append("## Top Missing Price Tokens")
    lines.append("")
    lines.append("| Token | Count |")
    lines.append("|-------|-------|")
    for k, v in _top_n(reg.get("missing_price_tokens") or {}, 25):
        lines.append(f"| {k} | {v} |")
    lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _suggest_map(symbol: str) -> Tuple[str, str] | None:
    s = (symbol or "").strip().upper()
    if not s:
        return None

    stable_roots = {
        "USDC", "USDT", "DAI", "TUSD", "USDE", "USDP", "GUSD",
        "PYUSD", "LUSD", "BUSD", "USDCE", "USDBC", "AXLUSDC",
    }
    wrapped_roots = {"ETH", "BTC", "BNB", "MATIC", "AVAX", "FTM", "ARB", "OP"}

    if s.endswith(".E.E"):
        root = s[:-4]
        if root in stable_roots:
            return ("USD", "stable suffix .E.E")
    if s.endswith(".E"):
        root = s[:-2]
        if root in stable_roots:
            return ("USD", "stable suffix .E")
        if root in {"ETH", "WETH"}:
            return ("ETH", "eth wrapper suffix .E")

    if s in {"WETHE", "WETH.E", "ETH.E"}:
        return ("ETH", "known ETH bridged wrapper")

    if s.startswith("W") and len(s) > 1 and s[1:] in wrapped_roots:
        return (s[1:], "wrapped root heuristic")

    return None


def _apply_auto_token_mappings(reg: dict, *, max_new: int) -> List[dict]:
    ap = _auto_token_map_path()
    data = _load_json(ap)
    token_map = data.get("token_map") if isinstance(data, dict) else {}
    if not isinstance(token_map, dict):
        token_map = {}

    existing = {str(k).upper() for k in token_map.keys()}
    added: List[dict] = []
    candidates = _top_n(reg.get("missing_price_tokens") or {}, 200)

    for sym, count in candidates:
        if len(added) >= max_new:
            break
        u = sym.upper()
        if u in existing:
            continue
        sug = _suggest_map(u)
        if not sug:
            continue
        mapped, reason = sug
        token_map[u] = mapped
        existing.add(u)
        added.append({"symbol": u, "maps_to": mapped, "count": int(count), "reason": reason})

    if added:
        data = data if isinstance(data, dict) else {}
        data.setdefault("meta", {})
        data["meta"]["updated_at_utc"] = _now_utc()
        data["meta"]["updated_by"] = "unknown_loop_v2"
        data["token_map"] = dict(sorted(token_map.items(), key=lambda kv: kv[0]))
        _save_json(ap, data)

    return added


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


def main() -> None:
    ap = argparse.ArgumentParser(description="Robust unknown loop v2 (ETH-focused by default).")
    ap.add_argument("--wallet-list", default="wallet_list_loop_runtime.txt")
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--iterations", type=int, default=12)
    ap.add_argument("--chains", default="eth")
    ap.add_argument("--top", type=int, default=200)
    ap.add_argument("--sleep-seconds", type=int, default=12)
    ap.add_argument("--max-new-maps-per-iter", type=int, default=5)
    ap.add_argument("--run-unknown-classifier", action="store_true", help="Also run heavy unknown_classifier_report step.")
    ap.add_argument("--out-root", default="")
    args = ap.parse_args()

    repo = _repo_root()
    wallet_list = Path(args.wallet_list)
    if not wallet_list.is_absolute():
        wallet_list = repo / wallet_list
    if not wallet_list.exists():
        raise FileNotFoundError(f"Wallet list not found: {wallet_list}")

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_root = Path(args.out_root) if args.out_root else (repo / "taxtrack" / "data" / "loop" / f"unknown_v2_{stamp}")
    out_root.mkdir(parents=True, exist_ok=True)
    run_log = out_root / "run.log"

    run_log.write_text(f"START {_now_utc()}\n", encoding="utf-8")
    iter_rows: List[dict] = []

    for i in range(1, int(args.iterations) + 1):
        iter_tag = f"iter_{i:02d}"
        iter_dir = out_root / iter_tag
        iter_dir.mkdir(parents=True, exist_ok=True)
        print(f"[{iter_tag}] start", flush=True)
        with run_log.open("a", encoding="utf-8") as f:
            f.write(f"[{iter_tag}] start {_now_utc()}\n")

        reg_before = _load_json(_registry_path())
        kpi_before = _snapshot_kpi(reg_before)

        cmd1 = [
            "python", "-m", "taxtrack.tools.batch_runner",
            "--wallet-list", str(wallet_list),
            "--year", str(args.year),
            "--chains", str(args.chains),
            "--top", str(args.top),
            "--debug-ambiguous-samples",
        ]
        ec1, out1 = _run_cmd(cmd1, repo)
        (iter_dir / "batch_runner.log").write_text(out1, encoding="utf-8")

        reg_after = _load_json(_registry_path())
        kpi_after = _snapshot_kpi(reg_after)
        _save_json(iter_dir / "unknown_registry_snapshot.json", reg_after)
        _write_quick_unknown_md(
            iter_dir / "UNKNOWN_CLASSIFICATION_REPORT.md",
            reg_after,
            year=int(args.year),
            chains=str(args.chains),
            iteration=i,
        )

        ec2 = 0
        if args.run_unknown_classifier:
            cmd2 = [
                "python", "-m", "taxtrack.tools.unknown_classifier_report",
                "--year", str(args.year),
                "--out", str(iter_dir / "UNKNOWN_CLASSIFICATION_REPORT_heavy.md"),
            ]
            ec2, out2 = _run_cmd(cmd2, repo)
            (iter_dir / "unknown_classifier.log").write_text(out2, encoding="utf-8")

        new_maps = _apply_auto_token_mappings(reg_after, max_new=int(args.max_new_maps_per_iter))

        summary = {
            "iteration": i,
            "started_utc": _now_utc(),
            "batch_runner_exit": ec1,
            "unknown_report_exit": ec2,
            "kpi_before": kpi_before,
            "kpi_after": kpi_after,
            "delta": {
                "methods_total": kpi_after["methods_total"] - kpi_before["methods_total"],
                "tokens_total": kpi_after["tokens_total"] - kpi_before["tokens_total"],
                "contracts_total": kpi_after["contracts_total"] - kpi_before["contracts_total"],
                "missing_price_total": kpi_after["missing_price_total"] - kpi_before["missing_price_total"],
            },
            "new_auto_token_maps": new_maps,
        }
        _save_json(iter_dir / "summary.json", summary)
        iter_rows.append(summary)

        with run_log.open("a", encoding="utf-8") as f:
            f.write(
                f"[{iter_tag}] batch_exit={ec1} unknown_exit={ec2} "
                f"new_maps={len(new_maps)} "
                f"delta_tokens={summary['delta']['tokens_total']} "
                f"delta_missing_price={summary['delta']['missing_price_total']}\n"
            )

        time.sleep(max(0, int(args.sleep_seconds)))

    with run_log.open("a", encoding="utf-8") as f:
        f.write(f"END {_now_utc()}\n")

    # Build easy summary report
    lines: List[str] = []
    lines.append("# Unknown Loop v2 Report")
    lines.append("")
    lines.append(f"- Year: `{args.year}`")
    lines.append(f"- Chains: `{args.chains}`")
    lines.append(f"- Wallet list: `{wallet_list}`")
    lines.append(f"- Iterations: `{args.iterations}`")
    lines.append(f"- Output folder: `{out_root}`")
    lines.append("")
    lines.append("## Iteration KPI")
    lines.append("")
    lines.append("| Iter | Batch | UnknownReport | +Tokens | +Contracts | +MissingPrice | New Auto Maps |")
    lines.append("|------|-------|--------------|---------|------------|---------------|---------------|")
    for row in iter_rows:
        d = row["delta"]
        lines.append(
            f"| {row['iteration']:02d} | {row['batch_runner_exit']} | {row['unknown_report_exit']} | "
            f"{d['tokens_total']} | {d['contracts_total']} | {d['missing_price_total']} | "
            f"{len(row['new_auto_token_maps'])} |"
        )
    lines.append("")
    lines.append("## What Was Integrated")
    lines.append("")
    lines.append("- New conservative token aliases were auto-added to `taxtrack/data/config/auto_token_map.json` when detected.")
    lines.append("- `token_mapper` now reads this file automatically, so learned aliases are active in the software immediately.")
    lines.append("- Per iteration details are in each `iter_XX/summary.json` and `iter_XX/UNKNOWN_CLASSIFICATION_REPORT.md`.")
    lines.append("")

    (out_root / "final_unknown_report.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    # German, human-friendly report variant
    de: List[str] = []
    de.append("# Abschlussreport Unknown-Loop (DE)")
    de.append("")
    de.append("## Laufkontext")
    de.append("")
    de.append(f"- Steuerjahr: `{args.year}`")
    de.append(f"- Chains: `{args.chains}`")
    de.append(f"- Wallet-Liste: `{wallet_list}`")
    de.append(f"- Iterationen: `{args.iterations}`")
    de.append(f"- Ergebnisordner: `{out_root}`")
    de.append("")
    de.append("## KPI pro Iteration")
    de.append("")
    de.append("| Iteration | Batch OK | Unknown-Report OK | Neue Tokens | Neue Contracts | Missing-Price Delta | Neu eingepflegt |")
    de.append("|-----------|----------|-------------------|-------------|----------------|---------------------|-----------------|")
    total_new_maps = 0
    for row in iter_rows:
        d = row["delta"]
        maps_count = len(row["new_auto_token_maps"])
        total_new_maps += maps_count
        de.append(
            f"| {row['iteration']:02d} | {row['batch_runner_exit']==0} | {row['unknown_report_exit']==0} | "
            f"{d['tokens_total']} | {d['contracts_total']} | {d['missing_price_total']} | {maps_count} |"
        )
    de.append("")
    de.append("## Was automatisch in die Software eingepflegt wurde")
    de.append("")
    if total_new_maps == 0:
        de.append("- Keine neuen sicheren Token-Mappings erkannt, die automatisch uebernommen werden konnten.")
    else:
        de.append(
            f"- Insgesamt wurden `{total_new_maps}` neue konservative Token-Mappings in "
            "`taxtrack/data/config/auto_token_map.json` eingetragen."
        )
        de.append("- Diese Mappings werden direkt vom `token_mapper` geladen und sind damit im System aktiv.")
    de.append("")
    de.append("## Wo du Details findest")
    de.append("")
    de.append("- Pro Iteration: `iter_XX/summary.json`")
    de.append("- Pro Iteration Unknown-Analyse: `iter_XX/UNKNOWN_CLASSIFICATION_REPORT.md`")
    de.append("- Registry-Snapshot: `iter_XX/unknown_registry_snapshot.json`")
    de.append("")
    de.append("## Naechste sinnvolle Schritte")
    de.append("")
    de.append("- Top-Contracts ohne Label aus den Reports pruefen und in `address_map.json` aufnehmen.")
    de.append("- Token mit hohen Missing-Price-Zaehlern priorisieren.")
    de.append("- Danach denselben Loop erneut laufen lassen und Delta vergleichen.")
    de.append("")
    (out_root / "final_unknown_report_de.md").write_text("\n".join(de) + "\n", encoding="utf-8")
    print(f"[DONE] {out_root}", flush=True)


if __name__ == "__main__":
    main()

