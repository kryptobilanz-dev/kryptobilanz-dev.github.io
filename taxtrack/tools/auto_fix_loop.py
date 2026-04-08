"""
Auto-fix loop: integrity_check -> one prioritized fix prompt for Cursor.
Never modifies code. Human applies prompt and re-runs.

  python -m taxtrack.tools.auto_fix_loop --wallet 0x... --year 2025
  python -m taxtrack.tools.auto_fix_loop --wallet 0x... --year 2025 --iterations 5
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from taxtrack.tools.integrity_check import run_checks


def _loop_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "loop" / "autofix"


def _classify(line: str, *, from_critical: bool) -> Optional[str]:
    s = line.strip()
    if "[DATA LOSS]" in s and "unified swap" in s:
        return "DATA_LOSS_SWAP"
    if "[DATA LOSS]" in s:
        return "DATA_LOSS_DISPOSAL"
    if "[DOUBLE]" in s:
        return "DOUBLE"
    if "[FEE]" in s:
        return "FEE"
    if "[TAX]" in s:
        if from_critical or ("diff" in s.lower() and "tax_ready" in s.lower()):
            return "TAX_MISMATCH"
        return "TAX_REPORTING"
    if "[PRICE]" in s:
        return "PRICE"
    return None


# Lower rank = higher priority
PRIORITY = {
    "DATA_LOSS_SWAP": 1,
    "DATA_LOSS_DISPOSAL": 2,
    "DOUBLE": 3,
    "FEE": 4,
    "TAX_MISMATCH": 5,
    "TAX_REPORTING": 6,
    "PRICE": 7,
}

PROMPT_DATA_LOSS = """--- FOCUS ---
Fix missing disposals: classified swap txs must produce economic rows in gains / pipeline output.

--- CONTEXT ---
integrity_check reports unified swap txs in classified.json that do not appear in gains.json.

--- TASK ---
1. Trace why compute_gains / group_gains_economic skips these txs (missing price, swap grouping, category).
2. Fix ONLY the minimal layer (e.g. price resolution for skipped outflows, or classification meta) so swaps with valid EUR produce GainRows.
3. Do NOT change FIFO ordering rules unless required for the skip.

--- CONSTRAINTS ---
- No auto-run; user re-runs pipeline + integrity_check after change.
- Do not weaken skip-missing-price safety without explicit user intent.

--- SUCCESS ---
integrity_check: zero "[DATA LOSS] ... unified swap" critical lines after harvest refresh.
"""

PROMPT_DOUBLE = """--- FOCUS ---
Fix Vault Double Counting (same tx: swap economic + position_exit).

--- CONTEXT ---
Two economic realizations for one vault exit tx_hash.

--- TASK ---
Ensure only one economic event per vault exit (e.g. exclude vault pattern from swap grouping OR drop swap when position_exit exists). See existing pipeline/evaluate patterns.

--- CONSTRAINTS ---
Do not change FIFO lot logic. Reporting-only dedupe is acceptable if already agreed.

--- SUCCESS ---
integrity_check: no [DOUBLE] issues; one PVG row per tx.
"""

PROMPT_FEE = """--- FOCUS ---
Fee consistency: exactly one tx fee application per tx_hash on economic rows.

--- CONTEXT ---
integrity_check flagged duplicate or missing fees vs classified sum.

--- TASK ---
1. In pipeline, ensure _apply_fees_net_pnl applies each tx fee once total (not per duplicate row).
2. If multiple economic rows share a tx, allocate fee once or merge rows — document choice.

--- CONSTRAINTS ---
Do not zero-out legitimate gas; align with classified fee_eur sum per tx.

--- SUCCESS ---
integrity_check: no [FEE] warnings for affected txs.
"""

PROMPT_TAX = """--- FOCUS ---
Tax / reporting alignment: tax_ready totals vs tax_summary.

--- CONTEXT ---
integrity_check: sum(tax_ready gain) vs tax_summary.total_gains_net_eur mismatch > 1 EUR.

--- TASK ---
1. Fix build_tax_ready_economic_gains_de or tax_summary aggregation so totals match row sums.
2. Ensure PDF/CSV use same tax_ready source (already wired) — verify no double exclusion.

--- CONSTRAINTS ---
Do not change FIFO holding math; fix interpreter summation or rounding only.

--- SUCCESS ---
integrity_check: no critical [TAX] mismatch; optional write tax_summary.json to harvest for tooling.
"""

PROMPT_TAX_REPORTING = """--- FOCUS ---
Persist tax_ready + tax_summary for integrity tooling.

--- TASK ---
After pipeline run, write economic_gains_tax_ready.json and tax_summary.json under harvest/<wallet>/<year>/.

--- CONSTRAINTS ---
Read-only consumers (integrity_check) expect these files for CHECK 5.

--- SUCCESS ---
integrity_check runs deep tax check without "[TAX] ... not present" warning.
"""

PROMPT_PRICE = """--- FOCUS ---
Price visibility (NOT inventing prices).

--- CONTEXT ---
Many txs flagged: valuation_missing or eur_value<=0 on out/swap legs.

--- TASK ---
1. Log or export a clear report: tx_hash, token, reason (missing price).
2. Optional: surface in PDF appendix "Unpriced disposals" — do NOT assign fake EUR.

--- CONSTRAINTS ---
Do not set eur_value=0.01 or default prices. Visibility only unless user approves pricing rules.

--- SUCCESS ---
User can reconcile list; integrity_check PRICE count may stay until prices fixed upstream.
"""


def _pick_top_issue(
    critical: List[str], warnings: List[str]
) -> Tuple[str, str, str]:
    """
    Returns (issue_type, priority_name, raw_line).
    """
    candidates: List[Tuple[int, str, str]] = []
    for line in critical:
        k = _classify(line, from_critical=True)
        if k and k in PRIORITY:
            candidates.append((PRIORITY[k], k, line))
    for line in warnings:
        k = _classify(line, from_critical=False)
        if k and k in PRIORITY:
            candidates.append((PRIORITY[k], k, line))
    if not candidates:
        return ("NONE", "NONE", "")
    candidates.sort(key=lambda x: x[0])
    rank, key, line = candidates[0]
    return (key, line, line)


def _prompt_for_issue(issue_key: str) -> Tuple[str, str]:
    """Returns (recommended_fix_title, full_prompt)."""
    if issue_key == "DATA_LOSS_SWAP" or issue_key == "DATA_LOSS_DISPOSAL":
        return ("Fix Missing Disposal Prompt", PROMPT_DATA_LOSS)
    if issue_key == "DOUBLE":
        return ("Fix Vault Double Counting Prompt", PROMPT_DOUBLE)
    if issue_key == "FEE":
        return ("Fix Fee Once Per Tx Prompt", PROMPT_FEE)
    if issue_key == "TAX_MISMATCH":
        return ("Align tax_ready vs tax_summary Prompt", PROMPT_TAX)
    if issue_key == "TAX_REPORTING":
        return ("Persist tax_ready JSON Prompt", PROMPT_TAX_REPORTING)
    if issue_key == "PRICE":
        return ("Price Visibility Prompt", PROMPT_PRICE)
    return ("No automated prompt", "Status SAFE or no classified issue. Re-run harvest/pipeline if needed.")


def _format_report(wallet: str, year: int, crit, warn, ok, verdict: str) -> str:
    lines = [
        "SYSTEM INTEGRITY REPORT",
        f"Wallet: {wallet.lower()}  Year: {year}",
        "",
        "--- Critical Issues ---",
    ]
    if crit:
        lines.extend(f"  {x}" for x in crit)
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("--- Warnings ---")
    if warn:
        lines.extend(f"  {x}" for x in warn)
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append("--- Passed Checks ---")
    if ok:
        lines.extend(f"  {x}" for x in ok)
    else:
        lines.append("  (none)")
    lines.append("")
    lines.append(f"FINAL: {verdict}")
    return "\n".join(lines)


def run_once(wallet: str, year: int, out_dir: Path) -> Tuple[str, str, str, str]:
    crit, warn, ok, verdict = run_checks(wallet, year)
    issue_key, _label, raw = _pick_top_issue(crit, warn)
    title, prompt = _prompt_for_issue(issue_key)

    out_dir.mkdir(parents=True, exist_ok=True)
    report = _format_report(wallet, year, crit, warn, ok, verdict)
    (out_dir / "integrity_report.txt").write_text(report, encoding="utf-8")
    (out_dir / "next_fix_prompt.txt").write_text(prompt, encoding="utf-8")

    n_crit, n_warn = len(crit), len(warn)
    top_human = raw[:120] + ("..." if len(raw) > 120 else "") if raw else verdict
    ctx = "\n".join(
        [
            f"wallet: {wallet.lower()}",
            f"year: {year}",
            f"verdict: {verdict}",
            f"critical_count: {n_crit}",
            f"warnings_count: {n_warn}",
            f"top_issue_type: {issue_key}",
            f"top_issue_summary: {top_human}",
            f"recommended_fix: {title}",
            "",
            "Next step: Apply next_fix_prompt.txt in Cursor (one change), re-run pipeline/harvest, then:",
            f"  python -m taxtrack.tools.integrity_check --wallet {wallet} --year {year}",
        ]
    )
    (out_dir / "context_summary.txt").write_text(ctx, encoding="utf-8")

    return verdict, issue_key, title, raw


def main() -> None:
    ap = argparse.ArgumentParser(description="Auto-fix loop: integrity -> one Cursor prompt")
    ap.add_argument("--wallet", required=True)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--iterations", type=int, default=1, help="Max loop rounds (waits for Enter between)")
    args = ap.parse_args()

    out_dir = _loop_dir()
    iterations = max(1, int(args.iterations))

    print("AUTO FIX REPORT")
    print("=" * 48)

    for i in range(1, iterations + 1):
        verdict, issue_key, title, raw = run_once(args.wallet, args.year, out_dir)
        print(f"Iteration {i}/{iterations}")
        print(f"Status: {verdict}")
        print(f"Top Issue: {issue_key}")
        if raw:
            print(f"Detail: {raw[:200]}")
        print(f"Recommended Fix: {title}")
        print()
        print(f"Files written under: {out_dir}")
        print("  - integrity_report.txt")
        print("  - next_fix_prompt.txt")
        print("  - context_summary.txt")
        print()
        print("Next Step: Apply next_fix_prompt.txt in Cursor, then re-run harvest/pipeline + integrity_check.")
        print("=" * 48)

        if verdict == "SAFE":
            print("Stopping: SAFE")
            sys.exit(0)
        if i < iterations:
            try:
                input("Press Enter after you applied the fix (or Ctrl+C to stop)... ")
            except EOFError:
                break

    sys.exit(0 if verdict == "SAFE" else 1)


if __name__ == "__main__":
    main()
