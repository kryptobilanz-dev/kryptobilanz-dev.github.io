"""
Controlled iteration: GPT then Cursor then confirm; strict scope PDF tax-ready reporting.

Does not auto-apply code. Human confirms each iteration.

Usage:
  set OPENAI_API_KEY   # optional; without it, prompts are written for manual GPT paste
  python -m taxtrack.tools.reporting_loop --wallet 0x... --year 2025 --iterations 20
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _taxtrack_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _loop_dir() -> Path:
    return _taxtrack_root() / "data" / "loop"


def _reporting_out_dir() -> Path:
    p = _loop_dir() / "reporting"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _state_path() -> Path:
    return _loop_dir() / "reporting_state.json"


def _log_path() -> Path:
    return _loop_dir() / "reporting_loop.log"


def _templates_dir() -> Path:
    return Path(__file__).resolve().parent / "templates"


def _log(msg: str) -> None:
    line = f"{datetime.now(timezone.utc).isoformat()} {msg}\n"
    try:
        with _log_path().open("a", encoding="utf-8") as f:
            f.write(line)
    except Exception:
        pass
    print(msg, flush=True)


def load_state() -> Dict[str, Any]:
    p = _state_path()
    if not p.exists():
        return {
            "wallet": "",
            "year": None,
            "iterations_planned": 20,
            "last_iteration": 0,
            "accepted_responses": 0,
            "rejected_responses": 0,
            "last_change_summary": "",
            "history": [],
        }
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {
            "wallet": "",
            "year": None,
            "iterations_planned": 20,
            "last_iteration": 0,
            "accepted_responses": 0,
            "rejected_responses": 0,
            "last_change_summary": "",
            "history": [],
        }


def save_state(state: Dict[str, Any]) -> None:
    _loop_dir().mkdir(parents=True, exist_ok=True)
    _state_path().write_text(
        json.dumps(state, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# GPT output filter (Step 4)
# ---------------------------------------------------------------------------

_FORBIDDEN = re.compile(
    r"(?i)"
    r"(taxtrack/rules/evaluate|rules/evaluate\.py|\blot_tracker\.py\b|\bgain_grouping\.py\b|"
    r"\btoken_mapper\.py\b|\bprice_provider\.py\b|\bswap_engine\.py\b|"
    r"\bclassify_batch\b|\bevaluate_batch\b|\bcoingecko\b|"
    r"\btax_interpreter_de\.py\b|\bfifo_from_economic\b)"
)

_ALLOWED = re.compile(
    r"(?i)"
    r"(pdf_report|build_pdf|tax_ready|tax_summary|economic_gains_tax_ready|"
    r"taxtrack/pdf/|pipeline\.py|reportlab|executive_summary|section_|"
    r"platypus|aggregates|EStG|steuerfrei|speculative_bucket|long_term_bucket)"
)


def filter_gpt_response(text: str) -> Tuple[bool, str]:
    if not (text or "").strip():
        return False, "empty response"
    if _FORBIDDEN.search(text):
        return False, "forbidden topic (classification/FIFO/price/swap/tax-logic file touch)"
    if not _ALLOWED.search(text):
        return False, "no on-topic PDF/tax-ready keywords"
    return True, "ok"


# ---------------------------------------------------------------------------
# Prompt (Step 3 strict structure + template)
# ---------------------------------------------------------------------------

def build_iteration_prompt(*, iteration: int, total: int, wallet: str, year: int) -> str:
    tpl = (_templates_dir() / "reporting_fix.txt").read_text(encoding="utf-8", errors="replace")
    if tpl.lstrip().upper().startswith("ONLY_MODIFY:"):
        lines = tpl.splitlines()
        tpl_body = "\n".join(lines[1:]).lstrip()
    else:
        tpl_body = tpl

    header = f"""--- ITERATION {iteration} / {total} ---
Wallet: {wallet}
Year: {year}
Timestamp UTC: {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")}

--- FOCUS ---
You MUST ONLY work on:
PDF reporting using tax-ready data
IGNORE EVERYTHING ELSE

--- CURRENT STATE ---
- PDF uses economic_gains via economic_records in build_pdf().
- tax-ready data exists: economic_gains_tax_ready + tax_summary (pipeline).

--- TASK ---
Fix PDF so that it uses economic_gains_tax_ready for Sec.23 / taxable vs tax-free.

--- CONSTRAINTS ---
- do NOT modify tax logic modules
- do NOT modify FIFO
- do NOT modify classification
- only modify reporting layer (+ minimal pipeline data passing)

--- SUCCESS CRITERIA ---
- PDF shows correct taxable vs tax-free
- Sec.23 logic visible in report
- no regression

--- TEMPLATE (follow) ---
"""
    return header + "\n" + tpl_body.strip() + "\n"


# ---------------------------------------------------------------------------
# OpenAI (optional)
# ---------------------------------------------------------------------------

def call_openai_chat(
    user_prompt: str,
    *,
    model: str | None = None,
) -> str:
    key = (os.environ.get("OPENAI_API_KEY") or "").strip()
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set")
    m = model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
    body = json.dumps(
        {
            "model": m,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You output only PDF/reporting wiring changes for TaxTrack. "
                        "Never suggest edits to classification, FIFO, swaps, or price engines."
                    ),
                },
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.15,
            "max_tokens": 6000,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=body,
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        err = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"OpenAI HTTP {e.code}: {err[:500]}") from e
    choices = data.get("choices") or []
    if not choices:
        raise RuntimeError("OpenAI: no choices")
    msg = (choices[0].get("message") or {}).get("content") or ""
    return str(msg).strip()


def write_cursor_instruction(iteration: int, gpt_text: str, accepted: bool, out_dir: Path) -> Path:
    p = out_dir / f"cursor_instruction_iter_{iteration:02d}.txt"
    body = f"""# Iteration {iteration} — Cursor instruction (reporting only)

## Scope
Only edit: taxtrack/pdf/** , taxtrack/root/pipeline.py (pass tax_ready + tax_summary into PDF), taxtrack/export/** (optional).

## GPT response filter
Status: {"ACCEPTED" if accepted else "REJECTED — do not blindly apply; re-run iteration or fix prompt"}

## Paste / apply
1. Read the GPT response in gpt_response_iter_{iteration:02d}.md
2. In Cursor, implement only reporting-layer changes.
3. Do not touch FIFO, classification, tax_interpreter, prices.

## GPT output (copy)
```
{gpt_text[:12000]}{'...[truncated]' if len(gpt_text) > 12000 else ''}
```
"""
    p.write_text(body, encoding="utf-8")
    return p


def main() -> None:
    ap = argparse.ArgumentParser(description="Reporting fix loop: GPT, then Cursor, then confirm")
    ap.add_argument("--wallet", required=True, help="Wallet address (for state / prompt context)")
    ap.add_argument("--year", type=int, required=True, help="Tax year")
    ap.add_argument("--iterations", type=int, default=20, help="Number of loop iterations (default 20)")
    ap.add_argument("--start-from", type=int, default=1, help="Start iteration number (resume)")
    ap.add_argument(
        "--no-gpt",
        action="store_true",
        help="Do not call OpenAI; write prompt only (paste into ChatGPT manually)",
    )
    ap.add_argument(
        "--gpt-file",
        default=None,
        help="Path to file with GPT response for current iteration (skips API)",
    )
    ap.add_argument("--model", default=None, help="OpenAI model override")
    args = ap.parse_args()

    wallet = args.wallet.strip()
    year = int(args.year)
    n_iter = max(1, int(args.iterations))
    start = max(1, int(args.start_from))
    out_dir = _reporting_out_dir()
    state = load_state()
    state["wallet"] = wallet
    state["year"] = year
    state["iterations_planned"] = n_iter

    _log(f"[REPORTING_LOOP] start wallet={wallet[:10]}... year={year} iterations={n_iter} start={start}")

    for i in range(start, n_iter + 1):
        prompt = build_iteration_prompt(iteration=i, total=n_iter, wallet=wallet, year=year)
        prompt_path = out_dir / f"prompt_iter_{i:02d}.txt"
        prompt_path.write_text(prompt, encoding="utf-8")
        _log(f"[ITER {i}] wrote prompt -> {prompt_path}")

        gpt_text = ""
        if args.gpt_file:
            gf = Path(args.gpt_file)
            if not gf.is_file():
                _log(f"[ITER {i}] ERROR --gpt-file not found: {gf}")
                sys.exit(1)
            gpt_text = gf.read_text(encoding="utf-8", errors="replace")
            (out_dir / f"gpt_response_iter_{i:02d}.md").write_text(gpt_text, encoding="utf-8")
        elif args.no_gpt:
            gpt_path_expected = out_dir / f"gpt_response_iter_{i:02d}.md"
            print(f"\n[--no-gpt] Copy prompt from:\n  {prompt_path}\n")
            print(f"Paste GPT reply into:\n  {gpt_path_expected}\n")
            try:
                input("Press Enter when the GPT response file is saved... ")
            except EOFError:
                pass
            if gpt_path_expected.is_file():
                gpt_text = gpt_path_expected.read_text(encoding="utf-8", errors="replace")
            else:
                _log(f"[ITER {i}] no file at {gpt_path_expected} - empty GPT body")
                gpt_text = ""
        else:
            try:
                gpt_text = call_openai_chat(prompt, model=args.model)
            except Exception as e:
                _log(f"[ITER {i}] GPT call failed: {e!r} - use --no-gpt and paste manually")
                gpt_text = ""

        gpt_path = out_dir / f"gpt_response_iter_{i:02d}.md"
        if gpt_text:
            gpt_path.write_text(gpt_text, encoding="utf-8")
            _log(f"[ITER {i}] saved GPT response -> {gpt_path}")

        accepted, reason = filter_gpt_response(gpt_text) if gpt_text else (False, "no gpt output")
        if accepted:
            state["accepted_responses"] = int(state.get("accepted_responses") or 0) + 1
        else:
            state["rejected_responses"] = int(state.get("rejected_responses") or 0) + 1

        cpath = write_cursor_instruction(i, gpt_text or "(empty)", accepted, out_dir)
        _log(f"[ITER {i}] filter: {reason} | cursor instruction -> {cpath}")

        hist = {
            "iteration": i,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "prompt_path": str(prompt_path),
            "gpt_response_path": str(gpt_path) if gpt_text else None,
            "filter_accepted": accepted,
            "filter_reason": reason,
            "user_confirmed": None,
            "user_note": None,
        }
        state.setdefault("history", []).append(hist)
        state["last_iteration"] = i
        if gpt_text and accepted:
            state["last_change_summary"] = (gpt_text[:280].replace("\n", " ") + "...") if len(gpt_text) > 280 else gpt_text
        save_state(state)

        # Step 5-8: manual confirmation (no auto-apply)
        print("\n" + "=" * 72)
        print(f"ITERATION {i}/{n_iter}")
        print("=" * 72)
        print(f"Prompt:  {prompt_path}")
        print(f"GPT out: {gpt_path if gpt_text else '(generate via --no-gpt + manual paste)'}")
        print(f"Filter:  {'ACCEPTED' if accepted else 'REJECTED'} - {reason}")
        print(f"Cursor:  {cpath}")
        print("-" * 72)
        print("Apply changes in Cursor only under taxtrack/pdf/, pipeline wiring, export/.")
        try:
            u = input("Confirm iteration done [Enter] | skip s | quit q: ").strip().lower()
        except EOFError:
            u = "q"
        if u == "q":
            _log(f"[ITER {i}] user quit")
            break
        hist["user_confirmed"] = u != "s"
        hist["user_note"] = "skipped" if u == "s" else "confirmed"
        state["history"][-1] = hist
        save_state(state)
        if u == "s":
            _log(f"[ITER {i}] user skipped confirmation")

    _log("[REPORTING_LOOP] finished")
    print(f"\nState saved: {_state_path()}")


if __name__ == "__main__":
    main()
