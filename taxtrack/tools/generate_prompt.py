from __future__ import annotations

import argparse
import json
import os
import textwrap
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _repo_taxtrack_root() -> Path:
    # taxtrack/tools -> taxtrack
    return Path(__file__).resolve().parents[1]


def _path_registry() -> Path:
    return _repo_taxtrack_root() / "data" / "registry"


def _path_loop() -> Path:
    return _repo_taxtrack_root() / "data" / "loop"


def _path_templates() -> Path:
    return _repo_taxtrack_root() / "tools" / "templates"


def _load_json(path: Path) -> Dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _load_json_list(path: Path) -> List[Dict[str, Any]]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        return obj if isinstance(obj, list) else []
    except Exception:
        return []


def _top_items(d: Dict[str, Any], *, n: int) -> List[Tuple[str, int]]:
    items: List[Tuple[str, int]] = []
    for k, v in (d or {}).items():
        try:
            items.append((str(k), int(v)))
        except Exception:
            continue
    items.sort(key=lambda kv: (-kv[1], kv[0]))
    return items[:n]


def _fmt_top(title: str, items: List[Tuple[str, int]]) -> List[str]:
    out = [title]
    if not items:
        out.append("  (none)")
        return out
    for k, v in items:
        out.append(f"  - {k}: {v}")
    return out


def _summarize_classified(rows: List[Dict[str, Any]], *, top_n: int = 12) -> List[str]:
    cats = Counter((r.get("category") or "").lower().strip() or "<empty>" for r in rows)
    tokens = Counter((r.get("token") or "").upper().strip() or "<empty>" for r in rows)
    methods = Counter((r.get("method") or "").strip() or "<empty>" for r in rows)

    lines: List[str] = []
    lines.append(f"Classified rows: {len(rows)}")
    lines.append("Top categories:")
    for k, v in cats.most_common(top_n):
        lines.append(f"  - {k}: {v}")
    lines.append("Top tokens:")
    for k, v in tokens.most_common(top_n):
        lines.append(f"  - {k}: {v}")
    lines.append("Top methods:")
    for k, v in methods.most_common(top_n):
        lines.append(f"  - {k}: {v}")
    return lines


def _read_optional_text(path: Path, *, max_lines: int = 80) -> List[str]:
    if not path.exists():
        return []
    try:
        txt = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return []
    if len(txt) > max_lines:
        return txt[:max_lines] + [f"... (truncated, {len(txt) - max_lines} more lines)"]
    return txt


def _load_template(template_name: str) -> Tuple[str, str]:
    """
    Template format:
      - optional first line: ONLY_MODIFY: ...
      - remainder: task template text
    """
    p = _path_templates() / f"{template_name}.txt"
    if not p.exists():
        raise SystemExit(f"Template not found: {p}")
    raw = p.read_text(encoding="utf-8", errors="replace").splitlines()
    only_modify = ""
    body_lines = raw
    if raw and raw[0].strip().upper().startswith("ONLY_MODIFY:"):
        only_modify = raw[0].split(":", 1)[1].strip()
        body_lines = raw[1:]
        # strip leading empty lines
        while body_lines and not body_lines[0].strip():
            body_lines = body_lines[1:]
    return only_modify, "\n".join(body_lines).strip() + "\n"


def _clamp_lines(lines: List[str], *, max_lines: int) -> List[str]:
    if len(lines) <= max_lines:
        return lines
    head = lines[: max_lines - 3]
    return head + ["", f"... (truncated to {max_lines} lines)", ""]


def generate_prompt(
    *,
    template: str,
    wallet: str,
    year: int,
    price_report: Optional[Path] = None,
    pipeline_summary: Optional[Path] = None,
    max_lines: int = 350,
) -> str:
    wallet = (wallet or "").strip().lower()
    reg_dir = _path_registry()
    loop_dir = _path_loop()
    loop_dir.mkdir(parents=True, exist_ok=True)

    unknown_path = reg_dir / "unknown_registry.json"
    sugg_path = reg_dir / "contract_suggestions.json"
    classified_path = _repo_taxtrack_root() / "data" / "harvest" / wallet / str(year) / "classified.json"

    unknown = _load_json(unknown_path)
    suggestions = _load_json(sugg_path)
    classified = _load_json_list(classified_path)

    only_modify, task_template = _load_template(template)

    # Unknown registry summaries
    top_unknown_tokens = _top_items(unknown.get("tokens") or {}, n=12)
    top_unknown_contracts = _top_items(unknown.get("unlabeled_contracts") or {}, n=12)
    amb = unknown.get("ambiguous_transfers") if isinstance(unknown.get("ambiguous_transfers"), dict) else {}
    top_amb_proto = _top_items(amb.get("by_protocol") or {}, n=10)
    top_amb_method = _top_items(amb.get("by_method") or {}, n=10)
    top_amb_contract = _top_items(amb.get("by_contract") or {}, n=10)

    # Contract suggestions summary (high confidence only)
    sugg_items: List[Tuple[str, float, str, str]] = []
    for addr, v in (suggestions or {}).items():
        if not isinstance(v, dict):
            continue
        conf = float(v.get("confidence") or 0.0)
        if conf < 0.7:
            continue
        proto = str(v.get("suggested_protocol") or "")
        typ = str(v.get("type") or "")
        sugg_items.append((str(addr).lower(), conf, proto, typ))
    sugg_items.sort(key=lambda x: (-x[1], x[0]))
    sugg_lines = ["High-confidence contract suggestions (>=0.7):"]
    if not sugg_items:
        sugg_lines.append("  (none)")
    else:
        for addr, conf, proto, typ in sugg_items[:12]:
            sugg_lines.append(f"  - {addr}  conf={conf:.2f}  proto={proto or '<none>'}  type={typ or '<none>'}")

    lines: List[str] = []
    lines.append("--- SYSTEM STATE ---")
    lines.append(f"Wallet: {wallet}")
    lines.append(f"Year: {year}")
    lines.append("")
    lines.extend(_fmt_top("Top Unknown Tokens:", top_unknown_tokens))
    lines.append("")
    lines.extend(_fmt_top("Top Unlabeled Contracts:", top_unknown_contracts))
    lines.append("")
    lines.extend(_fmt_top("Top Ambiguous Transfers (by protocol):", top_amb_proto))
    lines.append("")
    lines.extend(_fmt_top("Top Ambiguous Transfers (by method):", top_amb_method))
    lines.append("")
    lines.extend(_fmt_top("Top Ambiguous Transfers (by contract):", top_amb_contract))
    lines.append("")
    lines.extend(sugg_lines)
    lines.append("")
    lines.append("Harvest snapshot:")
    if classified_path.exists():
        lines.append(f"  classified.json: {classified_path}")
    else:
        lines.append(f"  classified.json: MISSING ({classified_path})")
    lines.append("")
    lines.extend(_summarize_classified(classified))
    lines.append("")

    if price_report:
        pr = _read_optional_text(price_report)
        if pr:
            lines.append("Optional price_report.txt (excerpt):")
            lines.extend("  " + x for x in pr)
            lines.append("")

    if pipeline_summary:
        ps = _read_optional_text(pipeline_summary)
        if ps:
            lines.append("Optional pipeline_summary.txt (excerpt):")
            lines.extend("  " + x for x in ps)
            lines.append("")

    lines.append("--- TASK ---")
    lines.append(task_template.rstrip())
    lines.append("")
    lines.append("--- CONSTRAINTS ---")
    lines.append("- Do not touch swap logic")
    lines.append("- Do not touch FIFO")
    lines.append("- Do not touch price logic (unless template explicitly says so)")
    if only_modify:
        lines.append(f"- Only modify: {only_modify}")
    lines.append("")

    # Clamp to keep Cursor-friendly size
    lines = _clamp_lines(lines, max_lines=max_lines)
    return "\n".join(lines).rstrip() + "\n"


def main() -> None:
    p = argparse.ArgumentParser(description="Generate a Cursor-ready prompt from local TaxTrack state.")
    p.add_argument("--template", required=True, help="Template name (without .txt), e.g. phase2")
    p.add_argument("--wallet", required=True, help="Wallet address")
    p.add_argument("--year", type=int, required=True, help="Tax year")
    p.add_argument("--price-report", default=None, help="Optional path to price_report.txt")
    p.add_argument("--pipeline-summary", default=None, help="Optional path to pipeline_summary.txt")
    p.add_argument("--max-lines", type=int, default=350, help="Clamp output to N lines (default 350)")
    p.add_argument("--open", action="store_true", help="Open generated prompt.txt automatically")
    args = p.parse_args()

    price_report = Path(args.price_report) if args.price_report else None
    pipeline_summary = Path(args.pipeline_summary) if args.pipeline_summary else None

    prompt = generate_prompt(
        template=args.template,
        wallet=args.wallet,
        year=int(args.year),
        price_report=price_report,
        pipeline_summary=pipeline_summary,
        max_lines=int(args.max_lines),
    )

    out_path = _path_loop() / "prompt.txt"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(prompt, encoding="utf-8")
    print(f"[PROMPT] wrote {out_path}")

    if args.open:
        try:
            if os.name == "nt":
                os.startfile(str(out_path))  # type: ignore[attr-defined]
            else:
                import webbrowser

                webbrowser.open(out_path.as_uri())
        except Exception as e:
            print(f"[PROMPT][WARN] open failed: {e!r}")


if __name__ == "__main__":
    main()

