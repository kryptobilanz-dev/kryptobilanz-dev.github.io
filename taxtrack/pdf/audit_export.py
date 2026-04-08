# taxtrack/pdf/audit_export.py
"""Write consolidated audit JSON under taxtrack/data/out/audit/."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List


def _taxtrack_root() -> Path:
    return Path(__file__).resolve().parents[1]


def write_audit_json(
    wallet: str,
    tax_year: int,
    economic_rows: List[Dict[str, Any]],
    tax_summary: Dict[str, Any],
    validation: Dict[str, Any],
    confidence_summary: Dict[str, Any],
    problematic_tokens: List[Dict[str, Any]],
    unresolved_tx: List[str],
) -> Path:
    wallet_norm = (wallet or "").strip().lower()
    out_dir = _taxtrack_root() / "data" / "out" / "audit"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{wallet_norm}_{tax_year}_audit.json"

    payload = {
        "wallet": wallet_norm,
        "year": tax_year,
        "tax_summary": tax_summary,
        "economic_gains_tax_ready": economic_rows,
        "validation": validation,
        "confidence_distribution": confidence_summary,
        "problematic_tokens": problematic_tokens,
        "unresolved_tx_hashes": unresolved_tx,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
