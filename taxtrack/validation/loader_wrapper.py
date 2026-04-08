"""
Loader validation wrapper: run a loader, validate each row, optionally log bad rows
and collect stats for a debug report.

Usage:
  from taxtrack.validation.loader_wrapper import load_with_validation

  rows, report = load_with_validation(
      load_etherscan,
      (path, wallet, chain_id),
      {},
      validate=True,
      log_bad_rows=True,
      require_chain_id=False,
  )
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from taxtrack.validation.raw_row import (
    RawRowValidationError,
    _row_as_dict,
    validate_raw_row,
)

logger = logging.getLogger(__name__)


def load_with_validation(
    loader_fn: Callable[..., List[Any]],
    loader_args: tuple,
    loader_kwargs: Optional[Dict[str, Any]] = None,
    *,
    validate: bool = True,
    require_chain_id: bool = False,
    allowed_directions: Optional[set] = None,
    log_bad_rows: bool = True,
    max_bad_rows_logged: int = 20,
    skip_invalid: bool = False,
) -> Tuple[List[Any], Dict[str, Any]]:
    """
    Run loader_fn(*loader_args, **loader_kwargs), then optionally validate each row.
    Returns (rows, report).

    If validate is True:
      - Each row is validated (RawRow or dict). Invalid rows are counted and optionally
        logged; if skip_invalid is False (default), the first validation error is raised.
      - If skip_invalid is True, invalid rows are dropped and the rest returned.

    report dict:
      - "loaded": int (raw count from loader)
      - "valid": int
      - "invalid": int
      - "invalid_indices": list[int]
      - "invalid_details": list[dict] (index, missing, invalid) for up to max_bad_rows_logged
      - "skipped" (if skip_invalid): list of row indices that were dropped
    """
    loader_kwargs = loader_kwargs or {}
    raw = loader_fn(*loader_args, **loader_kwargs)
    report = {
        "loaded": len(raw),
        "valid": 0,
        "invalid": 0,
        "invalid_indices": [],
        "invalid_details": [],
        "skipped": [],
    }

    if not validate:
        report["valid"] = len(raw)
        return raw, report

    valid_rows = []
    for i, row in enumerate(raw):
        try:
            validate_raw_row(
                row,
                index=i,
                require_chain_id=require_chain_id,
                allowed_directions=allowed_directions,
            )
            report["valid"] += 1
            valid_rows.append(row)
        except RawRowValidationError as e:
            report["invalid"] += 1
            report["invalid_indices"].append(i)
            if len(report["invalid_details"]) < max_bad_rows_logged:
                report["invalid_details"].append({
                    "index": i,
                    "missing": e.missing,
                    "invalid": e.invalid,
                })
            if log_bad_rows:
                logger.warning(
                    "[loader_validation] Row %s: missing=%s invalid=%s",
                    i, e.missing, e.invalid,
                )
            if not skip_invalid:
                raise
            report["skipped"].append(i)

    if skip_invalid:
        return valid_rows, report
    return raw, report


def build_loader_debug_report(
    per_file_reports: List[Dict[str, Any]],
    *,
    source_labels: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Build a small debug report from multiple load_with_validation reports.

    per_file_reports: list of report dicts from load_with_validation.
    source_labels: optional list of labels (e.g. file paths) for each report.

    Returns a dict with:
      - total_loaded, total_valid, total_invalid
      - by_source: list of { label?, loaded, valid, invalid, invalid_indices? }
    """
    total_loaded = sum(r["loaded"] for r in per_file_reports)
    total_valid = sum(r["valid"] for r in per_file_reports)
    total_invalid = sum(r["invalid"] for r in per_file_reports)

    by_source = []
    for idx, r in enumerate(per_file_reports):
        entry = {
            "loaded": r["loaded"],
            "valid": r["valid"],
            "invalid": r["invalid"],
        }
        if source_labels and idx < len(source_labels):
            entry["label"] = source_labels[idx]
        if r.get("invalid_indices"):
            entry["invalid_indices"] = r["invalid_indices"][:50]
        if r.get("invalid_details"):
            entry["invalid_details"] = r["invalid_details"][:10]
        by_source.append(entry)

    return {
        "total_loaded": total_loaded,
        "total_valid": total_valid,
        "total_invalid": total_invalid,
        "by_source": by_source,
    }
