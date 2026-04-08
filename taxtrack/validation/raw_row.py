"""
Strict validation for RawRow (or dict) before pipeline processing.

Required fields (must be present and "filled"):
  tx_hash, timestamp, dt_iso, token, amount, direction, from_addr, to_addr, method, chain_id

Raises RawRowValidationError with row index and missing/invalid field names.
"""

import os
from typing import Any, Dict, List, Optional, Set, Union
import math
from datetime import datetime, timezone, timedelta

# Required field names; all must be present and pass non-empty / type checks
REQUIRED_FIELDS = [
    "tx_hash",
    "timestamp",
    "dt_iso",
    "token",
    "amount",
    "direction",
    "from_addr",
    "to_addr",
    "method",
    "chain_id",
]

ALLOWED_DIRECTIONS = {"in", "out", "internal", "unknown"}

# Set TAXTRACK_DEBUG_VALIDATION=1 to enable loader-side validation
DEBUG_VALIDATION = os.environ.get("TAXTRACK_DEBUG_VALIDATION", "").lower() in ("1", "true", "yes")


class RawRowValidationError(ValueError):
    """Raised when a raw row (dict or RawRow) fails strict validation."""

    def __init__(
        self,
        message: str,
        *,
        row_index: Optional[int] = None,
        missing: Optional[List[str]] = None,
        invalid: Optional[List[str]] = None,
    ):
        super().__init__(message)
        self.row_index = row_index
        self.missing = missing or []
        self.invalid = invalid or []


def _row_as_dict(row: Union[Any, Dict[str, Any]]) -> Dict[str, Any]:
    """Normalize RawRow or dict to a single dict (using RawRow field names)."""
    if hasattr(row, "to_dict") and callable(row.to_dict):
        try:
            return row.to_dict()
        except Exception:
            # Defensive fallback if to_dict fails
            return {}
    if isinstance(row, dict):
        return row
    return {}


def _get_value(d: Dict[str, Any], key: str) -> Any:
    """Get value; support both RawRow-style (from_addr) and some loaders' 'from'/'to'."""
    if key in d:
        return d[key]
    if key == "from_addr" and "from" in d:
        return d["from"]
    if key == "to_addr" and "to" in d:
        return d["to"]
    return None


def _is_valid_iso_datetime(s: str) -> bool:
    """Check if a string is a valid ISO 8601 datetime.

    Enhanced to ensure:
    - Valid ISO format parseable by datetime.fromisoformat
    - The datetime is timezone-aware or explicitly naive (assumed UTC)
    - The datetime tzinfo offset is reasonable (<= 24h in absolute value)
    - The datetime is within a reasonable range (>= 1970-01-01 and <= now + 2 years)
    """
    try:
        if not s.strip():
            return False

        dt = datetime.fromisoformat(s)

        # Reject if string contains control characters or zero-width spaces
        if any(ord(c) < 32 for c in s):
            return False

        # Check tzinfo offset within +-24h
        if dt.tzinfo is not None:
            offset = dt.utcoffset()
            if offset is None:
                return False
            total_sec = abs(offset.total_seconds())
            if total_sec > 24 * 3600:
                return False
        # Naive datetime assumed UTC (no error), no tzinfo offset check needed

        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        try:
            two_years_future = now.replace(year=now.year + 2)
        except ValueError:
            # fallback for leap year
            two_years_future = now + timedelta(days=730)

        # Convert dt to aware datetime in UTC for comparison
        if dt.tzinfo is None:
            dt_aware = dt.replace(tzinfo=timezone.utc)
        else:
            dt_aware = dt.astimezone(timezone.utc)

        if not (epoch <= dt_aware <= two_years_future):
            return False

        return True
    except (ValueError, TypeError, OverflowError):
        return False


def validate_raw_row(
    row: Union[Any, Dict[str, Any]],
    *,
    index: Optional[int] = None,
    require_chain_id: bool = True,
    allowed_directions: Optional[Set[str]] = None,
) -> None:
    """
    Validate a single raw row (RawRow instance or dict). Raises RawRowValidationError
    if any required field is missing or invalid.

    Checks:
      - All REQUIRED_FIELDS are present.
      - tx_hash, dt_iso, token, direction, from_addr, to_addr, method, chain_id (if required)
        are non-empty strings without control characters.
      - dt_iso is a valid ISO 8601 datetime string within reasonable date range.
      - timestamp is int or int-convertible, > 0 and within reasonable bounds.
      - amount is numeric (int or float), finite (no NaN or infinite), and > 0.
      - direction is in allowed_directions (default: in, out, internal, unknown).
      - Protects against edge cases: e.g. empty strings, wrong types, extreme dates, NaN/inf.

    :param row: RawRow instance or dict (e.g. from to_dict() or loader).
    :param index: Optional 0-based row index for error reporting.
    :param require_chain_id: If True, chain_id must be non-empty.
    :param allowed_directions: Set of allowed direction values; default ALLOWED_DIRECTIONS.
    """
    d = _row_as_dict(row)
    if not d:
        raise RawRowValidationError(
            "Row is empty or not a dict/RawRow",
            row_index=index,
            missing=REQUIRED_FIELDS.copy(),
            invalid=[],
        )

    allowed = allowed_directions if allowed_directions is not None else ALLOWED_DIRECTIONS
    missing: List[str] = []
    invalid: List[str] = []

    def is_nonempty_str(x: Any) -> bool:
        return isinstance(x, str) and bool(x.strip())

    for key in REQUIRED_FIELDS:
        if key == "chain_id" and not require_chain_id:
            continue

        val = _get_value(d, key)

        # Check None or empty equivalents for string fields handled below
        if val is None:
            missing.append(key)
            continue

        # Validate string fields except chain_id, timestamp, amount, direction, dt_iso
        if key in ("tx_hash", "token", "from_addr", "to_addr", "method"):
            if not is_nonempty_str(val):
                missing.append(key)
                continue
            s = val.strip()
            # Disallow control characters or zero-width characters
            if any(ord(c) < 32 for c in s):
                invalid.append(key)

        elif key == "dt_iso":
            if not is_nonempty_str(val):
                missing.append(key)
                continue
            s = val.strip()
            # Disallow control or zero-width characters, and validate proper ISO datetime
            if any(ord(c) < 32 for c in s):
                invalid.append(key)
            elif not _is_valid_iso_datetime(s):
                invalid.append(key)

        elif key == "direction":
            if not is_nonempty_str(val):
                missing.append(key)
                continue
            dir_val = val.strip().lower()
            if dir_val not in allowed:
                invalid.append(key)

        elif key == "chain_id":
            # chain_id accepted as non-empty string without control characters, or int
            if isinstance(val, str):
                if not val.strip():
                    missing.append(key)
                    continue
                s = val.strip()
                if any(ord(c) < 32 for c in s):
                    invalid.append(key)
            elif isinstance(val, int):
                # Accept integers as is
                pass
            elif isinstance(val, float):
                # Reject NaN, inf floats (not valid chain_id)
                if math.isnan(val) or math.isinf(val):
                    invalid.append(key)
                else:
                    # floats that are integral numbers - reject anyway (chain_id should not be float)
                    invalid.append(key)
            else:
                invalid.append(key)

        elif key == "timestamp":
            # Accept int or integer-valued float or int-string representations; reject nan/inf
            try:
                if isinstance(val, float):
                    if math.isnan(val) or math.isinf(val):
                        invalid.append(key)
                        continue
                    if not val.is_integer():
                        invalid.append(key)
                        continue
                    ts = int(val)
                elif isinstance(val, int):
                    ts = val
                elif isinstance(val, str):
                    s = val.strip()
                    if not s:
                        missing.append(key)
                        continue
                    # Try parsing integer string with optional +/- sign
                    # Reject float string like '123.0'
                    if s.startswith(("+", "-")) and len(s) == 1:
                        invalid.append(key)
                        continue
                    # Confirm all chars except first (if + or -) are digits
                    start_idx = 1 if s[0] in "+-" else 0
                    if not s[start_idx:].isdigit():
                        invalid.append(key)
                        continue
                    # Convert to int
                    ts = int(s)
                else:
                    # Try int conversion for other types (defensive)
                    ts = int(val)
                # RawRow contract: timestamp must be strictly positive
                if ts <= 0:
                    invalid.append(key)
                    continue
                now_ts = int(datetime.now(timezone.utc).timestamp())
                two_years_seconds = 2 * 365 * 24 * 60 * 60
                if ts > now_ts + two_years_seconds:
                    invalid.append(key)
            except (TypeError, ValueError, OverflowError):
                invalid.append(key)

        elif key == "amount":
            # Must be numeric, finite (not NaN or infinite), and strictly positive.
            try:
                num = float(val)
                if math.isnan(num) or math.isinf(num):
                    invalid.append(key)
                elif num <= 0:
                    invalid.append(key)
            except (TypeError, ValueError):
                invalid.append(key)

        else:
            # Unknown key (should not happen) treated as missing (defensive)
            missing.append(key)

    if missing or invalid:
        parts = []
        if index is not None:
            parts.append(f"row index {index}")
        if missing:
            parts.append(f"missing or empty: {', '.join(sorted(missing))}")
        if invalid:
            parts.append(f"invalid: {', '.join(sorted(invalid))}")
        raise RawRowValidationError(
            "RawRow validation failed (" + "; ".join(parts) + ")",
            row_index=index,
            missing=missing,
            invalid=invalid,
        )


def validate_raw_row_dict(
    d: Dict[str, Any],
    *,
    index: Optional[int] = None,
    require_chain_id: bool = True,
    allowed_directions: Optional[Set[str]] = None,
) -> None:
    """Convenience wrapper: validate a dict row (e.g. from filtered_dicts)."""
    validate_raw_row(
        d,
        index=index,
        require_chain_id=require_chain_id,
        allowed_directions=allowed_directions,
    )


def validate_raw_rows(
    rows: List[Union[Any, Dict[str, Any]]],
    *,
    require_chain_id: bool = True,
    raise_on_first: bool = True,
) -> None:
    """
    Validate a list of raw rows. If raise_on_first is True (default), raises the first
    RawRowValidationError. Otherwise collects all errors and raises a single
    RawRowValidationError summarizing them (with ._all_errors attached).
    """
    errors: List[RawRowValidationError] = []
    for i, row in enumerate(rows):
        try:
            validate_raw_row(
                row,
                index=i,
                require_chain_id=require_chain_id,
            )
        except RawRowValidationError as e:
            if raise_on_first:
                raise
            errors.append(e)
    if errors:
        indices = [e.row_index for e in errors if e.row_index is not None]
        summary = RawRowValidationError(
            f"RawRow validation failed on {len(errors)} row(s): indices {indices[:10]}{'...' if len(indices) > 10 else ''}; "
            f"first row missing={errors[0].missing} invalid={errors[0].invalid}",
            row_index=errors[0].row_index,
            missing=errors[0].missing,
            invalid=errors[0].invalid,
        )
        setattr(summary, "_all_errors", errors)
        raise summary
