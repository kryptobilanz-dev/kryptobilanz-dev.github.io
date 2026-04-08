"""
Strict RawRow validator for Phase 1 Data Integrity.

Use in tests and optionally in loaders when DEBUG_VALIDATION is set.
Raises ValueError if the row violates the contract (required fields, direction, amount > 0, timestamp > 0).
"""

from typing import Any, Dict, Union

from taxtrack.validation.raw_row import (
    ALLOWED_DIRECTIONS,
    DEBUG_VALIDATION,
    REQUIRED_FIELDS,
    validate_raw_row,
)


def validate_rawrow(row: Union[Any, Dict[str, Any]]) -> None:
    """
    Backwards-compatible wrapper (deprecated).

    Single source of truth lives in `taxtrack.validation.raw_row.validate_raw_row`.
    This wrapper exists so existing loaders/tests can keep calling `validate_rawrow`.
    """
    validate_raw_row(row, require_chain_id=True, allowed_directions=ALLOWED_DIRECTIONS)
