# tests/test_rawrow_validation.py
"""
Phase 1 Data Integrity: validate the RawRow contract.
Every RawRow created by any loader must pass validate_rawrow().
Run: pytest tests/test_rawrow_validation.py
"""

import pytest
from taxtrack.schemas.RawRow import RawRow
from taxtrack.validation.raw_row import validate_raw_row, ALLOWED_DIRECTIONS


def _valid_rawrow(**overrides) -> RawRow:
    """Base valid RawRow; override any field via kwargs."""
    defaults = {
        "source": "test",
        "tx_hash": "0xabc123",
        "timestamp": 1700000000,
        "dt_iso": "2023-11-15T00:00:00",
        "from_addr": "0xfrom",
        "to_addr": "0xto",
        "token": "ETH",
        "amount": 1.0,
        "direction": "in",
        "method": "transfer",
        "chain_id": "eth",
    }
    defaults.update(overrides)
    return RawRow(**defaults)


# A) Valid RawRow should pass
def test_valid_rawrow_passes():
    row = _valid_rawrow()
    validate_raw_row(row)


def test_valid_rawrow_dict_passes():
    row = _valid_rawrow()
    validate_raw_row(row.to_dict())


# B) Missing tx_hash should fail
def test_missing_tx_hash_fails():
    row = _valid_rawrow(tx_hash="")
    with pytest.raises(ValueError) as exc_info:
        validate_raw_row(row)
    assert "tx_hash" in str(exc_info.value).lower() or "non-empty" in str(exc_info.value).lower()


# C) Missing token should fail
def test_missing_token_fails():
    row = _valid_rawrow(token="")
    with pytest.raises(ValueError) as exc_info:
        validate_raw_row(row)
    assert "token" in str(exc_info.value).lower() or "non-empty" in str(exc_info.value).lower()


# D) Missing / invalid timestamp should fail
def test_timestamp_zero_fails():
    row = _valid_rawrow(timestamp=0)
    with pytest.raises(ValueError) as exc_info:
        validate_raw_row(row)
    assert "timestamp" in str(exc_info.value).lower()


def test_timestamp_negative_fails():
    row = _valid_rawrow(timestamp=-1)
    with pytest.raises(ValueError) as exc_info:
        validate_raw_row(row)
    assert "timestamp" in str(exc_info.value).lower()


# E) Invalid direction should fail
def test_invalid_direction_fails():
    row = _valid_rawrow(direction="other")
    with pytest.raises(ValueError) as exc_info:
        validate_raw_row(row)
    assert "direction" in str(exc_info.value).lower()


def test_allowed_directions_pass():
    for direction in ALLOWED_DIRECTIONS:
        row = _valid_rawrow(direction=direction)
        validate_raw_row(row)


# F) Amount must be numeric and > 0
def test_amount_zero_fails():
    row = _valid_rawrow(amount=0.0)
    with pytest.raises(ValueError) as exc_info:
        validate_raw_row(row)
    assert "amount" in str(exc_info.value).lower()


def test_amount_negative_fails():
    row = _valid_rawrow(amount=-1.0)
    with pytest.raises(ValueError) as exc_info:
        validate_raw_row(row)
    assert "amount" in str(exc_info.value).lower()


def test_empty_row_fails():
    with pytest.raises(ValueError) as exc_info:
        validate_raw_row({})
    assert "empty" in str(exc_info.value).lower() or "validation" in str(exc_info.value).lower()
