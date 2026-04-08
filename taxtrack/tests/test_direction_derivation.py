# taxtrack/tests/test_direction_derivation.py

import pytest
from taxtrack.utils.direction import derive_direction


# Wallet used in tests
WALLET = "0xabc123"


def test_wallet_sends_token():
    """Wallet is from_addr; someone else is to_addr → out."""
    assert derive_direction(WALLET, WALLET, "0xrecipient") == "out"
    assert derive_direction(WALLET, "0xabc123", "0xrecipient") == "out"
    assert derive_direction("0xABC123", "0xabc123", "0xRECIPIENT") == "out"


def test_wallet_receives_token():
    """Wallet is to_addr; someone else is from_addr → in."""
    assert derive_direction(WALLET, "0xsender", WALLET) == "in"
    assert derive_direction(WALLET, "0xsender", "0xabc123") == "in"
    assert derive_direction("0xABC123", "0xSENDER", "0xabc123") == "in"


def test_wallet_internal_transfer():
    """Wallet is both from and to → internal."""
    assert derive_direction(WALLET, WALLET, WALLET) == "internal"
    assert derive_direction("0xABC123", "0xabc123", "0xABC123") == "internal"


def test_third_party_transfer():
    """Neither from nor to is wallet → unknown."""
    assert derive_direction(WALLET, "0xsender", "0xrecipient") == "unknown"
    assert derive_direction(WALLET, "0xa", "0xb") == "unknown"


def test_empty_wallet():
    """Empty wallet never matches → unknown unless addresses are also empty."""
    assert derive_direction("", "0xa", "0xb") == "unknown"
    assert derive_direction("", "", "") == "internal"  # f=="" and t=="" and w==""


def test_none_handling():
    """None is treated as empty string; empty wallet matches nothing."""
    assert derive_direction(None, WALLET, "0xother") == "unknown"  # wallet="" so no match
    assert derive_direction(WALLET, None, WALLET) == "in"  # to_addr == wallet
    assert derive_direction(WALLET, "0xa", None) == "unknown"  # to_addr="" != wallet
