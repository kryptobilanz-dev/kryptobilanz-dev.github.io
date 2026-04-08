"""
Derive transaction direction from wallet and addresses. Used by all loaders.
Direction must NOT come from CSV; it is derived deterministically.
All comparisons are lowercase-normalized.
"""


def derive_direction(wallet: str, from_addr: str, to_addr: str) -> str:
    """
    Derive direction from wallet and transfer addresses.

    Rules (order matters):
      - from_addr == wallet and to_addr == wallet  → "internal"
      - from_addr == wallet                         → "out"
      - to_addr == wallet                           → "in"
      - else                                        → "unknown"

    Wallet and addresses are normalized to lowercase for comparison.
    """
    wallet = (wallet or "").lower().strip()
    from_addr = (from_addr or "").lower().strip()
    to_addr = (to_addr or "").lower().strip()

    if from_addr == wallet and to_addr == wallet:
        return "internal"
    if from_addr == wallet:
        return "out"
    if to_addr == wallet:
        return "in"
    return "unknown"


def assert_direction_derivation(row, wallet: str) -> None:
    """
    Raise RuntimeError if row has direction "unknown" but from_addr or to_addr
    equals wallet (direction derivation would have produced in/out/internal).
    Call after RawRow creation to catch derivation bugs.
    """
    direction = (getattr(row, "direction", None) or (row.get("direction") if isinstance(row, dict) else None) or "").strip().lower()
    if direction != "unknown":
        return
    from_addr = (getattr(row, "from_addr", None) or (row.get("from_addr") or row.get("from") if isinstance(row, dict) else None) or "").strip().lower()
    to_addr = (getattr(row, "to_addr", None) or (row.get("to_addr") or row.get("to") if isinstance(row, dict) else None) or "").strip().lower()
    wallet_l = (wallet or "").lower().strip()
    if from_addr == wallet_l or to_addr == wallet_l:
        raise RuntimeError(
            "Direction derivation failed: direction is 'unknown' but wallet matches from_addr or to_addr "
            f"(wallet={wallet_l[:20]}..., from_addr={from_addr[:20] if from_addr else ''}..., to_addr={to_addr[:20] if to_addr else ''}...)"
        )
