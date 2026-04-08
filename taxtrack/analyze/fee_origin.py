# taxtrack/analyze/fee_origin.py
from __future__ import annotations

def _classify_fee_origin(record: dict) -> None:
    """
    Klassifiziert die Gebühr für einen einzelnen Record.
    """
    method = (record.get("method") or "").lower()
    source = (record.get("source") or "").lower()
    category = (record.get("category") or "").lower()

    # 1) klassische ETH Fees
    if record.get("fee_amount", 0) and source == "etherscan":
        record["fee_origin"] = "network"
        return

    # 2) Swap
    if "swap" in method or "swap" in category:
        record["fee_origin"] = "swap"
        return

    # 3) Protocol fees (Pendle, Beefy, Aave)
    if any(x in method for x in ["stake", "unstake", "withdraw", "deposit"]):
        record["fee_origin"] = "protocol"
        return

    # 4) Coinbase
    if source == "coinbase":
        record["fee_origin"] = "unknown"
        return

    record["fee_origin"] = "unknown"


def attach_fee_origin(records: list[dict]) -> list[dict]:
    """
    Wendet Fee-Origin auf eine Liste von Records an.
    """
    for r in records:
        _classify_fee_origin(r)

    return records
