from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


@dataclass
class SolanaRawTx:
    """
    Minimal raw Solana transaction container (signature-centric).

    The loader will eventually normalize various inputs (RPC JSON, explorers, indexers)
    into this representation.
    """

    signature: str
    block_time: int
    payload: Dict[str, Any]


def load_solana(path: Path, *, wallet: str | None = None) -> List[SolanaRawTx]:
    """
    Load Solana-native transactions from a file export.

    Placeholder: actual formats will be added (RPC JSON, Helius dumps, etc.).
    """
    _ = wallet
    raise NotImplementedError("Solana loader not implemented yet")

