from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CanonicalAssetId:
    """
    Canonical, chain-specific asset identifier.

    Examples:
      - EVM ERC-20:  eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48
      - Solana mint: solana:EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
    """

    value: str


def evm_erc20(chain_id: int, contract: str) -> CanonicalAssetId:
    c = (contract or "").strip().lower()
    return CanonicalAssetId(f"eip155:{int(chain_id)}/erc20:{c}")


def solana_mint(mint: str) -> CanonicalAssetId:
    m = (mint or "").strip()
    return CanonicalAssetId(f"solana:{m}")

