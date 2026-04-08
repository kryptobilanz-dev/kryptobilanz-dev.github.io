# taxtrack/download – fetch transaction data from Etherscan-compatible APIs

from taxtrack.download.etherscan_fetcher import (
    download_chain,
    fetch_normal_txs,
    fetch_erc20_txs,
    fetch_internal_txs,
)

__all__ = [
    "download_chain",
    "fetch_normal_txs",
    "fetch_erc20_txs",
    "fetch_internal_txs",
]
