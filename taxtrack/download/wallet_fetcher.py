"""
Ensure transaction CSVs exist for a wallet/chain before running the pipeline.

Detects if normal.csv, erc20.csv, internal.csv are missing in
taxtrack/data/inbox/<wallet>/<chain_id>/ and, when missing (or all empty),
fetches from Etherscan-compatible APIs and writes the expected files so existing loaders
work without modification.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# Same filenames expected by pipeline._load_transactions and loaders
REQUIRED_CSV_NAMES = ("normal.csv", "erc20.csv", "internal.csv")


@dataclass
class IngestOutcome:
    """Result of ensure_transactions_for_wallet_chain (single chain)."""

    ok: bool
    skipped_use_existing: bool
    raw_row_total: int
    message: str
    api_status: str  # e.g. "OK", "FAILED"


class DataIngestFailedError(RuntimeError):
    """Raised when inbox fetch failed or produced no usable rows (see message)."""


def count_inbox_chain_rows(inbox_root: Path, wallet: str, chain_id: str) -> int:
    """Public: total data rows in normal+erc20+internal CSVs for one wallet/chain."""
    w = wallet.strip().lower()
    c = chain_id.strip().lower()
    return _count_csv_data_rows(inbox_root / w / c)


def _count_csv_data_rows(chain_dir: Path) -> int:
    """Sum of data rows (excluding header) across the three CSVs if present."""
    total = 0
    for name in REQUIRED_CSV_NAMES:
        p = chain_dir / name
        if not p.is_file():
            continue
        lines = p.read_text(encoding="utf-8", errors="replace").splitlines()
        if len(lines) <= 1:
            continue
        total += len(lines) - 1
    return total


def csvs_missing_for_chain(inbox_root: Path, wallet: str, chain_id: str) -> bool:
    """
    Return True if the chain directory does not exist or any of the three
    required CSVs (normal.csv, erc20.csv, internal.csv) is missing.

    Used to decide whether to fetch transactions for this wallet/chain.
    """
    chain_dir = inbox_root / wallet.strip().lower() / chain_id.strip().lower()
    if not chain_dir.is_dir():
        return True
    for name in REQUIRED_CSV_NAMES:
        if not (chain_dir / name).is_file():
            return True
    return False


def csvs_empty_or_stale(inbox_root: Path, wallet: str, chain_id: str) -> bool:
    """
    True if CSVs are missing OR all three exist but contain no data rows (headers only).
    This avoids "success" after a previous run wrote empty CSVs when the API failed silently.
    """
    wallet = wallet.strip().lower()
    chain_id = chain_id.strip().lower()
    chain_dir = inbox_root / wallet / chain_id
    if csvs_missing_for_chain(inbox_root, wallet, chain_id):
        return True
    return _count_csv_data_rows(chain_dir) == 0


def ensure_transactions_for_wallet_chain(
    wallet: str,
    chain_id: str,
    inbox_root: Path,
    api_key: Optional[str] = None,
) -> IngestOutcome:
    """
    If CSVs are missing or empty, fetch from the chain API and
    write normal.csv, erc20.csv, internal.csv into inbox_root/<wallet>/<chain_id>/.

    Returns IngestOutcome with ok=True when data is present (existing or freshly fetched).
    ok=False when download failed (see message); caller should treat as FAILED ingest.
    """
    from taxtrack.download.etherscan_fetcher import (
        MissingExplorerAPIKeyError,
        download_chain,
    )

    wallet = wallet.strip().lower()
    chain_id = chain_id.strip().lower()
    chain_dir = inbox_root / wallet / chain_id

    if not csvs_empty_or_stale(inbox_root, wallet, chain_id):
        n = _count_csv_data_rows(chain_dir)
        return IngestOutcome(
            ok=True,
            skipped_use_existing=True,
            raw_row_total=n,
            message=f"using existing inbox CSVs ({n} raw rows)",
            api_status="OK",
        )

    try:
        meta = download_chain(
            chain_id=chain_id,
            address=wallet,
            output_dir=chain_dir,
            api_key=api_key,
        )
        total = int(meta.get("total_raw") or 0)
        src = meta.get("source") or "unknown"
        return IngestOutcome(
            ok=True,
            skipped_use_existing=False,
            raw_row_total=total,
            message=f"fetched via {src}",
            api_status=str(meta.get("api_status") or "OK"),
        )
    except MissingExplorerAPIKeyError as e:
        return IngestOutcome(
            ok=False,
            skipped_use_existing=False,
            raw_row_total=0,
            message=str(e),
            api_status="FAILED",
        )
    except Exception as e:
        return IngestOutcome(
            ok=False,
            skipped_use_existing=False,
            raw_row_total=0,
            message=str(e),
            api_status="FAILED",
        )
