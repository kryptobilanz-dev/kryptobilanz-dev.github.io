"""
Single entry point for TaxTrack / KryptoBilanz CLI (Option B).

Usage::

    python -m taxtrack --help
    python -m taxtrack customer --customer <slug> --year 2025
    python -m taxtrack download-wallet --wallet 0x... --chains eth,arb
    python -m taxtrack legacy --wallet mm_main --chain-id eth --year 2025

Legacy single-file runner lives in ``taxtrack.legacy.main_legacy``.
"""

from __future__ import annotations

import sys
from typing import List, Sequence


def _print_help() -> None:
    print(
        """TaxTrack / KryptoBilanz

Commands:
  customer         Run unified pipeline for a customer (recommended).
  download-wallet  Download EVM CSVs into inbox (Etherscan-compatible APIs).
  legacy           Legacy single-wallet + chain folder runner (simplified PDF).

Examples:
  python -m taxtrack customer --customer stefan --year 2025
  python -m taxtrack customer --customer-dir "C:/path/to/customer" --customer stefan --year 2025
  python -m taxtrack download-wallet --wallet 0x... --chains eth,arb --inbox "C:/workspace/customers/x/inbox"
  python -m taxtrack legacy --wallet mm_main --chain-id eth --year 2025 --out data/out
"""
    )


def main(argv: Sequence[str] | None = None) -> None:
    if argv is None:
        argv = sys.argv[1:]
    argv_list: List[str] = list(argv)

    if not argv_list:
        _print_help()
        sys.exit(2)
    if argv_list[0] in ("-h", "--help", "help"):
        _print_help()
        sys.exit(0)

    cmd = argv_list[0]
    rest = argv_list[1:]

    if cmd == "customer":
        from taxtrack.root.run_customer import main as run_customer_main

        run_customer_main(rest)
        return

    if cmd in ("download-wallet", "download_wallet"):
        from taxtrack.root.download_wallet import main as download_main

        download_main(rest)
        return

    if cmd == "legacy":
        from taxtrack.legacy.main_legacy import main as legacy_main

        legacy_main(rest)
        return

    print(f"Unknown command: {cmd!r}", file=sys.stderr)
    _print_help()
    sys.exit(2)


if __name__ == "__main__":
    main()
