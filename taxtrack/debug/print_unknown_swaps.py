# taxtrack/debug/print_unknown_swaps.py

from __future__ import annotations

from pathlib import Path
from typing import List

from taxtrack.loaders.evm_master_loader import load_evm_folder
from taxtrack.analyze.swap_engine import find_unknown_swaps
from taxtrack.schemas.RawRow import RawRow


def main() -> None:
    # TODO: auf deine Umgebung anpassen
    folder = Path("taxtrack/data/inbox/mm_main/eth")
    wallet = "mm_main"       # oder deine Wallet-Bezeichnung aus den CSVs
    chain_id = "eth"         # für Ethereum

    print(f"[DEBUG] Lade EVM-Daten aus {folder} ...")
    data = load_evm_folder(str(folder), wallet, chain_id)

    raw_rows: List[RawRow] = data["raw"]

    print(f"[DEBUG] Anzahl RawRows: {len(raw_rows)}")
    unknown_swaps = find_unknown_swaps(chain_id, raw_rows)

    if not unknown_swaps:
        print("[DEBUG] Keine Swaps mit UNKNOWN_CONTRACT gefunden. 🎉")
        return

    print(f"[DEBUG] {len(unknown_swaps)} Swaps mit UNKNOWN_CONTRACT gefunden:\n")

    for idx, entry in enumerate(unknown_swaps, start=1):
        print("=" * 80)
        print(f"[{idx}] TX: {entry['tx_hash']}")
        print(f"   token_in : {entry['token_in']}")
        print(f"   token_out: {entry['token_out']}")
        print(f"   contract_in : {entry['contract_in']}")
        print(f"   contract_out: {entry['contract_out']}")
        print(f"   raw_tokens  : {entry['raw_tokens']}")
        print(f"   raw_contracts:")
        for tok, addr in entry["raw_contracts"]:
            print(f"      - {tok:12s} {addr}")
        print(f"   events in tx: {entry['event_count']}")
        print()

if __name__ == "__main__":
    main()
