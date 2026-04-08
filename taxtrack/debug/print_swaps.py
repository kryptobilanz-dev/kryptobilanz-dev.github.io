# taxtrack/debug/print_swaps.py
from taxtrack.loaders.evm_master_loader import load_evm_folder
from taxtrack.analyze.swap_engine import extract_swaps
from pathlib import Path

def main():
    folder = Path("taxtrack/data/inbox/mm_main/eth")  # << HIER WICHTIG!
    wallet = "0x0d3465dafeda5c41b821ad6821c10177ee068706".lower()
    chain_id = "eth"  # << HIER WICHTIG!

    data = load_evm_folder(str(folder), wallet, chain_id)
    raw_rows = data["raw"]
    swaps = extract_swaps(chain_id, raw_rows)

    print(f"[DEBUG] Total Swaps found: {len(swaps)}\n")

    for i, se in enumerate(swaps[:10], start=1):
        print("="*80)
        print(f"[{i}] TX: {se.tx_hash}")
        print(f"   token_in : {se.token_in}")
        print(f"   token_out: {se.token_out}")
        print(f"   amount_in  : {se.amount_in}")
        print(f"   amount_out : {se.amount_out}")
        print(f"   legs      :")
        for leg in (se.legs or []):
            print(f"      - {leg.token:12s} {leg.direction:3s} {leg.amount:<20} {leg.contract_addr}")

if __name__ == "__main__":
    main()
