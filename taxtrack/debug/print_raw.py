from taxtrack.loaders.evm_master_loader import load_evm_folder
from pathlib import Path

def main():
    folder = Path("taxtrack/data/inbox/mm_main/matic")
    wallet = "mm_main"
    chain_id = "maitc"

    data = load_evm_folder(str(folder), wallet, chain_id)
    raw = data["raw"]

    for i, r in enumerate(raw[:20], start=1):
        print("="*60)
        print(f"{i}. TX={r.tx_hash}")
        print(f"   method={r.method}")
        print(f"   category={r.category}")
        print(f"   token={r.token}")
        print(f"   amount={r.amount}")
        print(f"   direction={r.direction}")
        print(f"   contract={r.contract_addr}")
        print(f"   meta={r.meta}")

if __name__ == "__main__":
    main()
