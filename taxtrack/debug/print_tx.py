from taxtrack.loaders.evm_master_loader import load_evm_folder
from pathlib import Path

TX = "0xd23a6fad0359978c3618aa13f3467f00e4ef3d83e9e5e1ff84a072abb0bf9bca".lower()

def main():
    folder = Path("taxtrack/data/inbox/mm_main/matic")
    wallet = "0x0d3465dafeda5c41b821ad6821c10177ee068706".lower()
    chain_id = "matic"

    data = load_evm_folder(str(folder), wallet, chain_id)
    raws = data["raw"]

    tx_rows = [r for r in raws if r.tx_hash.lower() == TX]

    print(f"Found {len(tx_rows)} rows for TX = {TX}")

    for r in tx_rows:
        print("------")
        print("token:", r.token)
        print("amount:", r.amount)
        print("direction:", r.direction)
        print("from:", r.from_addr)
        print("to:", r.to_addr)
        print("method:", r.method)
        print("category:", r.category)
        print("file:", r.meta.get("source_file"))
        print("-----")

if __name__ == "__main__":
    main()
