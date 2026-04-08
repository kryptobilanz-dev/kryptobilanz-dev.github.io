# Wallet Data in Repository & Reference Pipeline

## Wallet data present

| Location | Wallet / Run | Chains | Files | Loader | Runner |
|----------|--------------|--------|-------|--------|--------|
| **data/test_runs/mm_main_multichain_v1/** | 0x0d3465...8706 (from wallets.json) | eth, arb, op, base, avax | normal.csv, erc20.csv, internal.csv per chain | load_auto → evm_normal, evm_erc20, evm_internal (Etherscan) | **run_reference.py** |
| data/test_runs/eth_mm_main_v1/ | same wallet | (only wallets.json; no chain folders) | — | — | run_reference (incomplete run) |
| data/test_runs/arb_mm_main_v1/ | same wallet | (only wallets.json; no chain folders) | — | — | run_reference (incomplete run) |
| data/inbox/mm_main/ | mm_main → 0x0d3465...8706 (wallets.json) | eth, arb, matic, base, op, avax | *_2024.csv, *_2025.csv | load_auto (Etherscan) | main_evm.py or run_customer (if migrated) |
| data/inbox/tw_yield/ | tw_defi → 0xec29... (wallets.json) | eth, arb | *_2024.csv, *_2025.csv | load_auto (Etherscan) | main_evm.py |

## Best candidate for first PDF + audit CSV

- **Run:** `mm_main_multichain_v1`
- **Wallet:** `0x0d3465dafeda5c41b821ad6821c10177ee068706` (from test_runs/mm_main_multichain_v1/wallets.json)
- **Chains:** eth, arb, op, base, avax (folder names = chain IDs; load_auto gets chain_id from runner)
- **Loader:** `load_auto()` in run_reference → Etherscan normal/erc20/internal loaders per file
- **Runner:** `taxtrack.root.run_reference` (full pipeline: classify → gains → economic grouping → vault exits → fees → rewards → PDF + audit CSV)
- **Year:** 2025 (CSV timestamps are 2025)

## Config check

- **mm_main_multichain_v1/wallets.json** has `wallet` (no `wallet_address`). run_reference uses `data.get("wallet_address") or data.get("wallet")` → OK. No change required.

## Fix applied

- **Unicode in run_reference.py:** The POSITION AUDIT debug block used the character `↳` (U+21B3), which caused `UnicodeEncodeError` on Windows (cp1252). Replaced with spaces so the script runs on Windows without changing behavior.

## Command to generate PDF and audit CSV

From repo root:

```bash
python -m taxtrack.root.run_reference --run mm_main_multichain_v1 --year 2025 --chain eth,arb,op,base,avax
```

Outputs (under taxtrack/data/out/test_runs/):

- `mm_main_multichain_v1_eth,arb,op,base,avax_2025_report.pdf`
- `tax_audit_2025.csv`
