# ZenTaxCore – Project Overview & Proposed Folder Structure

## 1. Current folder structure

```
taxtrack/
├── root/                    # Entry points
│   ├── main.py              # zentaxcore (single wallet, single chain, PDF/CSV)
│   ├── main_evm.py          # zentaxcore-evm (single wallet, multi-chain, cache)
│   ├── run_reference.py     # Test runner: data/test_runs/<run>/ → PDF + audit CSV
│   └── run_customer.py     # Customer runner: customers/<name>/ → reports/
├── loaders/                 # Ingestion (Etherscan, Coinbase, generic)
├── rules/                   # Classification (evaluate_batch, tax logic)
├── analyze/                 # Gains, economic events, vault/LP/Pendle/restake
├── pdf/                     # ReportLab PDF (build_pdf, sections)
├── prices/                  # EUR pricing, cache, token mapping
├── utils/                   # Gas, time, contract labeler, wallet, cache
├── data/
│   ├── config/              # chain_config, address_map, taxlogic_de, wallets.json
│   ├── inbox/               # Wallet-first layout (see below)
│   ├── test_runs/           # Reference runs: <run>/<chain>/normal.csv, erc20.csv, internal.csv
│   ├── out/                 # Generated PDFs/CSVs (main, main_evm, test_runs)
│   ├── prices/              # CSV price data
│   └── cache/               # EVM cache (classified, gains, totals)
├── customers/               # Customer layer (config.json, inbox, reports)
├── export/                  # export_summary (CSV summaries)
├── schemas/                 # RawRow
├── debug/, tools/, tests/
```

---

## 2. Where wallet transaction data is expected

| Entry point        | Expected path pattern | Notes |
|--------------------|----------------------|--------|
| **main.py**        | `data/inbox/<wallet_alias>/<chain_id>/*.csv` (or PDF) | Single wallet, single chain; wallet alias from `wallets.json`. |
| **main_evm.py**    | `data/inbox/<wallet_alias>/<chain>/*.csv` | Single wallet; chains from CLI (e.g. `eth,arb,op`). |
| **run_reference.py** | `data/test_runs/<run>/<chain>/normal.csv`, `erc20.csv`, `internal.csv` | One wallet per run (from `wallet.json`/`wallets.json` in run dir). |
| **run_customer.py** | `customers/<customer_name>/inbox/<wallet_label_or_address>/<chain_folder>/*.csv` | Multiple wallets per customer; chains from subfolder names. |

So today there are **two conventions**:

- **Inbox (main / main_evm):** `inbox / wallet / chain / files`
- **Test runs:** `test_runs / run_name / chain / fixed filenames`
- **Customers:** `customers / customer / inbox / wallet / chain / files` (already customer → wallets → chains → files).

---

## 3. How chains are detected

- **By folder name (path):**
  - **evm_master_loader.py:** Infers `chain_id` from the **folder path** string (e.g. `"matic"`/`"polygon"` → `matic`, `"bsc"`/`"binance"`/`"bnbchain"` → `bnb`, else `eth`). Used when loading a single folder via `load_evm_folder`.
  - **auto_detect.py:** `_extract_chain_id_from_path(path)` – looks for one of `eth`, `arb`, `op`, `base`, `bnb`, `matic`, `ftm`, `avax` in path parts; default `eth`. Used when `chain_id` is not passed to `load_auto`.
- **By explicit argument:**
  - **run_reference.py:** Chains from CLI `--chain` (e.g. `eth,arb,op`); each chain is a subfolder under the run directory with fixed CSV names.
  - **main_evm.py:** Chains from CLI `--chain-id` (comma-separated); each chain is a subfolder under `inbox/<wallet_alias>/`.
  - **run_customer.py:** Chains from **subfolder names** under each wallet inbox; normalized via `_normalize_chain_id()` (eth, arb, op, base, matic, bnb, etc.).

Canonical chain IDs used in config/labeling: `eth`, `arb`, `op`, `base`, `bnb`, `matic`, `avax`, `ftm` (see `chain_config.py` and address_map).

---

## 4. Main entry point for running a tax report

- **Production-style reference pipeline (full engine):**  
  **`taxtrack/root/run_reference.py`**  
  - Usage: `python -m taxtrack.root.run_reference --run <run_name> --year <year> [--chain eth,arb,...]`  
  - Reads `data/test_runs/<run>/`, runs load → classify → gains → economic grouping → vault exits → rewards → **PDF** + **audit CSV**.  
  - This is the “golden” path that uses the full pipeline and writes both `*_report.pdf` and `tax_audit_<year>.csv` (in `data/out/test_runs/`).

- **Single-wallet EVM (with cache):**  
  **`taxtrack/root/main_evm.py`**  
  - Usage: `zentaxcore-evm --wallet <alias> --chain-id eth,arb --year 2025`  
  - Reads `data/inbox/<wallet_alias>/<chain>/` via `load_evm_folder`; can use cache; **PDF API currently mismatched** (see earlier analysis).

- **Single-wallet legacy:**  
  **`taxtrack/root/main.py`**  
  - Usage: `zentaxcore --wallet <alias> --chain-id eth --year 2025`  
  - Reads `data/inbox/<wallet_alias>/<chain_id>/`; **PDF call also uses old signature**.

- **Multi-wallet customer:**  
  **`taxtrack/root/run_customer.py`**  
  - Usage: `python -m taxtrack.root.run_customer --customer <customer_name> [--year 2025]`  
  - Reads `taxtrack/customers/<customer_name>/config.json` and `inbox/<wallet>/<chain>/`; writes to `customers/<customer_name>/reports/` (`tax_report_<year>.pdf`, `tax_audit_<year>.csv`).

So the **canonical entry point that runs the full tax report pipeline end-to-end** is **`run_reference.py`** (and, for multiple wallets per customer, **`run_customer.py`**, which reuses the same pipeline logic).

---

## 5. What generates the PDF report

- **Module:** `taxtrack/pdf/pdf_report.py`
- **Function:** `build_pdf(economic_records, reward_records, summary, debug_info, outpath)`
  - **economic_records:** List of dicts (economic events: swap, position_exit, lp_remove, etc.) with e.g. `tx_hash`, `category`, `proceeds_eur`, `cost_basis_eur`, `pnl_eur`, `taxable`, `fees_eur`, `net_pnl_eur`, `hold_days`, `dt_iso`, `token`.
  - **reward_records:** List of dicts (classified transactions used for §22 reward/income section); typically the full classified list or a filtered view.
  - **summary:** Dict (e.g. per-token PnL `totals` from `compute_gains`; can include `open_lp_positions`).
  - **debug_info:** Dict (e.g. `wallet`, `chain`, `year`, `from`, `to`, `customer`) for cover/footer.
  - **outpath:** Path or string for the output PDF file.

The PDF is assembled from sections in `taxtrack/pdf/sections/` (cover, executive_summary, transactions, rewards, lp, fees, counterparties, legend, etc.) and theme/layout under `pdf/theme/`, `pdf/layout/`.

---

## 6. Data structures required to run the pipeline successfully

1. **Input (per file):**  
   - **load_auto(path, wallet, chain_id)** returns a list of **RawRow** (or dicts compatible with RawRow).  
   - **RawRow** (see `taxtrack/schemas/RawRow.py`): `source`, `tx_hash`, `timestamp`, `dt_iso`, `from_addr`, `to_addr`, `token`, `amount`, `direction`, `method`, plus optional `contract_addr`, `fee_token`, `fee_amount`, `category`, `eur_value`, `fee_eur`, `taxable`, `meta`.

2. **After load:**  
   - A single **list of RawRows (or dicts)** from all wallets/chains/files, then:
   - **Year filter:** keep rows with `ts_from <= timestamp < ts_to`.
   - **To dict:** each row as dict (e.g. `row_to_dict(r)`).

3. **Classification:**  
   - **evaluate_batch(txs: List[dict], wallet: str)** → `(List[ClassifiedItem], debug_info)`.  
   - Input dicts must contain at least: `tx_hash`, `timestamp` or `dt_iso`, `from`/`from_addr`, `to`/`to_addr`, `token`, `amount`, `direction`, `method`, `category` (optional), plus optional `eur_value`, `fee_*`, `contract_addr`, `meta`.  
   - Output: **ClassifiedItem** list (with `to_dict()` for downstream).

4. **Gains & economic events:**  
   - **compute_gains(classified: List[ClassifiedItem])** → `(gains: List[GainRow], totals: dict)`.  
   - **group_gains_economic([g.to_dict() for g in gains])** → list of economic event dicts.  
   - **apply_vault_exits(economic_gains, classified_dicts, gains_dicts)** → extended economic event list (adds `position_exit`).  
   - Fee aggregation per `tx_hash` and `net_pnl_eur` on each economic event.

5. **Rewards:**  
   - **group_rewards(classified_dicts)** for reward summary; reward records are the same classified dicts (or filtered by category).

6. **PDF / audit CSV:**  
   - **build_pdf(economic_records, reward_records, summary, debug_info, outpath)** as above.  
   - Audit CSV: one row per economic event with `tx_hash`, `date`, `category`, `token`, `amount`, `proceeds_eur`, `cost_basis_eur`, `pnl_eur`, `taxable`.

So the **minimal contract** for the engine is: **list of dicts (RawRow-like) → evaluate_batch → compute_gains → group_gains_economic → apply_vault_exits → (optional) group_rewards**, then pass the resulting lists and `totals` into `build_pdf` and an audit CSV writer. The engine does **not** depend on a specific folder layout; only the **runners** (main, main_evm, run_reference, run_customer) map folders to that list of dicts.

---

## 7. Proposed improved folder structure (customer → wallets → chains → files)

Goal: one clear convention that supports **customer → wallets → chains → transaction files** with **minimal changes** to the existing engine (no change to loaders, evaluate, gains, vault_exit_resolver, build_pdf, etc.).

### 7.1 Recommended directory layout

Use a **single root** for all “customer” data (including the current “inbox” and “test run” use cases) so that one discovery loop can drive the pipeline:

```
taxtrack/
  data/                          # optional: keep for config, cache, prices, legacy out
  customers/                     # canonical root for “customer” runs
    <customer_id>/
      config.json                # name, tax_year, wallets[{ address, label }]
      inbox/
        <wallet_id>/             # wallet_id = label or address (unique per customer)
          <chain_id>/            # eth | arb | op | base | bnb | matic | avax | ftm
            *.csv
            *.txt
            (optional: normal.csv, erc20.csv, internal.csv for compatibility)
      reports/
        tax_report_<year>.pdf
        tax_audit_<year>.csv
```

- **customer_id:** folder name (e.g. `acme_corp`, `mm_main_multichain_v1`).  
- **wallet_id:** subfolder under `inbox/`; can be wallet label (e.g. `Main Wallet`) or address (e.g. `0x123...`); must be unique per customer.  
- **chain_id:** subfolder under each wallet; **canonical names** recommended: `eth`, `arb`, `op`, `base`, `bnb`, `matic`, `avax`, `ftm` (same set as in `chain_config` and `_normalize_chain_id`).

This is exactly **customer → wallets → chains → transaction files**.

### 7.2 Why this minimizes engine changes

- **Loaders and core logic unchanged:**  
  - `load_auto(file, wallet, chain_id)` already takes a path, wallet address, and chain_id.  
  - The runner only has to: (1) discover `customers/<id>/inbox/<wallet_id>/<chain_id>/` and (2) for each file, pass the correct `wallet` (from config) and `chain_id` (from folder name, normalized). No change to `load_auto`, `evaluate_batch`, `compute_gains`, `group_gains_economic`, `apply_vault_exits`, `build_pdf`, or schema.

- **Single discovery algorithm:**  
  - One loop: for each customer → for each wallet in config → resolve inbox folder by label or address → for each chain subfolder → normalize chain_id → for each `.csv`/`.txt` → `load_auto(path, wallet_address, chain_id)`.  
  - Aggregate all RawRows, then run the existing reference pipeline (filter by year, classify, gains, economic grouping, vault exits, fees, rewards, PDF + audit CSV).  
  - This is what **run_customer.py** already does; the only “improvement” is to treat this layout as the **single** standard and (optionally) migrate `data/inbox` and `data/test_runs` to the same shape under `customers/` for consistency.

- **Backward compatibility:**  
  - **data/inbox:** Can be kept as-is for `main.py` / `main_evm.py` (they continue to use `inbox/<wallet>/<chain>/`).  
  - **data/test_runs:** Either leave as-is and keep `run_reference.py` for test runs only, or add a “synthetic” customer per run (e.g. `customers/<run_name>/` with a one-wallet config and the same chain/filename layout) and run them via `run_customer.py` so one code path handles both.

### 7.3 config.json (unchanged)

```json
{
  "name": "Customer Name",
  "tax_year": 2025,
  "wallets": [
    { "address": "0x123...", "label": "Main Wallet" },
    { "address": "0xabc...", "label": "Trading Wallet" }
  ]
}
```

- **Inbox resolution:** For each wallet, look for `inbox/<label>/` or `inbox/<address>/` (or a normalized address substring). Use that folder’s subfolders as chain folders.

### 7.4 Optional: migrate existing layouts into customers/

To fully adopt the new structure without breaking existing scripts:

- **From data/inbox:**  
  - Create e.g. `customers/mm_main/config.json` with one wallet and `tax_year`.  
  - Move or copy `data/inbox/mm_main/` to `customers/mm_main/inbox/Main Wallet/` (or `inbox/0x.../`) and rename chain folders to canonical `eth`, `arb`, etc.  
  - Run with `run_customer --customer mm_main --year 2025`.

- **From data/test_runs/<run>:**  
  - Create `customers/<run>/config.json` with the run’s wallet and chains.  
  - Copy `<run>/eth/`, `arb/`, etc. to `customers/<run>/inbox/<wallet_id>/eth/`, etc., with the same CSV names.  
  - Run with `run_customer --customer <run> --year 2025` so the same pipeline and folder convention apply.

### 7.5 Summary

- **Current state:** Multiple entry points and two path conventions (inbox vs test_runs); customer layer already uses customer → wallets → chains → files.  
- **Proposed improvement:** Adopt **customers/<customer_id>/inbox/<wallet_id>/<chain_id>/** as the single, canonical layout for “customer” and (optionally) test runs.  
- **Engine:** No changes; only the **runner** (run_customer.py, and optionally run_reference.py) needs to discover paths and call the existing pipeline.  
- **Outputs:** `customers/<customer_id>/reports/tax_report_<year>.pdf` and `tax_audit_<year>.csv`, as already implemented in run_customer.py.
