# Customer Folder Structure Design

## Goal

Support:

- **Multiple customers**
- **Multiple wallets per customer**
- **Multiple chains per wallet**

With:

- **Automatic chain detection** from folder names
- **One combined PDF report** per customer
- **Configurable** wallet addresses and labels
- **Minimal changes** to existing loaders

---

## 1. Proposed folder structure

```
taxtrack/
  customers/
    <customer_name>/           # e.g. acme_corp, john_doe
      config.json              # customer name, tax_year, wallet list (id → address, label)
      wallets/
        <wallet_id>/           # e.g. wallet_1, main, trading (must match config)
          eth/                 # chain folder → auto-detected
            *.csv
            *.txt
          arb/
            *.csv
          bnb/
            *.csv
        <wallet_id_2>/
          eth/
          op/
      reports/
        tax_report_<year>.pdf   # one combined report per customer
        tax_audit_<year>.csv
```

**Example:**

```
customers/
  acme_corp/
    config.json
    wallets/
      wallet_1/
        eth/
        arb/
        bnb/
      wallet_2/
        eth/
    reports/
      tax_report_2025.pdf
      tax_audit_2025.csv
```

**Rules:**

- **customer_name:** Directory name under `customers/` (e.g. `acme_corp`).
- **wallet_id:** Directory name under `wallets/`. Must match an entry in `config.json` (see below). Examples: `wallet_1`, `main`, `trading`.
- **Chain folders:** Direct subdirectories of each `wallets/<wallet_id>/`. Names are normalized to canonical chain IDs (eth, arb, op, base, bnb, matic, avax, ftm). Only folders that normalize to a known chain are read; others can be skipped or ignored.
- **Transaction files:** Any `.csv` or `.txt` inside a chain folder (Etherscan-style or generic; `load_auto` picks the loader).
- **reports/:** Output directory for the customer’s combined PDF and audit CSV.

---

## 2. Config: wallet addresses and labels

Wallet folder names are **ids** that must be mapped to an **address** (for the engine) and optionally a **label** (for display). Use a single `config.json` per customer.

**Location:** `customers/<customer_name>/config.json`

**Schema:**

```json
{
  "name": "Customer Display Name",
  "tax_year": 2025,
  "wallets": [
    {
      "id": "wallet_1",
      "address": "0x1234...",
      "label": "Main Wallet"
    },
    {
      "id": "wallet_2",
      "address": "0xabcd...",
      "label": "Trading Wallet"
    }
  ]
}
```

- **id (required):** Must match the folder name under `wallets/` (e.g. `wallet_1` → `wallets/wallet_1/`). This is the only binding between filesystem and config.
- **address (required):** The wallet address passed to `load_auto` and `evaluate_batch` (e.g. lowercase).
- **label (optional):** Human-readable name for reports and logs; does not affect loading.

**Backward compatibility:** If you need to support the current format without `id`, you can allow `id` to default to `label` or `address` (so existing configs that use `label` or `address` as the folder name still work).

---

## 3. Automatic chain detection from folder names

Chain detection should happen **in the runner only**, not inside the loaders.

- **Where:** In the customer runner, when iterating subdirectories of `wallets/<wallet_id>/`, treat each subdirectory name as the chain folder name.
- **How:** Use a single **normalize function** that maps folder name → canonical chain ID, e.g.:
  - `eth`, `mainnet` → `eth`
  - `arb`, `arbitrum` → `arb`
  - `op`, `optimism` → `op`
  - `base` → `base`
  - `bnb`, `bsc`, `binance`, `bnbchain` → `bnb`
  - `matic`, `polygon`, `pol` → `matic`
  - `avax`, `avalanche` → `avax`
  - `ftm`, `fantom` → `ftm`
- **Behavior:** If the normalized value is not in the known set, skip that folder (or log a warning). Do not pass unknown values to the engine.
- **Loaders:** They already receive an explicit `chain_id` from the caller. No change: `load_auto(path, wallet, chain_id=chain_id)`. Path-based chain detection in `auto_detect._extract_chain_id_from_path` remains a fallback when `chain_id` is not passed; the runner will always pass `chain_id`, so loader logic stays unchanged.

---

## 4. One combined PDF per customer

- **Input:** All RawRows from all wallets and all chains under that customer, filtered by tax year.
- **Pipeline:** Single run of the existing reference pipeline (classify → gains → economic grouping → vault exits → fee aggregation → rewards) over the combined list, then one call to `build_pdf(...)` and one audit CSV.
- **Output:**  
  - `customers/<customer_name>/reports/tax_report_<year>.pdf`  
  - `customers/<customer_name>/reports/tax_audit_<year>.csv`  
- No code change inside the PDF or gains engine; only the runner must aggregate all wallet/chain data before calling the pipeline once.

---

## 5. What code needs to change

### 5.1 No change (minimal loader impact)

- **load_auto(path, wallet, chain_id)**  
  Signature and behavior stay the same. The runner passes the correct `path`, wallet `address`, and normalized `chain_id` for each file.

- **Etherscan / Coinbase / generic loaders**  
  No changes; they are called via `load_auto` as today.

- **evm_master_loader.load_evm_folder**  
  Optional use only. If the runner prefers to call it for a single `wallets/<wallet_id>/<chain>/` folder, it already infers chain from the path; the only requirement is that the folder path contain a substring that matches the existing logic (e.g. `arb`, `bnb`). No change required for the new structure as long as the runner can pass a path that includes the chain folder name.

- **evaluate_batch, compute_gains, group_gains_economic, apply_vault_exits, group_rewards, build_pdf**  
  No changes; they already operate on lists of dicts / ClassifiedItems and do not depend on folder layout.

### 5.2 Runner: `taxtrack/root/run_customer.py`

**Path layout:**

- **Current:** `customer_dir / "inbox" / (label or address) / chain_folder / files`
- **New:** `customer_dir / "wallets" / wallet_id / chain_folder / files`

**Changes:**

1. **Discovery loop**
   - Use `customer_dir / "wallets"` instead of `customer_dir / "inbox"`.
   - Iterate over subdirectories of `wallets/` as `wallet_id`.
   - For each `wallet_id`, look up the corresponding wallet in `config["wallets"]` by matching `w["id"] == wallet_id` (or, for backward compatibility, `w.get("id") or w.get("label") or w.get("address")`).
   - If no config entry is found, skip that folder or warn.
   - Use the resolved entry’s `address` for `load_auto(..., wallet=address, ...)` and optional `label` for logging.

2. **Config schema**
   - Require or allow an `id` field per wallet that matches the folder name under `wallets/`.
   - Keep `address` and `label` as today. If `id` is missing, you can fall back to treating the folder name as label or address (same as current inbox behavior) so existing configs still work.

3. **Chain detection**
   - Keep using a normalize function (e.g. `_normalize_chain_id(chain_dir.name)`) when iterating subdirectories of `wallets/<wallet_id>/`. Optionally extend the mapping to include `avax`, `ftm` if desired. No change to loaders.

4. **Output**
   - Keep writing to `customer_dir / "reports" / f"tax_report_{year}.pdf"` and `f"tax_audit_{year}.csv"`. No structural change.

5. **Backward compatibility (optional)**
   - If you want to support both the old layout (`inbox/<label_or_address>/<chain>/`) and the new one (`wallets/<wallet_id>/<chain>/`), the runner can check for existence of `wallets/` and, if present, use the new discovery; otherwise fall back to `inbox/` and current config (no `id`, use label/address as folder name).

### 5.3 Optional: shared chain normalization

- **Where:** e.g. `taxtrack/utils/chain.py` or next to `run_customer.py`.
- **What:** One function that maps folder name → canonical chain ID (and optionally returns `None` for unknown names).
- **Used by:** `run_customer.py` and, if desired, `evm_master_loader` (so both use the same mapping). This is a small refactor for consistency, not required for the new structure.

### 5.4 Summary of code changes

| Component              | Change |
|-----------------------|--------|
| Loaders               | **None** (runner passes path, wallet, chain_id). |
| PDF / gains / grouping| **None**. |
| **run_customer.py**   | **Yes:** (1) Use `wallets/` instead of `inbox/`, (2) Resolve wallet by `id` in config, (3) Iterate `wallets/<wallet_id>/<chain>/` and normalize chain name; rest of pipeline unchanged. |
| Config schema         | **Add** `id` per wallet and document that it must match the folder name under `wallets/`. |
| Optional              | Centralize chain normalization in one place and reuse in runner (and optionally in evm_master_loader). |

---

## 6. Example end-to-end

**Config** `customers/acme_corp/config.json`:

```json
{
  "name": "Acme Corp",
  "tax_year": 2025,
  "wallets": [
    { "id": "wallet_1", "address": "0x1111...", "label": "Main" },
    { "id": "wallet_2", "address": "0x2222...", "label": "Trading" }
  ]
}
```

**Filesystem:**

```
customers/acme_corp/
  config.json
  wallets/
    wallet_1/
      eth/
        normal.csv
        erc20.csv
      arb/
        normal.csv
      bnb/
        normal.csv
    wallet_2/
      eth/
        normal.csv
  reports/
    (empty until run)
```

**Runner behavior:**

1. Load config; get `wallets` and `tax_year`.
2. For each directory under `wallets/` (e.g. `wallet_1`, `wallet_2`):
   - Find config entry with `id == "wallet_1"` (or `"wallet_2"`).
   - Get `address` for that entry.
   - For each subdirectory of `wallets/wallet_1/` (e.g. `eth`, `arb`, `bnb`):
     - Normalize to chain ID (`eth`, `arb`, `bnb`).
     - For each `.csv`/`.txt` in that folder: `load_auto(file, wallet=address, chain_id=...)`, append to combined `raw_rows`.
3. Filter `raw_rows` by tax year.
4. Run existing pipeline (evaluate_batch → compute_gains → group_gains_economic → apply_vault_exits → fees → rewards).
5. Write `reports/tax_report_2025.pdf` and `reports/tax_audit_2025.csv`.

**Result:** One combined report per customer, chains detected from folder names, wallet addresses and labels fully configurable, loaders unchanged.
