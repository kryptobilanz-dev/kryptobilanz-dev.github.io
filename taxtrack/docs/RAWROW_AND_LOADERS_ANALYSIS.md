# RawRow Schema and Loader Implementation Analysis

## 1. RawRow dataclass (schemas/RawRow.py)

**Required (no defaults):** `source`, `tx_hash`, `timestamp`, `dt_iso`, `from_addr`, `to_addr`, `token`, `amount`, `direction`, `method`  
**Optional (defaults):** `amount_raw`, `decimals`, `contract_addr`, `fee_token`, `fee_amount`, `category`, `eur_value`, `fee_eur`, `taxable`, `chain_id=""`, `meta`

The dataclass allows empty strings and zero for required fields; Python does not enforce non-empty values.

---

## 2. Loader-by-loader field guarantees

### loaders/etherscan/normal_loader.py

| Field      | Always produced | Notes |
|-----------|------------------|--------|
| tx_hash   | No               | From "Transaction Hash" / "Txn Hash" / "Txhash" / "Hash"; if CSV uses other names, stays `""`. |
| timestamp | No               | Can be `0` when no UnixTimestamp and no parseable DateTime/Date. |
| dt_iso    | No               | `""` when `ts <= 0`. |
| from_addr | Yes*             | From "From"/"from"; can be `""` if column missing. |
| to_addr   | Yes*             | From "To"/"to"; can be `""`. |
| token     | Yes              | Native symbol fallback. |
| amount    | Yes              | Can be 0. |
| direction | Yes              | in/out/internal/unknown. |
| method    | No               | From "Method"/"Function"/"FunctionName"; can be `""`. |
| chain_id  | Yes              | Passed in. |

**Incomplete rows:** Rows with `tx_hash == ""` or `timestamp == 0` are still appended. No skip.

### loaders/etherscan/erc20_loader.py

| Field      | Always produced | Notes |
|-----------|------------------|--------|
| tx_hash   | Yes              | Row skipped if `not tx_hash`. |
| timestamp | No               | Can be `0` if no date column. |
| dt_iso    | No               | `""` when `ts <= 0`. |
| from_addr | Yes*             | From CSV; can be `""`. |
| to_addr   | Yes*             | From CSV; can be `""`. |
| token     | Yes              | map_token(raw_token). |
| amount    | Yes              | |
| direction | Yes              | |
| method    | Yes              | Always `"ERC20_TRANSFER"`. |
| chain_id  | Yes              | Passed in. |

**Incomplete rows:** Only rows with non-empty tx_hash are returned. timestamp/dt_iso can still be 0/"".

### loaders/etherscan/internal_loader.py

| Field      | Always produced | Notes |
|-----------|------------------|--------|
| tx_hash   | No               | From "Transaction Hash"/"Txn Hash"/"Txhash"/"Hash"; can be `""`. |
| timestamp | No               | Can be `0`. |
| dt_iso    | No               | `""` when `ts <= 0`. |
| from_addr | Yes*             | Can be `""`. |
| to_addr   | Yes*             | Can be `""`. |
| token     | Yes              | Native symbol. |
| amount    | Yes              | |
| direction | Yes              | |
| method    | No               | From "Type"/"traceType"; can be `""`. |
| chain_id  | Yes              | Passed in. |

**Incomplete rows:** All rows appended; tx_hash and timestamp can be empty/0.

### loaders/coinbase/loader.py

| Field      | Always produced | Notes |
|-----------|------------------|--------|
| tx_hash   | No               | `row.get("ID") or ""`. |
| timestamp | Yes              | Parsed; row skipped if no Timestamp. |
| dt_iso    | Yes              | iso_from_unix(ts). |
| from_addr | Yes              | `"coinbase"`. |
| to_addr   | Yes              | `"wallet"`. |
| token     | Yes*             | From "Asset"; can be `""` if column empty. |
| amount    | Yes              | |
| direction | Yes              | |
| method    | Yes              | From "Transaction Type". |
| chain_id  | No               | **Not passed to RawRow** → default `""`. |

**Incomplete rows:** Rows without Timestamp are skipped. chain_id is never set.

### loaders/coinbase/rewards_loader.py

| Field      | Always produced | Notes |
|-----------|------------------|--------|
| tx_hash   | No               | From first column (txid); can be empty. |
| timestamp | Yes              | Row skipped if parse fails. |
| dt_iso    | Yes              | |
| from_addr | Yes              | `"coinbase"`. |
| to_addr   | Yes              | `"wallet"`. |
| token     | Yes*             | From "Asset". |
| amount    | Yes              | |
| direction | Yes              | `"in"`. |
| method    | Yes              | `"reward"`. |
| chain_id  | No               | **Not passed** → `""`. |

**Incomplete rows:** Returns **list of dicts** (RawRow(...).to_dict()), not RawRow instances. chain_id missing.

### loaders/coinbase/pdf_loader.py

| Field      | Always produced | Notes |
|-----------|------------------|--------|
| tx_hash   | Yes              | Synthetic `f"cbpdf:{page}:{row}"`. |
| timestamp | Yes              | Row skipped if parse fails. |
| dt_iso    | Yes              | |
| from_addr | Yes              | `"coinbase"`. |
| to_addr   | Yes              | `"wallet"`. |
| token     | Yes*             | From table. |
| amount    | Yes              | |
| direction | Yes              | |
| method    | Yes              | |
| chain_id  | No               | **Not passed** → `""`. |

**Incomplete rows:** Returns **list of dicts** (rr.to_dict()). chain_id missing.

### loaders/generic/generic_loader.py

| Field      | Always produced | Notes |
|-----------|------------------|--------|
| tx_hash   | No               | `line.get("tx_hash", "") or ""`. |
| timestamp | Yes              | Row skipped if no "timestamp". |
| dt_iso    | Yes              | iso_from_unix(unix). |
| from_addr | No               | `(line.get("from") or "").strip()`. |
| to_addr   | No               | `(line.get("to") or "").strip()`. |
| token     | Yes*             | Default "ETH" if missing. |
| amount    | Yes              | |
| direction | Yes              | Default "out" if missing. |
| method    | Yes              | Default "Transfer" if missing. |
| chain_id  | No               | **Not passed** → `""`. load_auto(path, wallet) does not pass chain_id to load_generic. |

**Incomplete rows:** from_addr, to_addr, tx_hash can be empty. chain_id never set.

### auto_detect.py

- `load_auto(path, wallet, chain_id=None)` passes chain_id only to EVM loaders. Coinbase PDF/Rewards/CSV and generic are called without chain_id (or generic gets only path, wallet). So **chain_id is only guaranteed for EVM-loaded rows**.

---

## 3. Optional fields used later in the pipeline

- **chain_id:** Used in pipeline for tx_to_chain, price resolution (logging), and normalization. Pipeline normalizes from meta if missing. **Required for correct price/chain attribution**; currently optional in schema and missing from Coinbase/generic unless pipeline or loader is fixed.
- **fee_token / fee_amount:** Used for fee_eur calculation. Optional; missing is treated as 0.
- **eur_value:** Filled by pipeline for base tokens/rewards; can be 0 if price missing.
- **category:** Raw category from CSV; classification can override. Optional for pipeline (evaluate uses method/direction/counterparty).

---

## 4. Where required fields are not guaranteed

| Required field | Not guaranteed in |
|----------------|--------------------|
| tx_hash        | normal_loader, internal_loader (can be ""); coinbase loader (ID can be ""); generic (column may be missing). |
| timestamp      | normal_loader, internal_loader, erc20_loader (can be 0). |
| dt_iso         | Any loader when timestamp is 0 (dt_iso then ""). |
| token          | All loaders can produce "" if source column empty (e.g. Coinbase Asset). |
| amount         | Always a number; can be 0. |
| direction      | Always set by loaders (with fallbacks). |
| from_addr      | Can be "" in EVM if column missing; generic from/to can be "". |
| to_addr        | Same. |
| method         | normal_loader, internal_loader (can be ""); generic defaults "Transfer". |
| chain_id       | **Coinbase loader, rewards_loader, pdf_loader, generic_loader** never set it. Pipeline later normalizes from meta for EVM; Coinbase/generic have no chain_id in meta. |

---

## 5. Suggested validation (strict)

- **Required (must be present and “filled”):**  
  tx_hash (non-empty), timestamp (int, can be 0 but then dt_iso often useless), dt_iso (str), token (non-empty), amount (float), direction (non-empty, in {"in","out","internal","unknown"}), from_addr (str), to_addr (str), method (str), chain_id (non-empty for pipeline correctness).
- **Validation function:** Accept a row (dict or RawRow); check required keys; check non-empty for tx_hash, token, direction, from_addr, to_addr, method, chain_id; optionally check direction in allowed set. Raise a clear error (e.g. `RawRowValidationError`) with row index and missing/invalid field names.

---

## 6. Where to run validation

- **Option A – Inside each loader:** Before appending a RawRow (or dict), call `validate_raw_row(r)` and skip or raise. Con: every loader must be updated; some loaders return dicts.
- **Option B – After loading, in pipeline:** In `_load_transactions`, after building `filtered_dicts`, run a validation pass over each row (e.g. `validate_raw_row_dict(r, index=i)`). Invalid rows: either raise (strict) or collect errors and raise once with a summary, or skip and log. **Recommended:** single place, works for all loaders and for dicts; pipeline can enforce “no incomplete rows” or “warn and skip”.
- **Option C – In load_auto:** After each loader returns, validate each item and filter or raise. Centralized but load_auto returns mixed RawRow/dict; would need to normalize to dict first.

**Recommendation:** **After loading, in the pipeline** (in `_load_transactions` after building `filtered_dicts`, before returning). Use a strict validation that raises on first invalid row (with row index and field names), or a “collect all errors then raise” mode. Optionally, a **non-strict mode** that logs and skips invalid rows so the run can continue for reporting.

**Implemented:** Validation runs in `run_pipeline()` immediately after `_load_transactions()`, when `config["validate_raw_rows"]` is True. See `taxtrack/validation/raw_row.py` (`validate_raw_row`, `validate_raw_rows`, `RawRowValidationError`). Use `require_chain_id=False` in config if you have Coinbase/generic data without chain_id.

---

## 7. Summary

- **Always produced by all loaders:** direction, amount (numeric).  
- **Often missing or empty:** tx_hash (EVM normal/internal, Coinbase ID, generic); timestamp (0 in EVM); dt_iso ("" when ts<=0); method (EVM normal/internal); from_addr/to_addr (can be "" in EVM/generic); token (can be ""); **chain_id (missing in Coinbase and generic).**
- **Loaders can produce incomplete rows:** Yes. EVM normal/internal do not skip on empty tx_hash; Coinbase/generic do not set chain_id.
- **Validation:** Implement a strict `validate_raw_row` / `validate_raw_row_dict` and run it in the pipeline after load (Option B). Optionally add a fill step for chain_id from path/config for Coinbase/generic before validation.
