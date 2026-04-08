# Loader Data Integrity Analysis

Analysis of all taxtrack loader modules for data integrity risks: missing fields, column assumptions, silent skips, and robustness improvements.

---

## 1. Loader risks by integrity concern

### 1.1 Rows with missing or empty tx_hash

| Loader | Risk | Behavior |
|--------|------|----------|
| **etherscan/normal_loader** | Yes | Uses `Transaction Hash` / `Txn Hash` / `Txhash` / `Hash`. If CSV uses different column name (e.g. `TxHash`, `TXID`), `tx_hash` is `""`. Row still appended. |
| **etherscan/erc20_loader** | No | **Skips** row with `if not tx_hash: continue`. Only non-empty tx_hash rows are returned. |
| **etherscan/internal_loader** | Yes | Same column fallbacks as normal; can be `""`. Row still appended. |
| **coinbase/loader** | Yes | `tx_hash = row.get("ID") or ""`. If column missing or empty, row has empty tx_hash. Row still appended. |
| **coinbase/rewards_loader** | Yes | `tx_hash = txid` from first split; if first column empty, tx_hash empty. Row still appended. |
| **coinbase/pdf_loader** | No | Synthetic `tx_hash = f"cbpdf:{page_idx+1}:{row_idx+1}"` always set. |
| **generic/generic_loader** | Yes | `tx_hash = line.get("tx_hash", "") or ""`. If column missing, empty. Row still appended. |

### 1.2 Rows with missing or empty token

| Loader | Risk | Behavior |
|--------|------|----------|
| **etherscan/normal_loader** | Low | Fallback `native_symbol`; token normalized with `.upper()` so only empty if column present and empty and native_symbol somehow empty. |
| **etherscan/erc20_loader** | Low | `map_token(raw_token)`; raw_token can be `""` if TokenSymbol/symbol/Token all missing → token can be mapped to a default but could be empty in edge cases. |
| **etherscan/internal_loader** | No | Always `native_symbol`. |
| **coinbase/loader** | Yes | `token = (row.get("Asset") or "").upper()` → can be `""` if Asset column missing or empty. |
| **coinbase/rewards_loader** | Yes | `token = asset.strip().upper()`; if `parts[4]` (asset) is empty, token is `""`. |
| **coinbase/pdf_loader** | Yes | `asset = str(cells[i_asset]).strip().upper()`; can be `""` if cell empty. |
| **generic/generic_loader** | No | Default `(line.get("token") or "ETH").strip().upper()`. |

### 1.3 Rows with missing or zero timestamp

| Loader | Risk | Behavior |
|--------|------|----------|
| **etherscan/normal_loader** | Yes | If no UnixTimestamp and no DateTime/Date, `ts = 0`, `dt_iso = ""`. Row still appended. |
| **etherscan/erc20_loader** | Yes | Same; `ts = 0` if no date column. Row still appended (unless tx_hash empty). |
| **etherscan/internal_loader** | Yes | Same; `ts = 0`, `dt_iso = ""`. Row still appended. |
| **coinbase/loader** | No | Skips: `if not ts_raw: continue`; parse failure `except: continue`. |
| **coinbase/rewards_loader** | No | Parse failure `except: continue` → row skipped. |
| **coinbase/pdf_loader** | No | Parse failure `except: continue` → row skipped. |
| **generic/generic_loader** | No | `if not ts_raw: continue` → row skipped. |

### 1.4 Rows with missing or invalid direction

| Loader | Risk | Behavior |
|--------|------|----------|
| **etherscan/* ** | No | Always set to `in` / `out` / `internal` / `unknown` from wallet/address logic. |
| **coinbase/loader** | No | `"in"` or `"out"` from action. |
| **coinbase/rewards_loader** | No | Always `"in"`. |
| **coinbase/pdf_loader** | Yes | Uses **`"other"`** for convert/trade/swap and default. Validation `ALLOWED_DIRECTIONS = {"in","out","internal","unknown"}` does **not** include `"other"` → strict validation fails. |
| **generic/generic_loader** | No | Default `(line.get("direction") or "out").strip().lower()`. |

### 1.5 Rows with missing chain_id

| Loader | Risk | Behavior |
|--------|------|----------|
| **etherscan/* ** | No | `chain_id` passed in and set on every RawRow. |
| **coinbase/loader** | Yes | **Never passes chain_id** to RawRow → default `""`. |
| **coinbase/rewards_loader** | Yes | **Never passes chain_id** → `""`. |
| **coinbase/pdf_loader** | Yes | **Never passes chain_id** → `""`. |
| **generic/generic_loader** | Yes | **Does not accept or set chain_id**; `load_auto(path, wallet)` calls it without chain_id → `""`. |

### 1.6 Rows with empty method

| Loader | Risk | Behavior |
|--------|------|----------|
| **etherscan/normal_loader** | Yes | `Method` / `Function` / `FunctionName`; if all missing, `method = ""`. |
| **etherscan/erc20_loader** | No | Always `"ERC20_TRANSFER"`. |
| **etherscan/internal_loader** | Yes | `Type` / `traceType`; if missing, `method = ""`. |
| **coinbase/* ** | No | Set from Transaction Type or table. |
| **generic/generic_loader** | No | Default `line.get("method", "Transfer") or "Transfer"`. |

---

## 2. CSV column missing or renamed

### 2.1 Etherscan-style loaders

- **Column names are hardcoded** with a fixed set of alternatives (e.g. `Transaction Hash`, `Txn Hash`, `Txhash`, `Hash`). If an export uses `TxHash` (capital H) or `TransactionHash` (no space), the field is missed and value is `""` or `0`.
- **No header validation**: Loader does not check that expected columns exist; it only uses `.get()` and fallbacks. No warning or error if the file is from a different schema (e.g. BscScan with different headers).
- **Date columns**: `UnixTimestamp`, `DateTime (UTC)`, `DateTime`, `Date`, `date`. Variants like `Timestamp` or `Time` are not tried.

### 2.2 Coinbase loaders

- **loader.py**: Requires header containing both `"Transaction Type"` and `"Timestamp"`. If Coinbase changes header wording, `header_line` is `None` → **raises** `ValueError("Kein gültiger Coinbase-Header gefunden!")`. Good. Other columns (`ID`, `Asset`, `Quantity Transacted`, etc.) are optional; missing → empty or 0.
- **rewards_loader.py**: Same header requirement; **raises** if not found. Parses first 5 columns by position after `split(",", 5)`; if header order changes, columns misalign.
- **pdf_loader.py**: Table header must contain (after normalization) `timestamp`, `transactiontype`/`type`, `asset`, `quantitytransacted`/`quantity`, `total1`/`total`. If any is missing, **entire table is skipped** (`if None in (i_ts, ...): continue`) with no log.

### 2.3 Generic loader

- Expects lowercase column names: `timestamp`, `tx_hash`, `from`, `to`, `token`, `amount`, `direction`, `method`. No case normalization (e.g. `Timestamp` would be missed). Missing columns → empty string or defaults; no warning.

### 2.4 auto_detect / load_auto

- **Coinbase CSV never selected**: `detect_loader()` only returns `"coinbase_pdf"`, `"evm_internal"`, `"evm_erc20"`, `"evm_normal"`, or `"generic"`. It **never** returns `"coinbase"` or `"coinbase_rewards"`. So Coinbase transaction or rewards CSV files are **always routed to generic loader**, leading to wrong parsing and likely bad or dropped rows.
- **Chain ID**: For generic and Coinbase loaders, `load_auto` does not pass `chain_id` (or the loader does not accept it), so `chain_id` remains `""`.

---

## 3. Silent skip vs raise

| Loader | Silent skip | Raise |
|--------|-------------|--------|
| **normal_loader** | No skips; all rows appended (even ts=0, empty tx_hash). | Only on file read/parse failure. |
| **erc20_loader** | Skips rows with empty `tx_hash` (no log). | — |
| **internal_loader** | No skips. | — |
| **coinbase/loader** | Skips row if no `Timestamp`; skips on timestamp parse failure. No log. | Raises if no valid header. |
| **coinbase/rewards_loader** | Skips if `len(parts) < 5`; skips on timestamp parse failure. No log. | Raises if no header. |
| **coinbase/pdf_loader** | Skips table if required column index is `None`; skips row on timestamp parse failure. No log. | — |
| **generic_loader** | Skips row if `timestamp` missing/empty. No log. | — |

**Summary**: Most loaders **silently drop** rows (no count, no log, no debug report). Only Coinbase loaders **raise** on missing header. No loader reports how many rows were skipped or why.

---

## 4. Recommended validation and robustness improvements

### 4.1 Validation

1. **Use existing strict validation** (`taxtrack.validation.raw_row.validate_raw_row` / `validate_raw_rows`) in the pipeline when `config["validate_raw_rows"]` is True.
2. **Extend allowed directions** to include `"other"` if Coinbase PDF (and any similar source) is in use, or normalize `"other"` to `"unknown"` in loaders before validation.
3. **Optional per-loader validation**: Wrap each loader’s output in a validation step that logs and optionally filters invalid rows instead of failing the whole run (e.g. "warn and skip" mode).

### 4.2 Logging of bad rows

1. **Count and log skipped rows** in every loader: e.g. `logger.warning("[loader_name] Skipped N rows: no timestamp")` with optional detail (first few row indices or line numbers).
2. **Log rows with empty critical fields** (tx_hash, token, timestamp, method) when not skipping: e.g. `[normal_loader] Row N: empty tx_hash (columns: ...)`.
3. **Structured skip reasons**: e.g. `{ "no_timestamp": 3, "empty_tx_hash": 2 }` per file, and pass to a small debug report.

### 4.3 Optional debug report

1. **Per-file summary**: file path, loader used, rows read, rows emitted, rows skipped (with reasons).
2. **Bad-row sample**: first K rows that failed validation or were skipped, with row index and missing/invalid fields.
3. **Column report**: For CSV loaders, log detected vs expected column names so renames or missing columns are visible.

### 4.4 Other improvements

1. **Fix auto_detect**: Add detection for Coinbase CSV (e.g. header contains `"Transaction Type"` and `"Timestamp"` and `"Asset"`) and return `"coinbase"` or `"coinbase_rewards"` so `load_auto` routes correctly.
2. **Pass chain_id to Coinbase and generic loaders**: e.g. `load_coinbase(path, chain_id="coinbase")`, `load_generic(path, wallet, chain_id=...)`, and set on RawRow so pipeline has a consistent chain_id.
3. **Normalize direction in pdf_loader**: Map `"other"` to `"unknown"` (or add `"other"` to validation) so all rows pass strict validation.

---

## 5. Summary table: loader risks

| Risk | normal | erc20 | internal | coinbase | rewards | pdf | generic |
|------|--------|-------|----------|----------|---------|-----|---------|
| Missing tx_hash | ✓ | — (skip) | ✓ | ✓ | ✓ | — | ✓ |
| Missing token | low | low | — | ✓ | ✓ | ✓ | — |
| Missing timestamp | ✓ (ts=0) | ✓ (ts=0) | ✓ (ts=0) | — (skip) | — (skip) | — (skip) | — (skip) |
| Missing direction | — | — | — | — | — | ✓ ("other") | — |
| Missing chain_id | — | — | — | ✓ | ✓ | ✓ | ✓ |
| Empty method | ✓ | — | ✓ | — | — | — | — |
| Column rename/missing | ✓ | ✓ | ✓ | Header raise | Header raise | Table skip | ✓ |
| Silent skips | — | ✓ | — | ✓ | ✓ | ✓ | ✓ |

---

## 6. Code example: loader validation wrapper

Use `taxtrack.validation.loader_wrapper.load_with_validation` to run any loader, validate each row, and optionally log bad rows or skip invalid rows and get a small report.

```python
from pathlib import Path
from taxtrack.loaders.etherscan.normal_loader import load_etherscan
from taxtrack.validation.loader_wrapper import load_with_validation, build_loader_debug_report

# Single loader with validation; raise on first invalid row
rows, report = load_with_validation(
    load_etherscan,
    (Path("data/eth/normal.csv"), "0x123...", "eth"),
    {},
    validate=True,
    log_bad_rows=True,
    require_chain_id=True,
    skip_invalid=False,
)
print(f"Loaded {report['loaded']}, valid {report['valid']}, invalid {report['invalid']}")

# Same loader but skip invalid rows and continue (e.g. for debug report)
rows, report = load_with_validation(
    load_etherscan,
    (Path("data/eth/normal.csv"), "0x123...", "eth"),
    {},
    validate=True,
    log_bad_rows=True,
    skip_invalid=True,
)
if report["invalid_details"]:
    for d in report["invalid_details"]:
        print(f"  Row {d['index']}: missing={d['missing']} invalid={d['invalid']}")

# Coinbase PDF uses direction="other" → allow it when validating
from taxtrack.loaders.coinbase.pdf_loader import load_coinbase_pdf
rows, report = load_with_validation(
    load_coinbase_pdf,
    (Path("report.pdf"),),
    {},
    validate=True,
    allowed_directions={"in", "out", "internal", "unknown", "other"},
    require_chain_id=False,
)
```

Aggregate multiple files into one debug report:

```python
reports = []
labels = []
for path in paths:
    rows, rep = load_with_validation(loader_fn, (path, wallet, chain_id), {}, validate=True, skip_invalid=True)
    reports.append(rep)
    labels.append(str(path))
debug = build_loader_debug_report(reports, source_labels=labels)
# debug["total_loaded"], debug["total_invalid"], debug["by_source"]
```

---

See `taxtrack/validation/raw_row.py` for the validation utility and `taxtrack/validation/loader_wrapper.py` for the loader validation wrapper.
