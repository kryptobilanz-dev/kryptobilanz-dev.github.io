# run_pipeline() – Analysis and Improvement Suggestions

## chain_id flow (loader → classification → price resolution)

**Where chain_id is set and preserved:**

| Stage | Where | How |
|-------|--------|-----|
| **Load** | EVM loaders (normal, erc20, internal) | `RawRow(..., chain_id=chain_id)` + kept in `meta` for backward compatibility. |
| **Load** | Generic / Coinbase | `RawRow` default `chain_id=""`; pipeline later normalizes from `meta` if present. |
| **Normalize** | Pipeline after `_load_transactions` | Every row dict gets top-level `chain_id`: from existing key or `meta["chain_id"]`. |
| **Classify** | `evaluate_batch` | Reads `tx.get("chain_id")` or `(tx.get("meta") or {}).get("chain_id")`, sets `ClassifiedItem(..., chain_id=chain_id)`. |
| **to_dict** | After classification | `ClassifiedItem.to_dict()` (via `asdict`) includes `chain_id`. |
| **Pipeline fallback** | After building `classified_dicts` | `d.setdefault("chain_id", tx_to_chain.get(...))` so dicts always have `chain_id` for fee/reward price calls. |
| **Price resolution** | `_fee_eur_on_dicts`, `_fill_base_token_eur_value`, `_fee_eur_on_classified_dicts`, `_reward_eur_value` | All use `tx.get("chain_id")` or `getattr(it, "chain_id", None)` and pass `chain=...` into `get_eur_price()`. |

**Previously:** EVM loaders only stored chain_id in `meta`; `RawRow` and `ClassifiedItem` had no `chain_id` field; pipeline built `tx_to_chain` from `r.get("chain_id")` which was always missing, so price requests got `chain=""`.

**Now:** `chain_id` is a top-level field on `RawRow` and `ClassifiedItem`, set at load and in `evaluate_batch`, and normalized on dicts in the pipeline so it is available for every price request.

---

## 1. Execution flow diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ INPUTS                                                                           │
│   wallet_data: List[{wallet, chain_id, base_dir? | files?}]                     │
│   tax_year: int                                                                  │
│   config: { output_dir?, primary_wallet?, debug_info?, pdf_filename?, ... }     │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 1. LOAD TRANSACTIONS (_load_transactions)                                        │
│    • For each item: load_auto(path, wallet, chain_id) → raw_rows                 │
│    • Year filter: ts_from ≤ timestamp < ts_to                                    │
│    • _row_to_dict() → filtered_dicts                                             │
│    Out: raw_rows, filtered_dicts                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                          ┌─────────────┴─────────────┐
                          │ filtered_dicts empty?     │──Yes──► EARLY RETURN
                          └─────────────┬─────────────┘         (empty result dict)
                                        │ No
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 2. NORMALIZE (implicit)                                                          │
│    • tx_to_chain = {tx_hash → chain_id} from filtered_dicts                      │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 3. CLASSIFY                                                                      │
│    • _fee_eur_on_dicts(filtered_dicts)     ← MUTATES filtered_dicts              │
│    • evaluate_batch(filtered_dicts, primary_wallet) → classified, debug_info     │
│    Out: classified (List[ClassifiedItem])                                        │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 4–5. RESOLVE PRICES + COMPUTE EUR_VALUE                                          │
│    • _fill_base_token_eur_value(classified)        ← MUTATES classified          │
│    • classified_dicts = [c.to_dict() for c in classified]                        │
│    • Inject chain_id into classified_dicts                                        │
│    • _fee_eur_on_classified_dicts(classified_dicts) ← MUTATES classified_dicts   │
│    • _lp_vault_mint_eur_value(classified)          ← MUTATES classified          │
│    • classified_dicts = [c.to_dict() for c in classified]  (rebuild)             │
│    • Inject chain_id again                                                        │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 6. FIFO GAIN CALCULATION                                                         │
│    • compute_gains(classified) → gains, totals                                  │
│    Out: gains (List), totals (dict)                                              │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 7. ECONOMIC GROUPING                                                             │
│    • group_gains_economic([g.to_dict() for g in gains]) → economic_gains         │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 8. RESOLVE VAULT EXITS                                                           │
│    • economic_gains = apply_vault_exits(economic_gains, classified_dicts, gains)  │
│    • _cleanup_vault_exit_per_tx(economic_gains)   ← MUTATES economic_gains       │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 9. APPLY TAX LOGIC                                                               │
│    • _apply_fees_net_pnl(classified_dicts, economic_gains)  ← MUTATES both       │
│    • _reward_eur_value(classified_dicts)                     ← MUTATES            │
│    • _usd_fallback_eur_value(classified_dicts)               ← MUTATES            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 10. COMPUTE REWARDS                                                              │
│     • group_rewards(classified_dicts) → reward_summary                            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 11. GENERATE REPORT (if output_dir)                                              │
│     • _write_audit_csv(economic_gains, gains, audit_file, tax_year)               │
│     • build_pdf(economic_gains, classified_dicts, totals, debug_info, pdf_file)  │
│     • Side effect: files written to disk                                         │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ OUTPUTS (returned dict)                                                          │
│   economic_gains, classified_dicts, gains, totals, reward_summary, debug_info   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Inputs

| Input | Type | Description |
|-------|------|-------------|
| `wallet_data` | `List[WalletDataItem]` | Per item: `wallet` (address), `chain_id`, and either `base_dir` (Path) or `files` (List[str]). No schema validation. |
| `tax_year` | `int` | Calendar year for filter and report. No range or validity check. |
| `config` | `Optional[PipelineConfig]` | Optional dict: `output_dir`, `report_label`, `primary_wallet`, `debug`, `debug_info`, `pdf_filename`, `audit_filename`. |

---

## 3. Outputs

| Output | Type | Description |
|--------|------|-------------|
| `economic_gains` | `List[Dict]` | §23 economic events (swap, vault_exit, etc.) with pnl_eur, fees_eur, net_pnl_eur. |
| `classified_dicts` | `List[Dict]` | Classified rows (in-place mutated with fee_eur, eur_value, chain_id). |
| `gains` | `List` | FIFO gain objects. |
| `totals` | `Dict` | Summary from compute_gains. |
| `reward_summary` | `Dict` | From group_rewards. |
| `debug_info` | `Dict` | Merged debug info for PDF. |
| Side effects | — | Audit CSV and PDF files if `output_dir` is set. |

---

## 4. State mutations

| Location | Mutated object | What changes |
|----------|----------------|---------------|
| `_fee_eur_on_dicts` | `filtered_dicts` | Adds/overwrites `fee_eur` on each row. |
| `_fill_base_token_eur_value` | `classified` (ClassifiedItem) | Sets `it.eur_value`. |
| `classified_dicts` (first build) | New list | Filled from `classified`; then `chain_id` added in place. |
| `_fee_eur_on_classified_dicts` | `classified_dicts` | Sets `fee_eur` on each row. |
| `_lp_vault_mint_eur_value` | `classified` | Sets `r.eur_value` on vault/LP IN items. |
| `classified_dicts` (rebuild) | New list | Rebuilt from `classified`; `chain_id` re-injected. |
| `_cleanup_vault_exit_per_tx` | `economic_gains` | `clear()` then `extend(cleaned)`. |
| `_apply_fees_net_pnl` | `economic_gains`, (read-only classified_dicts) | Adds `fees_eur`, `net_pnl_eur` to each economic gain. |
| `_reward_eur_value` | `classified_dicts` | Sets `eur_value` on reward rows. |
| `_usd_fallback_eur_value` | `classified_dicts` | Sets `eur_value` from USD when missing. |
| Step 11 | Filesystem | Writes audit CSV and PDF. |

Important: `filtered_dicts` and `classified` are mutated then later used again; `classified_dicts` is rebuilt from `classified` twice, so ordering and identity of objects change across steps.

---

## 5. Potential performance bottlenecks

| Bottleneck | Cause | Impact |
|------------|--------|--------|
| **Repeated price lookups** | `get_eur_price()` called per row in `_fee_eur_on_dicts`, `_fill_base_token_eur_value`, `_fee_eur_on_classified_dicts`, `_reward_eur_value`. No batching. | Many I/O or API calls; cache helps but keys are still computed per call. |
| **Duplicate fee pass** | Fee EUR computed on `filtered_dicts` then again on `classified_dicts` (same logical data). | Double price resolution for fees. |
| **Two builds of classified_dicts** | `classified_dicts` built from `classified` twice (after base eur_value fill and after LP/vault). | Extra list/dict allocation and iteration. |
| **No price deduplication** | Same (symbol, ts, chain) can be requested many times across rows. | Redundant work even with cache. |
| **Sequential file load** | `_load_transactions` loads files one by one. | Minor; could parallelize per (wallet, chain) if needed. |
| **PDF generation** | `build_pdf()` is heavy (ReportLab, many sections). | Dominant cost for large reports; no streaming or chunking. |
| **Large in-memory structures** | All rows and gains kept in memory. | For very large wallets, memory can grow with no cap. |

---

## 6. Missing validation steps

| Missing validation | Where | Risk |
|--------------------|--------|------|
| **wallet_data schema** | Entry | Wrong/missing `wallet` or `chain_id`; neither `base_dir` nor `files`; invalid paths. |
| **tax_year range** | Entry | e.g. 0 or 2100; no check. |
| **primary_wallet format** | After defaulting | Not validated as address-like. |
| **Empty wallet / chain_id** | `_load_transactions` | `wallet` or `chain_id` empty still used in load_auto. |
| **Row schema after load** | After load | No check for required keys (tx_hash, timestamp, token, amount, direction, etc.). |
| **ClassifiedItem invariants** | After evaluate_batch | No check that eur_value ≥ 0 or amounts are finite. |
| **Gains invariants** | After compute_gains | No sanity check (e.g. cost_basis + pnl ≈ proceeds). |
| **output_dir writable** | Step 11 | No check before writing; can fail late. |
| **config key types** | Entry | e.g. output_dir could be str; Path preferred. |

---

## 7. Improvement suggestions

### Performance

1. **Batch price resolution**  
   Collect all (symbol, ts, chain) for fees and eur_value (base tokens, rewards), deduplicate, call `resolve_prices_batch(queries)` once, then assign prices when building fee_eur and eur_value. Reduces API/cache lookups and duplicate work.

2. **Single fee pass**  
   Compute fee_eur only once, after classification, on `classified_dicts` (using dt_iso or timestamp). Drop `_fee_eur_on_dicts` so fee prices are not resolved twice.

3. **Build classified_dicts once**  
   After all in-place updates to `classified` (base eur_value, LP/vault eur_value), build `classified_dicts` once and inject `chain_id`. Remove the second rebuild and second chain_id pass.

4. **Optional lazy/streaming report**  
   For very large runs, allow writing audit CSV incrementally and/or building PDF in chunks to limit peak memory.

5. **Parallel load**  
   In `_load_transactions`, load different (wallet, chain_id, path) in parallel (e.g. ThreadPoolExecutor) when many files exist. Keep order deterministic when merging.

### Reliability

1. **Validate inputs at entry**  
   - `wallet_data`: non-empty; each item has `wallet` (non-empty), `chain_id`, and exactly one of `base_dir` or `files`; paths exist when using `base_dir`/`files`.  
   - `tax_year`: e.g. 2000 ≤ tax_year ≤ 2100.  
   - `config`: if `output_dir` is present, resolve to Path and optionally check writable.

2. **Structured errors**  
   Raise custom exceptions (e.g. `PipelineConfigError`, `PipelineLoadError`) with clear messages and context (path, wallet, chain, step) instead of generic ValueError or bare exceptions from helpers.

3. **No silent swallow**  
   In `_fill_base_token_eur_value`, `_reward_eur_value`, `_usd_fallback_eur_value`, log or collect parse/price failures (e.g. symbol, ts, row id) and optionally attach to result or re-raise, instead of bare `except Exception: pass`.

4. **Idempotent report write**  
   Create report in a temp path, then atomic rename to final audit CSV / PDF so partial writes are not left on disk on failure.

5. **Read-only intermediate data**  
   Where possible, avoid mutating shared structures (e.g. return new lists from “cleanup” steps instead of clear/extend) so steps are easier to reason about and replay.

### Testability

1. **Inject dependencies**  
   Pass loader, price resolver, and PDF builder as optional arguments (or a small “pipeline context” object) so tests can inject mocks (e.g. no I/O, fixed prices, no PDF write).

2. **Pure steps where possible**  
   Split “business” steps (e.g. fee aggregation, vault cleanup, tax logic) into pure functions: input → output, no I/O. Test them with fixed inputs and assert on outputs.

3. **Stable result contract**  
   Document and assert the shape of `PipelineResult` (e.g. TypedDict or dataclass) and key invariants (e.g. length of economic_gains vs gains, presence of required keys). Add a small validation function for the returned dict.

4. **Fixture-friendly wallet_data**  
   Support passing pre-built row dicts (e.g. `wallet_data = [{ "wallet": "0x...", "chain_id": "eth", "rows": [...] }]`) so tests can skip file load and run from in-memory data only.

5. **Step-level entry points**  
   Expose each logical step (e.g. `load_and_filter`, `classify`, `compute_eur_values`, `fifo_gains`, `economic_grouping`, `vault_exits`, `tax_logic`, `rewards`, `write_report`) as callable functions with explicit inputs/outputs so they can be unit-tested and composed in tests without running the full pipeline.

6. **Deterministic ordering**  
   Sort `raw_rows` (e.g. by (timestamp, tx_hash)) after load so pipeline output order is deterministic and tests can use snapshot or golden-file comparisons.

---

## 8. Summary

- **Diagram:** Single linear flow from inputs through 11 steps to outputs and optional file write; one early exit when there are no filtered rows.
- **Inputs:** `wallet_data`, `tax_year`, `config`; no formal validation.
- **Outputs:** Dict with economic_gains, classified_dicts, gains, totals, reward_summary, debug_info; plus optional CSV and PDF.
- **State mutations:** Multiple in-place updates to filtered_dicts, classified, classified_dicts, economic_gains; two rebuilds of classified_dicts.
- **Bottlenecks:** Per-row price lookups (no batching), duplicate fee pass, double classified_dicts build, PDF generation, and full in-memory data.
- **Missing validation:** wallet_data schema, tax_year, primary_wallet, row schema, invariants after classify/gains, output_dir writability.
- **Improvements:** Batch prices, single fee pass, single classified_dicts build, input and result validation, structured errors, less silent failure, dependency injection, pure steps, and step-level test entry points.
