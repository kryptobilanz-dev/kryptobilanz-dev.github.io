# Taxtrack – Engineering Assessment by Layer

Structured assessment of the current development status of each layer, based on the existing codebase only.

---

## Summary Table

| Layer | Maturity | Missing pieces | Main risks | Key dependencies |
|-------|----------|----------------|------------|------------------|
| 1. Data Loaders | Usable | Coinbase CSV detection, schema validation | Format drift, silent skips | None (entry) |
| 2. RawRow schema | Usable | Validation, optional-field docs | Dict vs RawRow drift | Loaders |
| 3. Classification engine | Usable | Tax-logic coverage for all categories | Unknown/gray categories | RawRow, contract_labeler |
| 4. FIFO gains engine | Usable | Per-wallet lots, edge-case tests | Multi-wallet aggregation | Classification, tax_rules |
| 5. Price engine | Usable | Missing-price policy, API keys | Zero price → wrong PnL | None (used by pipeline) |
| 6. Tax logic | Usable | position_exit, vault_exit, learning_reward, earn_reward, transfer | Under-reporting for missing categories | Classification |
| 7. Reporting | Usable | Schema validation of inputs, API/JSON | KeyErrors on missing fields | Economic gains, classified_dicts |
| 8. Customer folder structure | Usable | Schema validation, multi-run idempotency | Config typo, empty inbox | Pipeline, loaders |

**Recommended development order to stabilize:** 2 → 6 → 1 → 5 → 3 → 4 → 7 → 8.

---

## 1. Data Loaders

**Maturity: Usable**

**What exists:**
- `auto_detect.detect_loader(path)`: recognizes Coinbase PDF, EVM normal/internal/ERC20 by header; fallback generic.
- `load_auto(path, wallet, chain_id)`: routes to Etherscan normal/ERC20/internal, Coinbase PDF, or generic. Chain from path if not passed.
- EVM loaders: return `RawRow` with required fields, `chain_id` passed explicitly (normal, erc20, internal).
- Coinbase: PDF loader, rewards loader, CSV loader (latter gated by `allow_coinbase_csv`); all produce RawRow.
- Generic loader: CSV with timestamp column; builds RawRow with minimal mapping.
- Encoding: generic and EVM use robust text reading (BOM, UTF-8/16, cp1252).

**Missing pieces:**
- Coinbase CSV/Coinbase Rewards are not auto-detected in `detect_loader()`; only PDF is. So "coinbase" and "coinbase_rewards" are never returned; those loaders are only used if called explicitly elsewhere or the detection logic is extended.
- No formal schema validation of loader output (e.g. that every row has non-empty tx_hash, timestamp).
- No loader registry / plugin API for new formats.
- EVM internal detection relies on specific header names; alternate trace export formats may fall through to generic.

**Risk areas:**
- Malformed CSV: EVM loaders use DictReader and field names; typos or missing columns can yield empty/zero fields or skipped rows without a clear error.
- Coinbase PDF: depends on pdfplumber and layout; layout changes can break parsing.
- Generic loader: requires a "timestamp" column; other columns are optional; direction/method may be wrong or empty.

**Dependencies:** None (loaders are the entry point; they depend only on RawRow and paths).

---

## 2. RawRow Schema

**Maturity: Usable**

**What exists:**
- Single dataclass in `schemas/RawRow.py`: required fields (source, tx_hash, timestamp, dt_iso, from_addr, to_addr, token, amount, direction, method); optional (amount_raw, decimals, contract_addr, fee_token, fee_amount, category, eur_value, fee_eur, taxable, chain_id, meta).
- `to_dict()` via asdict for downstream.
- All EVM and Coinbase loaders construct RawRow explicitly; generic loader fills required fields and leaves optionals default.

**Missing pieces:**
- No runtime validation (e.g. non-empty tx_hash, timestamp in range, direction in {"in","out","internal","unknown"}). Pipeline and evaluate assume dicts/objects with these keys.
- No documented contract (which fields are required for which stage). Downstream (evaluate_batch, pipeline) uses get()/getattr with fallbacks, so missing fields degrade gracefully but silently.
- Pipeline sometimes works with dicts (e.g. filtered_dicts) that may have been built from RawRow.to_dict(); no enforcement that new code preserves required keys.

**Risk areas:**
- New loaders or code paths that build dicts without all required RawRow fields can cause wrong direction, wrong timestamp, or KeyError in code that assumes presence.
- chain_id was added later; loaders set it, but any code path that builds rows without chain_id can cause empty chain in price lookups.

**Dependencies:** Loaders must produce RawRow-compatible output. Classification and pipeline consume it.

---

## 3. Classification Engine

**Maturity: Usable**

**What exists:**
- `evaluate_batch(txs, wallet)`: one ClassifiedItem per input row; direction from wallet vs from/to; method from multiple possible keys; raw_cat from tx; base_cat from _basic_category(method, direction); counterparty from contract_labeler; final category from _refine_category(...).
- _basic_category: method keywords → swap, sell, buy, deposit, withdraw, reward, receive/withdraw, unknown.
- _refine_category: self-transfer, raw category, bridge, Pendle, DEX, lending, restake, LP/vault, rewards, then fallbacks (base, transfer).
- Postprocessors: _postprocess_swaps (IN+OUT → gray OUT as swap), _postprocess_lp (method/counterparty → lp_add/lp_remove), _postprocess_pendle (PENDLE-LPT out → pendle_redeem).
- TaxLogic.get_rule(category) and describe(); taxable on ClassifiedItem from rule.
- ClassifiedItem has chain_id, method, category, direction, token, amount, eur_value, fee_*, etc.

**Missing pieces:**
- customer_wallets parameter is not present in current evaluate_batch signature in the repo; self-transfer is only is_self_transfer(wallet, from_addr, to_addr) or direction=="internal". So multi-wallet same-customer internal_transfer detection may be absent unless added in a branch.
- Contract labeler coverage is external (address_map/protocol labels); unknown contracts stay "transfer" or "unknown".
- No explicit handling for margin, futures, options, or lending interest; they can fall to transfer/unknown.
- Complex aggregator/multi-hop swaps may not get IN+OUT in one tx and can remain transfer/withdraw.

**Risk areas:**
- Unknown method strings → unknown category → then tax logic defaults to non-taxable; can under-report if it was actually a disposal.
- Gray-zone overrides (swap, lp_add, lp_remove, pendle_redeem) depend on category priority (can_override); wrong order could overwrite a correct special category.

**Dependencies:** RawRow (or dict with same keys); contract_labeler (label_address); taxlogic for rule lookup. Used by pipeline after load.

---

## 4. FIFO Gains Engine

**Maturity: Usable**

**What exists:**
- `lot_tracker`: Lot dataclass; add_lot(lots, token, amount, cost_eur, timestamp, reinvest); remove_lot(lots, token, amount) returns list of lots consumed (FIFO, with partial lot support).
- `compute_gains(classified_items)`: sort by time; _normalize_liquidity_and_pendle (e.g. pendle_redeem/restake_out eur_value from inflow sum); for each item, skip rewards (§22) and internal_transfer/self_transfer; inflows (buy, receive, deposit, lp_add, pendle_deposit, restake_in, reward/airdrop) → add_lot; outflows (lp_remove, pendle_redeem, restake_out, swap, sell, withdraw) → remove_lot and append GainRow (proceeds, cost_basis, pnl, hold_days, tax_type, taxable).
- tax_rules: calc_holding_days, classify_tax_type, taxable_status (§23 ≤365 days, §22 always).
- group_gains_economic: group by tx_hash, pick main category by ECONOMIC_PRIORITY, aggregate to one event per tx.

**Missing pieces:**
- Lots are global per token (one dict of queues); no per-wallet or per-account separation. When run_customer aggregates multiple wallets, all go through the same lots → cost basis can be attributed across wallets.
- No unit tests for edge cases (zero amount, negative, missing dt_iso, unknown category in loop).
- Open LP positions are collected from remaining lots at end but only for tokens starting with "LP::"; other LP token naming may not be tracked.

**Risk areas:**
- Multi-wallet runs: single lot pool can make cross-wallet cost basis wrong.
- Categories that reach the gains loop but are not explicitly inflow/outflow (e.g. a new category) may fall through to "if cat in (... sell, swap ...) or dirn == 'out'" and be treated as outflow, or be ignored if none match → inconsistent behavior.
- internal_transfer must be skipped; if classification fails to mark it, FIFO will create gains on internal moves.

**Dependencies:** Classification (ClassifiedItem list); tax_rules (holding period, taxable); lp_engine, pendle_engine, restake_engine for protocol-specific events.

---

## 5. Price Engine

**Maturity: Usable**

**What exists:**
- price_provider: PriceQuery; get_price(q): RAM → disk (SQLite) → _fetch_from_source (hybrid then CoinGecko fallback). get_eur_price(symbol, ts, policy, chain) wrapper. TTL by policy (recent/historic_final).
- price_resolver: resolve_prices_batch(queries) deduplicates by key, calls get_price once per unique key, returns list in query order.
- token_mapper.map_token(symbol): canonical symbol for cache/provider (e.g. WETH→ETH).
- provider_master (hybrid): CSV, Yahoo, Binance, Kraken, stablecoin/restaking fallbacks.
- coingecko_price_provider: fallback when hybrid returns 0; optional token_price_mapping.json; in-memory cache.
- Pipeline: _collect_price_queries_from_classified; resolve_prices_batch; _build_price_map; _fill_base_token_eur_value, _fee_eur_on_classified_dicts, _reward_eur_value use price_map.

**Missing pieces:**
- get_price raises ValueError if provider returns no "price" field; resolve_prices_batch does not catch it → one failed query can abort the whole batch. No "missing price" result (e.g. { "price": 0, "missing": true }) or policy (warn/skip/fail).
- No configurable API keys in code path for all providers (env/file); some may be hardcoded or missing.
- Chain is not part of cache key; chain is only for logging. So no L2/CEX-specific price differentiation.
- CoinGecko free tier rate limits; no backoff/retry in coingecko_price_provider (if any) is not centralized with etherscan_fetcher-style retry.

**Risk areas:**
- Missing or zero price → eur_value/fee_eur = 0 → wrong PnL and wrong tax base. Pipeline does not currently enforce a "missing price" list or fail-fast policy in all code paths.
- Rate limit or timeout from provider → exception → batch fails; no per-query try/except in resolve_prices_batch.

**Dependencies:** None (used by pipeline). Pipeline depends on it for eur_value and fee_eur.

---

## 6. Tax Logic

**Maturity: Usable**

**What exists:**
- TaxLogic("de"): loads taxlogic_de.json; get_rule(category) returns dict (taxable, paragraph, type, description) or default {"taxable": False, "type": "Unbekannt", ...}.
- tax_rules: calc_holding_days(ts_buy, ts_sell); classify_tax_type(category); taxable_status(tax_type, hold_days) — §23 ≤365 days taxable, §22 always, else conservative false.
- taxlogic_de.json: swap, sell, withdraw, reward, staking_reward, vault_reward, pendle_*, restake_*, airdrop, lp_add/lp_remove, bridge_*, internal_transfer, deposit, receive, unknown, etc.

**Missing pieces:**
- Categories used in code but not in taxlogic_de.json: position_exit, vault_exit, learning_reward, earn_reward. For these get_rule returns default → type "Unbekannt", taxable false → potential under-reporting of §23/§22.
- "transfer" is returned by _refine_category but has no key in taxlogic_de.json → same default (Unbekannt, non-taxable). Usually correct for true transfers, but if something was misclassified as transfer it stays non-taxable.
- No jurisdiction abstraction (only DE); no parameterization of holding period or rates.

**Risk areas:**
- position_exit / vault_exit: realized gains from vault exits can be reported as "Unbekannt" and taxable=false.
- learning_reward / earn_reward: income can be reported as non-taxable.
- Any new category added in evaluate.py but not in taxlogic_de.json will be defaulted.

**Dependencies:** Classification (produces category). Consumed by gains (taxable_status, classify_tax_type) and by evaluate (reason text).

---

## 7. Reporting

**Maturity: Usable**

**What exists:**
- build_pdf(economic_records, reward_records, summary, debug_info, outpath): ReportLab; sections for cover, executive_summary, transactions, rewards, fees, lp, counterparties, legend. Theme (colors, typography, tables), layout (kpi_boxes).
- _write_audit_csv(economic_gains, gains, output_path, tax_year): CSV with tx_hash, date, category, token, amount, proceeds_eur, cost_basis_eur, pnl_eur, taxable.
- PDF uses get()/as_float/etc. for safe access; economic_records and reward_records are list of dicts.

**Missing pieces:**
- No formal schema or validation of economic_records/reward_records (e.g. required keys). Missing keys can cause KeyError in sections if they assume presence.
- No JSON/API output; no multi-year or multi-currency summary report.
- Audit CSV columns are fixed; no configuration for extra columns or output path pattern.

**Risk areas:**
- New section or change in data shape that assumes a key (e.g. net_pnl_eur, hold_days) can break PDF if pipeline ever omits it for a code path.
- Large reports: no pagination or streaming; full lists in memory.

**Dependencies:** Pipeline output (economic_gains, classified_dicts, totals). Pipeline calls build_pdf and _write_audit_csv when output_dir is set.

---

## 8. Customer Folder Structure

**Maturity: Usable**

**What exists:**
- customers/<name>/config.json with name, tax_year, wallets (list of {address, label}).
- run_customer: _load_customer_config (validates dict, wallets list); discovers inbox by wallet label or address; iterates chain dirs; _normalize_chain_id(name); collects files (csv/txt); builds wallet_data with wallet, chain_id, files; creates reports dir; calls run_pipeline with output_dir, primary_wallet, pdf/audit filenames.
- Inbox layout: customers/<name>/inbox/<wallet_label_or_address>/<chain_folder>/*.csv|*.txt.

**Missing pieces:**
- No JSON schema validation for config.json (e.g. required "name", "wallets", wallet object with "address").
- No idempotency or versioning: re-run overwrites same PDF/CSV; no "run id" or timestamp in filename by default.
- No check that address format is valid (e.g. 0x + 40 hex); typos can lead to empty or wrong wallet_data.
- Chain folder names are fixed in _normalize_chain_id; new chains need code change.

**Risk areas:**
- Config typo (e.g. "wallets" vs "wallet") → ValueError; missing address in a wallet entry → row skipped with WARN; empty wallets list → ValueError.
- Empty inbox for a wallet → wallet_data item not added; pipeline runs with other wallets only (no explicit "no data for wallet X" in report).

**Dependencies:** Pipeline (run_pipeline); loaders (load_auto on discovered files). Folder structure is convention; loaders do not depend on it.

---

## Recommended Development Order to Stabilize

1. **RawRow schema (2)**  
   Add minimal validation (required keys, timestamp/direction allowed values) and a one-page contract doc. Ensures every downstream layer can assume a consistent shape.

2. **Tax logic (6)**  
   Add taxlogic_de.json entries for position_exit, vault_exit, learning_reward, earn_reward, and transfer (if desired). Prevents under-reporting for existing code paths.

3. **Data loaders (1)**  
   Add header validation or row validation after load (e.g. non-empty tx_hash); optionally extend Coinbase CSV/rewards detection in detect_loader so they can be used via load_auto.

4. **Price engine (5)**  
   Define missing-price policy: return { price: 0 } or { missing: true } instead of raising; optionally collect missing in pipeline and report. Add try/except in resolve_prices_batch or in get_price wrapper so one failure does not abort the batch.

5. **Classification engine (3)**  
   Document gray categories and unknown handling; optionally add customer_wallets to evaluate_batch for multi-wallet internal_transfer; extend taxlogic_de.json when adding new categories.

6. **FIFO gains engine (4)**  
   Add tests for edge cases (unknown category, internal_transfer, zero amount); optionally introduce per-wallet or per-account lot keys when stabilizing multi-wallet.

7. **Reporting (7)**  
   Validate economic_records/reward_records keys in build_pdf or in a thin wrapper; document expected shape so sections stay safe.

8. **Customer folder structure (8)**  
   Optional JSON schema for config.json; validate address format; optional run-id or timestamp in output filenames for idempotency and traceability.

This order addresses data shape and tax correctness first (2, 6), then input and price robustness (1, 5), then classification and gains (3, 4), and finally output and orchestration (7, 8).
