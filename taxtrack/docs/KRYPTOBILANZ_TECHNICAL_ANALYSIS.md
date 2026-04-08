# Kryptobilanz – Technical Analysis

A structured technical description of the current system for developers new to the codebase. The project is implemented under the package name **taxtrack** (ZenTaxCore).

---

## 1. Project Purpose

### Main problem

The program is a **crypto tax engine** that:

- Ingests blockchain and exchange transaction data for one or more wallets.
- Classifies each transaction (swap, transfer, reward, LP, vault, etc.).
- Resolves EUR prices and computes euro values for amounts and fees.
- Applies **FIFO (first-in, first-out)** lot accounting to compute **realized gains** (proceeds, cost basis, PnL).
- Applies **tax rules** (currently German: §23 EStG private disposal, §22 EStG other income) to determine taxable status and holding periods.
- Produces **tax reports** (PDF) and an **audit CSV** for the chosen tax year.

### Input

- **Wallet addresses** – Used when downloading data or when identifying the “primary” wallet for classification (in/out direction, self-transfers).
- **CSV files** – Main input:
  - **EVM / Etherscan-style:** `normal.csv`, `erc20.csv`, `internal.csv` per chain (columns: Txhash, DateTime, From, To, Token, Value, etc.).
  - **Coinbase:** CSV exports or PDF transaction history.
- **Blockchain data** – Fetched via **Etherscan-compatible APIs** (Ethereum, Arbitrum, Base, Optimism, Avalanche, etc.) and written to CSV; the pipeline then reads those CSVs.
- **Folder layout** – Data is organized by run/customer, wallet, and chain (e.g. `data/test_runs/<run>/<chain>/*.csv`, `customers/<name>/inbox/<wallet>/<chain>/*.csv`).

### Output

- **PDF tax report** – `tax_report_<year>.pdf`: cover, executive summary, transaction table (economic events with proceeds, cost basis, PnL, taxable, fees), rewards section, fees, LP, counterparties, legend.
- **Audit CSV** – `tax_audit_<year>.csv`: one row per economic event with `tx_hash`, `date`, `category`, `token`, `amount`, `proceeds_eur`, `cost_basis_eur`, `pnl_eur`, `taxable`.
- **Pipeline result dict** – In-memory: `economic_gains`, `classified_dicts`, `gains`, `totals`, `reward_summary`, `debug_info` (and optionally `missing_prices`, `unknown_generic_classifications`, `reward_classifications` where implemented).

---

## 2. High-Level Architecture

The system is built around a **single pipeline** executed by one function: **`run_pipeline(wallet_data, tax_year, config)`** in `taxtrack/root/pipeline.py`. All runners (run_reference, run_wallet, run_customer) call this function; there is no duplicated core logic.

### End-to-end flow (verified against code)

| Stage | What happens |
|-------|----------------|
| **1. Load** | For each `wallet_data` item: `load_auto(path, wallet, chain_id)` or load from `base_dir` (normal.csv, erc20.csv, internal.csv). Output: list of `RawRow` (or dicts). Year filter: `ts_from <= timestamp < ts_to`. Rows normalized to dicts with top-level `chain_id`. |
| **2. Normalize** | Dicts already have `chain_id` from load; `tx_to_chain` map built for downstream. |
| **3. Classify** | `evaluate_batch(filtered_dicts, primary_wallet, customer_wallets)` → list of `ClassifiedItem` (category, direction, method, token, amount, eur_value, fee_*, chain_id, etc.). |
| **4. Price resolution** | Unique `PriceQuery(symbol, ts, chain)` collected from classified (base tokens, fees, rewards). `resolve_prices_batch(queries)` → one price per unique key; results mapped to `(normalized_symbol, ts)` → price. |
| **5. EUR value** | Base-token and reward `eur_value` filled from price map; `fee_eur` set per row. LP/vault mint: funding OUT (base assets) → proportional `eur_value` for vault/LP token IN. |
| **6. FIFO gains** | `compute_gains(classified)` → chronological processing, `add_lot` / `remove_lot` per token, output `GainRow` list and `totals`. |
| **7. Economic grouping** | `group_gains_economic(gains)` → one economic event per tx (priority: position_exit, vault_exit, lp_remove, pendle_redeem, restake_out, swap, sell). |
| **8. Vault exits** | `apply_vault_exits(economic_gains, classified_dicts, gains)` → vault-ledger FIFO for position tokens; adds `position_exit` events with cost_basis_eur, pnl_eur, hold_days. §23 cleanup: per tx, if any vault_exit exists, keep only vault_exit rows. |
| **9. Tax logic** | `_apply_fees_net_pnl`: aggregate `fee_eur` per tx, set `fees_eur` and `net_pnl_eur` on economic gains. `_reward_eur_value`: fill missing reward eur_value from price map. `_usd_fallback_eur_value`: use USD value if present. |
| **10. Rewards** | `group_rewards(classified_dicts)` → summary by token for reward categories. |
| **11. Report** | `_write_audit_csv`; `build_pdf(economic_records, reward_records, summary, debug_info, outpath)`. |

Optional **download** step (run_wallet): before the pipeline, transaction data can be fetched via Etherscan-compatible APIs and written to `data/inbox/<wallet>/<chain>/` CSVs.

---

## 3. Core Modules

| Module | Responsibility |
|--------|----------------|
| **loaders/** | **auto_detect**: detect format (EVM normal/ERC20/internal, Coinbase PDF/CSV, generic). **EVM loaders**: parse Etherscan-style CSVs into `RawRow` with `chain_id`. **Coinbase**: CSV/PDF/rewards. Output: list of RawRow or dict with same fields. |
| **rules/evaluate** | **evaluate_batch(txs, wallet, customer_wallets)**: one pass over transactions → `ClassifiedItem` per row. Uses method, direction, counterparty (contract_labeler), raw category; _refine_category for DeFi (LP, Pendle, restake, bridge, self-transfer). Postprocessors: _postprocess_swaps, _postprocess_lp, _postprocess_pendle. **Self-transfer**: single-wallet or both sender/receiver in `customer_wallets` → `internal_transfer`. |
| **rules/taxlogic** | Loads `taxlogic_de.json`; **get_rule(category)** → taxable, paragraph, type, description. Used by evaluate (reason text) and by **analyze/tax_rules**: **classify_tax_type**, **taxable_status** (§23 hold_days ≤ 365, §22 always taxable). |
| **prices/** | **price_provider**: PriceQuery, get_price (RAM → disk → hybrid → CoinGecko fallback), get_eur_price. **price_resolver**: resolve_prices_batch (dedup by key, one get_price per unique query). **token_mapper**: map_token (canonical symbol for cache/provider). **provider_master**, **coingecko_price_provider**, CSV/Yahoo/Binance/Kraken. |
| **analyze/gains** | **compute_gains(classified_items)**: sort by time; per-item inflow (add_lot) or outflow (remove_lot); **internal_transfer**/self_transfer skipped; rewards/lp_add/pendle_deposit/restake_in → inflow; lp_remove/pendle_redeem/restake_out/swap/sell/withdraw → outflow. **GainRow**: dt_iso, token, amount_out, proceeds_eur, cost_basis_eur, pnl_eur, method, tx_hash, buy_date_iso, hold_days, tax_type, taxable, is_reinvest. **lot_tracker**: add_lot, remove_lot (FIFO queue per token). |
| **analyze/gain_grouping** | **group_gains_economic(gains)**: group by tx_hash; pick main category by ECONOMIC_PRIORITY; aggregate proceeds, cost, pnl, fees, net_pnl, taxable, hold_days; one dict per economic event. |
| **analyze/vault_exit_resolver** | **apply_vault_exits**: build position ledger from classified (in, vault token, eur_value > 0); for txs with OUT vault token + IN base asset, consume ledger FIFO → cost_basis_eur, pnl_eur, hold_days; append position_exit events. |
| **analyze/lp_engine, pendle_engine, restake_engine, swap_engine** | Protocol-specific: LP add/remove, Pendle deposit/redeem, restake in/out, swap detection (method/events). Used inside evaluate (postprocessors) or gains (process_lp_add, process_lp_remove, etc.). |
| **analyze/reward_grouping** | **group_rewards(classified_dicts)**: filter by REWARD_CATEGORIES; aggregate by token for summary. |
| **analyze/tax_rules** | **calc_holding_days**, **classify_tax_type**, **taxable_status** (German rules). |
| **pdf/** | **build_pdf**: ReportLab; sections (cover, executive_summary, transactions, rewards, fees, lp, counterparties, legend); theme/layout. |
| **root/pipeline** | **run_pipeline**: single entry; implements all 11 steps; returns economic_gains, classified_dicts, gains, totals, reward_summary, debug_info (and optionally missing_prices, unknown_generic_classifications, reward_classifications). Helpers: _load_transactions, _collect_price_queries_from_classified, _build_price_map, _fill_base_token_eur_value, _fee_eur_on_classified_dicts, _lp_vault_mint_eur_value, _reward_eur_value, _cleanup_vault_exit_per_tx, _apply_fees_net_pnl, _write_audit_csv. |
| **root/run_reference** | Load wallet_data from `data/test_runs/<run>/<chain>/`; call run_pipeline; output to `data/out/test_runs/` (PDF + audit CSV). |
| **root/run_wallet** | Optional download via etherscan_fetcher; build wallet_data from `data/inbox/<wallet>/<chain>/`; run_pipeline; output to `data/out/reports/<wallet>/`. |
| **root/run_customer** | Load config from `customers/<name>/config.json`; discover inbox/<wallet>/<chain>; build wallet_data; run_pipeline; output to `customers/<name>/reports/`. |
| **download/etherscan_fetcher** | Fetch normal, ERC20, internal transactions from Etherscan-compatible APIs; retry/backoff; write CSVs to a target directory. |

**Interaction:** Runners build `wallet_data` and `config`, then call `run_pipeline`. Pipeline calls loaders → evaluate_batch → price batch + eur_value/fee_eur → compute_gains → group_gains_economic → apply_vault_exits → tax/reward steps → _write_audit_csv + build_pdf.

---

## 4. Data Model

| Structure | Role |
|-----------|------|
| **RawRow** | Loader output. Fields: source, tx_hash, timestamp, dt_iso, from_addr, to_addr, token, amount, direction, method; optional fee_token, fee_amount, category, eur_value, fee_eur, chain_id, meta. |
| **filtered_dicts** | RawRow converted to dict (to_dict or dict), year-filtered; top-level chain_id ensured. Input to evaluate_batch. |
| **ClassifiedItem** | One per row after classification. tx_hash, dt_iso, token, amount, eur_value, from_addr, to_addr, direction, category, method, fee_token, fee_amount, taxable, reason, counterparty, chain_id, meta. to_dict() used to get classified_dicts. |
| **classified_dicts** | List of dicts (from ClassifiedItem); mutated with fee_eur, eur_value, chain_id. Used for reward eur_value, fee aggregation, vault_exit_resolver, group_rewards, PDF reward_records. |
| **price_map** | Dict[(normalized_symbol, ts), float]. Built from batch price results; used for base token eur_value, fee_eur, reward eur_value. |
| **GainRow** | One per FIFO disposal. dt_iso, token, amount_out, proceeds_eur, cost_basis_eur, pnl_eur, method, tx_hash, buy_date_iso, hold_days, tax_type, taxable, is_reinvest. |
| **economic_gains** | List of dicts: one economic event per tx (or per chosen category). Aggregated proceeds_eur, cost_basis_eur, pnl_eur, fees_eur, net_pnl_eur, taxable, hold_days; tx_hash, category, dt_iso, token, etc. After apply_vault_exits: may include position_exit. After _cleanup_vault_exit_per_tx: only vault_exit rows kept per tx when vault_exit exists. Input to PDF and audit CSV. |
| **totals** | Dict from compute_gains: e.g. per-token PnL totals. Passed to build_pdf as summary. |
| **reward_summary** | From group_rewards: aggregated reward amounts by token. |

**Evolution:** Raw CSV row → RawRow → filtered dict → ClassifiedItem → (same items as dicts with fee_eur, eur_value) → FIFO produces GainRow → group_gains_economic → economic_gains dicts → vault_exits and fees applied → report inputs.

---

## 5. Price System

- **Batching:** Pipeline collects all needed (symbol, ts, chain) as **PriceQuery** from classified (base tokens, fees, rewards). **resolve_prices_batch(queries)** deduplicates by internal key (symbol, date, source, etc.), calls **get_price** once per unique key, returns a list of results in query order. **_build_price_map** maps (map_token(symbol), ts) → price for pipeline use.
- **Symbol normalization:** **token_mapper.map_token(symbol)** maps raw symbols to a canonical symbol (e.g. WETH/STETH → ETH, USDC/USDT/DAI → USD). Used for cache key and provider lookups; chain is not part of the cache key (used for logging).
- **Resolution order (get_price):** (1) RAM cache, (2) disk cache (SQLite eur_price_cache.sqlite), (3) live: hybrid (provider_master: CSV, Yahoo, Binance, Kraken) then **CoinGecko fallback** if hybrid returns 0.
- **Caching:** In-memory and SQLite; TTL by policy (e.g. recent vs historic_final).
- **Usage:** **eur_value** = amount × price for base tokens and rewards; **fee_eur** = fee_amount × price; **proceeds** and **cost basis** come from FIFO (eur_value at inflow/outflow) and from vault_exit_resolver (ledger cost vs. proceeds from base-asset IN).

---

## 6. Transaction Classification

- **evaluate_batch** uses **method** (from tx: method, function_name, action, input_function, etc.), **direction** (in/out/internal from from_addr/to_addr vs wallet), and **raw category** from CSV. **_basic_category(method, direction)** maps method keywords (swap, trade, sell, buy, deposit, withdraw, reward, claim, harvest, transfer) to a base category. **_refine_category** then overrides using counterparty info (contract_labeler: label, protocol, type, tags) and rules: |
  - **internal_transfer**: direction == "internal" or both sender/receiver in customer_wallets or is_self_transfer(wallet, from, to). |
  - Bridge, Pendle, LP, restake, reward-style flows by counterparty/method. |
  - Fallback: base_category or "unknown".
- **Swap detection:** _postprocess_swaps: per tx, if both in and out legs exist, gray-zone categories (withdraw, transfer, receive, deposit, unknown) can be overridden to "swap" (with can_override by category priority).
- **LP:** _postprocess_lp: method patterns (add liquidity, remove liquidity, etc.) and counterparty protocol/type → lp_add / lp_remove.
- **Pendle:** _postprocess_pendle: counterparty + method → pendle_deposit, pendle_redeem, pendle_reward.
- **Rewards:** Categories: reward, staking_reward, vault_reward, pendle_reward, restake_reward, airdrop, learning_reward, earn_reward. Detected via method (reward, claim, harvest) or counterparty/refine logic.
- **Method signature:** The raw **method** string (e.g. "Transfer", "0x095ea7b3") is stored on ClassifiedItem and used in debug reports (unknown/generic classifications, reward detection) to see which method names produced which categories.

---

## 7. FIFO Gain Engine

- **Lot:** (token, amount, cost_eur, timestamp, reinvest). Lots live in a dict per token: **lots[token]** = list of lots (FIFO queue).
- **Creation (inflow):** add_lot(token, amount, cost_eur, timestamp, reinvest). Used for: buy, receive, bridge_in, deposit, reward, staking_reward, airdrop, lp_add (process_lp_add), pendle_deposit, restake_in. Rewards/airdrop marked reinvest=True.
- **Consumption (outflow):** remove_lot(lots, token, amount) returns list of lots consumed (possibly partial). For each consumed lot: **proceeds** = eur_value × (lot_amount / total_amount); **hold_days** = calc_holding_days(lot.timestamp, ts_sell); **tax_type** = classify_tax_type(category); **taxable** = taxable_status(tax_type, hold_days); **pnl** = proceeds − lot.cost_eur. One **GainRow** per consumed lot.
- **Holding period:** calc_holding_days(ts_buy, ts_sell) = (ts_sell − ts_buy) // 86400. Used in taxable_status for §23 (≤ 365 days taxable).
- **Economic grouping:** group_gains_economic merges GainRows by tx_hash into one economic event per tx (main category by priority); aggregates proceeds, cost_basis, pnl, fees, net_pnl; min hold_days, any taxable.
- **Exclusions:** internal_transfer and self_transfer are skipped at the start of the gains loop (no inflow/outflow), so they do not create lots or gains.

---

## 8. Debug and Validation System

The codebase supports (or has been extended with) several diagnostics; their exact presence depends on the version of pipeline.py. Conceptually they include:

- **Missing price detection:** When a price for (symbol, date, chain) cannot be resolved (price 0 or missing), the transaction (or row) can be logged and appended to a **missing_prices** list. At the end, a summary "MISSING PRICES: X transactions" (unique tx count) can be printed. Important so that zero eur_value/fee_eur is not silently used in tax figures.
- **Unknown/generic classification report:** Rows with category in **unknown**, **transfer**, **internal_transfer** are collected. A summary prints the count and **method signatures** that produced these categories (method → count). Helps find unclassified or generic flows that may need rule or mapping updates.
- **Fee summary:** total_fees_eur, transactions_with_fee, transactions_without_fee (from classified_dicts). Verifies fee attribution.
- **Reward detection report:** Rows with category in REWARD_CATEGORIES; per-row tx_hash, category, method; then method-signature summary. Confirms reward identification.
- **Pipeline health summary:** Can include rows_loaded, rows_classified, gains_generated, economic_gains count, rewards_detected, missing_prices (unique txs), unknown_methods (count of "unknown" category), zero_pnl_swaps (swap/sell with pnl_eur ≈ 0), total_fees_eur. Gives a single snapshot of pipeline output for verification.
- **Price/value logging:** [PRICE REQUEST], [PRICE FETCH], [PRICE DEBUG], [VALUE CALC] logs in the price provider and pipeline for tracing which symbol/date/chain was requested and which source/price was used.

These diagnostics help ensure that tax calculations are based on complete prices, correct classification, and expected counts and totals.

---

## 9. Current Capabilities

- **EVM explorer data:** Read Etherscan-style CSVs (normal, ERC20, internal); chain_id from path or config; multiple chains (eth, arb, op, base, bnb, matic, avax, ftm). |
- **Coinbase:** CSV and PDF transaction history; rewards loader. |
- **Download:** Optional fetch of transactions via Etherscan-compatible APIs (run_wallet); retry and backoff. |
- **Unified pipeline:** Single run_pipeline(); all runners use it. |
- **Classification:** Method + direction + counterparty; swap/LP/Pendle/restake postprocessors; internal_transfer for same-wallet or same-customer transfers (customer_wallets passed from pipeline). |
- **Batch price resolution:** One pass of unique PriceQuery; price_map for eur_value and fee_eur. |
- **FIFO:** Lot tracking per token; add_lot/remove_lot; GainRow with proceeds, cost_basis, pnl, hold_days, tax_type, taxable. |
- **Economic grouping:** One event per tx with priority; fees_eur and net_pnl_eur. |
- **Vault exits:** Position ledger from vault-token mints; position_exit with cost_basis_eur, pnl_eur, hold_days; §23 cleanup. |
- **German tax rules:** §23 (holding period), §22 (rewards); taxable_status and classify_tax_type. |
- **Reporting:** PDF (ReportLab) and audit CSV. |
- **Multi-wallet customer:** run_customer with config.json and inbox per wallet/chain; one report per customer. |

---

## 10. Current Limitations

- **Tax logic coverage:** taxlogic_de.json may lack entries for position_exit, vault_exit, learning_reward, earn_reward; those categories then get default "Unbekannt" and taxable false, which can under-report. |
- **Price engine:** Single canonical symbol per token (no chain-specific pricing in cache key); CoinGecko free-tier limits; API keys not consistently configurable for all providers. |
- **Classification:** Unknown methods fall to "unknown"; complex multi-hop or aggregator swaps may remain transfer/unknown. No explicit margin/futures/options/lending. |
- **Data sources:** Only EVM (Etherscan-style) and Coinbase; no other CEXs or generic connector framework. |
- **FIFO:** One lot pool per (chain, token) globally; no per-wallet or per-account separation when aggregating multiple wallets. |
- **Vault/LP:** Protocol-specific code (Beefy, Camelot, Pendle, etc.); new protocols require new logic. |
- **Tax jurisdiction:** German rules only; no abstraction for other countries. |
- **Reporting:** PDF + one audit CSV; no API, no multi-year or multi-currency aggregation. |
- **Validation:** No formal schema validation for wallet_data or customer config; missing price policy (warn/skip/fail) not centralized. |
- **Testing:** Limited end-to-end and edge-case coverage (missing price, unknown category, multi-chain). |

---

## 11. Future Potential

Given the current architecture, the system could evolve in these directions without a full rewrite:

- **Full crypto tax engine:** Extend loaders (more CEXs, more chains), complete tax rule coverage (all categories, multiple jurisdictions), and optional per-wallet/account FIFO for multi-customer or multi-account use.
- **Multi-exchange tax platform:** Plugin-style loaders and a common RawRow/ClassifiedItem contract; add Binance, Kraken, etc., with the same pipeline and reporting.
- **Automated wallet tax reporting:** Tighten download + pipeline + report into a single flow; configurable API keys and rate limits; optional scheduling or webhook-triggered runs.
- **SaaS tax-report generator:** Expose run_pipeline behind an API; add authentication, job queue, and structured output (JSON/API) in addition to PDF/CSV; multi-tenant customer and report storage.

The pipeline is already modular (load → classify → price → eur_value → FIFO → grouping → vault → tax → rewards → report); extending data sources, tax rules, and output formats fits this structure.

---

*Document based on the current taxtrack codebase. Implementation details (e.g. presence of every debug summary in pipeline.py) may vary by commit.*
