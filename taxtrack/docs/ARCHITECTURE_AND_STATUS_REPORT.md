# ZenTaxCore – Architecture and Status Report

Technical overview of the crypto tax engine: pipeline, modules, price engine, data flow, debug system, limitations, and implementation priorities.

---

## 1. CURRENT PIPELINE

End-to-end execution from input to final output:

| Stage | Description |
|-------|--------------|
| **Input** | Wallet CSVs (Etherscan-style: normal.csv, erc20.csv, internal.csv) or Coinbase CSV/PDF; optional: download via Etherscan-compatible APIs. |
| **Load** | `load_auto()` → format detection (EVM normal/ERC20/internal, Coinbase, generic) → parse to `RawRow` (or dict) with `chain_id` from path. |
| **Filter** | Year filter: `ts_from <= timestamp < ts_to` (tax year). |
| **Fee valuation** | For each row: `fee_eur = fee_amount * get_eur_price(fee_token, ts, chain)`. |
| **Classification** | `evaluate_batch()` → method/category (swap, reward, lp_add/lp_remove, transfer, etc.) → `ClassifiedItem` list. |
| **eur_value fill** | Missing eur_value for base tokens (ETH, WETH, USDC, …) and rewards: `eur_value = amount * get_eur_price(token, ts, chain)`. |
| **LP/Vault mint cost basis** | Per tx: funding OUT (base assets) → proportional eur_value for vault/LP token IN. |
| **FIFO engine** | `compute_gains()` → lot tracking, FIFO matching → gain records (cost_basis_eur, proceeds_eur, pnl_eur, hold_days). |
| **Economic grouping** | `group_gains_economic()` → one economic event per tx (swap, vault_exit, lp_remove, …). |
| **Vault exits** | `apply_vault_exits()` → position_exit/vault_exit from vault-ledger FIFO; fees from tx. |
| **§23 cleanup** | Per tx: if any vault_exit exists, keep only vault_exit rows. |
| **Fees & net PnL** | Aggregate fee_eur per tx; set `fees_eur`, `net_pnl_eur` on economic gains. |
| **Rewards** | Reward categories: eur_value from price engine if missing; `group_rewards()` for summary. |
| **Output** | `build_pdf()` → tax_report_<year>.pdf; audit CSV (tx_hash, date, category, token, amount, proceeds_eur, cost_basis_eur, pnl_eur, taxable). |

**Entry points**

- **Reference run:** `run_reference.py --run <name> --year <year> --chain eth,arb` (data under `data/test_runs/<run>/<chain>/*.csv`).
- **Single wallet:** `run_wallet.py --wallet 0x... --chains eth,arb --year 2025` (optional download → `data/inbox/<wallet>/<chain>/` → pipeline → `data/out/reports/<wallet>/`).
- **Customer (multi-wallet):** `run_customer.py --customer <name> --year 2025` (inbox under `customers/<name>/inbox/<wallet>/<chain>/`, reports under `customers/<name>/reports/`).

---

## 2. MODULE MAP

| Module | Responsibility |
|--------|----------------|
| **loaders/** | Ingest: `auto_detect` (detect format), EVM normal/erc20/internal, Coinbase CSV/PDF/rewards, generic; output RawRow/dict with chain_id. |
| **prices/** | **Price engine:** `price_provider` (RAM → disk → hybrid → CoinGecko), `provider_master` (Hybrid: CSV, Yahoo, Binance, Kraken), `provider_csv`, `coingecko_price_provider`, `token_mapper`, `price_resolver` (batch dedup), `fx_price_provider`. |
| **rules/** | **Classification:** `evaluate` (evaluate_batch → ClassifiedItem), `taxlogic` (German tax rules, holding period, §23/§22). |
| **analyze/** | **FIFO & events:** `gains` (compute_gains, lot_tracker), `lot_tracker`, `gain_grouping` (group_gains_economic), `economic_events`, `vault_exit_resolver`, `lp_engine`, `pendle_engine`, `restake_engine`, `swap_engine`, `reward_grouping`, `tax_rules`, `fee_validator`, `fee_origin`, `counterparty`, `relation_engine`. |
| **pdf/** | **Reporting:** `pdf_report` (build_pdf), sections (cover, executive_summary, transactions, rewards, fees, lp, counterparties, legend), theme (colors, typography, tables), layout (kpi_boxes). |
| **download/** | Etherscan-compatible fetch: `etherscan_fetcher` (normal, ERC20, internal; retry, rate-limit handling); used by `download_wallet.py` and `run_wallet`. |
| **root/** | **Runners:** `run_reference`, `run_wallet`, `run_customer`, `download_wallet`, `main`, `main_evm`. |
| **schemas/** | `RawRow` dataclass (source, tx_hash, timestamp, dt_iso, from/to, token, amount, direction, method, fee_*, category, eur_value, …). |
| **data/config/** | `chain_config` (eth, arb, op, base, bnb, matic, avax, ftm: api, explorer, rpc), token_price_mapping (for CoinGecko). |
| **utils/** | wallet (is_self_transfer), contract_labeler, debug_log, path, time, num, cache, gas, merge_known_contracts. |
| **customers/** | Customer layout: config.json, inbox/<wallet>/<chain>/, reports/. |
| **debug/** | print_swaps, print_tx, print_raw, print_unknown_swaps. |
| **tests/** | Unit/integration: price_provider, price_resolver, token_mapper, evaluate/gains flow, vault_exit_resolver, PDF sections. |

---

## 3. PRICE ENGINE STATUS

**Resolution order (per `get_price(PriceQuery)`):**

1. **RAM cache** – In-memory dict keyed by hash(symbol, date, source, quote, policy, logic_rev). TTL: 7 days (recent) or 365 days (historic_final).
2. **Disk cache** – SQLite `eur_price_cache.sqlite` (key, symbol, date, source, payload, fetched_at, ttl). Same key; expired entries deleted on read.
3. **Live fetch** – `_fetch_from_source()`:
   - **Hybrid (provider_master):** CSV (data/prices) → Yahoo Finance → Binance OHLC → Kraken OHLC → stablecoin/restaking fallbacks.
   - **CoinGecko fallback:** If hybrid returns 0, `get_eur_price_fallback(symbol, ts)` with in-memory cache; built-in + optional `token_price_mapping.json` for symbol → CoinGecko id.

**Token normalization (map_token):**

- `token_mapper.map_token(symbol)` maps raw symbols to canonical (e.g. WETH/STETH/RSETH → ETH, USDT/USDC/DAI → USD, chain tokens ARB/OP/AVAX/BNB/MATIC/FTM). Used for cache key and provider lookups; chain is not part of the cache key (logging only).

**Policies:**

- `historic_final` for data older than `HISTORIC_FREEZE_DAYS` (3); otherwise `recent`. Affects TTL only.

---

## 4. DATA FLOW

Single transaction path:

```
Raw CSV row
  → load_auto (format detection) → RawRow / dict
  → year filter → filtered dicts
  → fee_eur = fee_amount * get_eur_price(fee_token, ts, chain)
  → evaluate_batch → ClassifiedItem (category, direction, token, amount, eur_value, …)
  → eur_value fill for base tokens & rewards (amount * get_eur_price)
  → LP/Vault mint: eur_value from funding OUT assigned to vault/LP IN
  → compute_gains (FIFO lots, add_lot/remove_lot) → Gain records
  → group_gains_economic → one economic event per tx (swap, vault_exit, lp_remove, …)
  → apply_vault_exits → position_exit/vault_exit with cost_basis_eur, pnl_eur, hold_days
  → §23 cleanup (vault_exit dominates per tx)
  → fees_eur, net_pnl_eur on economic gains
  → build_pdf + audit CSV
```

**Key structures:** `RawRow` (loader output) → dict with `chain_id` → `ClassifiedItem` → gain dict (tx_hash, method/category, cost_basis_eur, proceeds_eur, pnl_eur, …) → economic gain dict (same + fees_eur, net_pnl_eur).

---

## 5. DEBUG SYSTEM

| Tag | When | Content |
|-----|------|---------|
| **[PRICE REQUEST]** | Start of `get_price(q)` | Normalized symbol, timestamp, date, chain. |
| **[PRICE FETCH]** | Inside `_fetch_from_source()` | Hybrid attempt; if hybrid=0 then CoinGecko attempt and result or exception. |
| **[PRICE DEBUG]** | After any successful price resolution (RAM, disk, or fresh) | Symbol, date, source (hybrid_eur / coingecko), price. |
| **[VALUE CALC]** | Before each eur_value = amount × price | Token, amount, timestamp, chain. Used in run_reference, run_wallet, run_customer, evm_master_loader, coinbase loader, coinbase rewards_loader. |

Additional debug: LP/Vault mint eur_value logs, position audit (per-tx sums), reward event counts/sums, exit candidates.

---

## 6. CURRENT LIMITATIONS

- **Price engine:** No chain-specific pricing; one canonical symbol per token. CoinGecko free tier rate limits; no configurable API key in code path for some providers.
- **Loaders:** EVM and Coinbase only; no generic CEX/DeFi connector framework. RawRow has no formal `chain_id` field (relies on loader-added dict keys).
- **Classification:** Method/category heuristics and contract labels; unknown methods can fall to transfer/unknown. Swap detection can miss complex multi-hop or aggregator patterns.
- **FIFO:** Single global lot pool per (chain, token); no explicit multi-wallet or multi-account separation in lot_tracker.
- **Vault/LP:** Logic is protocol-specific (Beefy, Camelot, etc.); new protocols require code. Vault ledger only considers vault tokens (mint eur_value > 0).
- **Tax rules:** German (§23, §22) only; no jurisdiction or regime abstraction.
- **Reporting:** PDF + one audit CSV; no API, no multi-year or multi-currency aggregation report.
- **Runners:** Reference run expects fixed folder layout; run_customer and run_wallet duplicate pipeline logic (fee fill, eur_value fill, LP/vault, gains, economic, vault_exits, rewards).
- **Testing:** Limited coverage for full pipeline and edge cases (missing prices, unknown categories, multi-chain).
- **Config:** Chain list in run_wallet vs chain_config; customer config.json schema not enforced; no single config layer for feature flags or thresholds.

---

## 7. NEXT IMPLEMENTATION PRIORITIES

1. **Single pipeline entry** – Extract one “core pipeline” function (load → classify → eur_value → LP/vault → gains → economic → vault_exits → fees/rewards) and call it from run_reference, run_wallet, run_customer to remove duplication and fix drift.
2. **Config and schema** – Central config (chains, features, paths); formal schema for customer config and RawRow/ClassifiedItem (e.g. chain_id on RawRow); validate at startup.
3. **Price engine hardening** – Configurable API keys (env/file); rate limiting and backoff for all providers; optional chain in cache key or provider selection for L2/CEX-specific quotes.
4. **Error and missing-data handling** – Explicit handling for missing prices (warn/skip/fail policy); structured logging (levels, correlation ids) and optional debug toggles.
5. **Testing** – End-to-end tests for run_wallet/run_customer with fixtures; tests for missing price, unknown category, and multi-chain; regression tests for PDF and audit CSV.
6. **Documentation** – API docs for pipeline stages and public functions; runbooks for download vs no-download and customer setup; changelog for tax logic.
7. **Reporting extensions** – Optional JSON/API output; multi-year summary; configurable audit CSV columns and location.
8. **Tax and jurisdiction** – Abstract tax rules (holding period, rates, categories) by jurisdiction; keep German rules as first implementation.
9. **Loader and protocol extensibility** – Plugin-style loaders (register format + parser); document how to add new vault/LP/DeFi protocols.
10. **Operational readiness** – Retry and backoff for download and price APIs; health/readiness checks; optional idempotent report generation and output versioning.

---

*Report generated from repository analysis. Pipeline and module layout reflect current codebase.*
