# Taxtrack: Project Tree and Core Implementation Reference

Full project tree, `run_pipeline()` with commented stages, classification engine, and gains engine implementation.

---

## 1. Full Project Tree

```
taxtrack/
├── __init__.py (if present)
│
├── root/                          # Entry points & pipeline
│   ├── __init__.py
│   ├── pipeline.py                # ★ run_pipeline() – unified pipeline
│   ├── run_reference.py           # ★ Entry: test runs (data/test_runs/<run>/<chain>/)
│   ├── run_wallet.py              # ★ Entry: single wallet + optional download
│   ├── run_customer.py            # ★ Entry: multi-wallet customer (customers/<name>/)
│   ├── download_wallet.py         # ★ Entry: download only (Etherscan-compatible)
│   ├── main.py                    # Entry: legacy single wallet
│   └── main_evm.py                # Entry: legacy EVM multi-chain
│
├── loaders/                       # Data ingestion
│   ├── __init__.py
│   ├── auto_detect.py             # Format detection, load_auto()
│   ├── evm_master_loader.py       # EVM folder loading, chain_id normalization
│   ├── etherscan/
│   │   ├── normal_loader.py
│   │   ├── erc20_loader.py
│   │   └── internal_loader.py
│   ├── coinbase/
│   │   ├── loader.py
│   │   ├── rewards_loader.py
│   │   ├── pdf_loader.py
│   │   └── coinbase_rules.py
│   └── generic/
│       ├── __init__.py
│       └── generic_loader.py
│
├── rules/                         # Classification & tax rules
│   ├── __init__.py
│   ├── evaluate.py                # ★ evaluate_batch, _basic_category, _refine_category, swap/LP/Pendle
│   ├── taxlogic.py                # TaxLogic, get_rule()
│   └── taxlogic_de.json           # German tax rules (category → taxable, paragraph, type)
│
├── analyze/                       # FIFO, gains, grouping, vault/LP/Pendle
│   ├── __init__.py
│   ├── gains.py                   # ★ compute_gains(), GainRow, gains loop
│   ├── lot_tracker.py             # ★ Lot, add_lot(), remove_lot()
│   ├── gain_grouping.py           # ★ group_gains_economic()
│   ├── vault_exit_resolver.py     # apply_vault_exits(), position ledger
│   ├── lp_engine.py               # process_lp_add, process_lp_remove
│   ├── pendle_engine.py           # process_pendle_deposit, process_pendle_redeem
│   ├── restake_engine.py          # process_restake_in, process_restake_out
│   ├── swap_engine.py             # swap detection helpers
│   ├── reward_grouping.py         # group_rewards(), REWARD_CATEGORIES
│   ├── tax_rules.py               # calc_holding_days, classify_tax_type, taxable_status
│   ├── economic_events.py
│   ├── fee_validator.py
│   ├── fee_origin.py
│   ├── counterparty.py
│   └── relation_engine.py
│
├── prices/                        # Price resolution & providers
│   ├── __init__.py                # PriceQuery, get_price, get_eur_price, resolve_prices_batch
│   ├── price_provider.py          # ★ Main: RAM/disk cache, get_price(), get_eur_price()
│   ├── price_resolver.py          # ★ resolve_prices_batch() – dedup and batch get_price
│   ├── token_mapper.py            # map_token() – symbol normalization
│   ├── provider_master.py        # Hybrid: CSV, Yahoo, Binance, Kraken
│   ├── provider_csv.py            # CSV price source
│   ├── coingecko_price_provider.py # CoinGecko fallback
│   ├── fx_price_provider.py       # FX (e.g. USD→EUR)
│   ├── _versioning.py
│   └── RESERVED/
│       └── provider_coingecko.py
│
├── pdf/                           # Reporting
│   ├── pdf_report.py              # build_pdf()
│   ├── utils.py
│   ├── sections/
│   │   ├── cover.py
│   │   ├── executive_summary.py
│   │   ├── transactions.py
│   │   ├── rewards.py
│   │   ├── fees.py
│   │   ├── lp.py
│   │   ├── counterparties.py
│   │   └── legend.py
│   ├── theme/
│   │   ├── colors.py
│   │   ├── typography.py
│   │   └── tables.py
│   └── layout/
│       └── kpi_boxes.py
│
├── download/                      # Transaction download
│   ├── __init__.py
│   └── etherscan_fetcher.py       # Etherscan-compatible fetch, retry, CSV write
│
├── schemas/
│   └── RawRow.py                  # RawRow dataclass (loader output)
│
├── utils/
│   ├── __init__.py
│   ├── wallet.py                  # is_self_transfer, is_customer_self_transfer
│   ├── contract_labeler.py        # label_address() – counterparty protocol/label
│   ├── debug_log.py
│   ├── cache.py
│   ├── time_range.py
│   ├── time.py
│   ├── path.py
│   ├── num.py
│   ├── gas.py
│   ├── merge_known_contracts.py
│   └── contract_autoupdater.py
│
├── data/
│   └── config/
│       ├── chain_config.py        # CHAIN_CONFIG (eth, arb, op, base, bnb, matic, avax, ftm)
│       ├── taxlogic_de.json       # (or under rules/)
│       └── ...
│
├── customers/                     # Customer layout (config, inbox, reports)
│   └── __init__.py
│
├── debug/
│   ├── print_swaps.py
│   ├── print_tx.py
│   ├── print_raw.py
│   └── print_unknown_swaps.py
│
├── export/
│   └── export_summary.py
│
├── tools/
│   ├── debug_inbox.py
│   ├── fix_coinbase_csv.py
│   ├── download_prices_2024.py
│   ├── download_prices_yahoo_2024.py
│   ├── download_prices_binance_2024.py
│   └── download_prices_kraken_2024.py
│
├── tests/
│   ├── test_vault_exit_resolver.py
│   ├── test_evaluate_gains_flow.py
│   ├── test_price_provider_integration.py
│   ├── test_price_resolver.py
│   ├── test_token_mapper.py
│   ├── test_price_provider_cache.py
│   └── pdf/
│       ├── conftest.py
│       ├── test_pdf_smoke.py
│       ├── test_lp_grouping.py
│       ├── test_lp_open_positions.py
│       ├── test_no_lp_in_pvg.py
│       └── test_pdf_lp_section.py
│
└── docs/
    ├── KRYPTOBILANZ_TECHNICAL_ANALYSIS.md
    ├── ARCHITECTURE_AND_STATUS_REPORT.md
    ├── PIPELINE_ANALYSIS.md
    ├── PROJECT_OVERVIEW_AND_FOLDER_STRUCTURE.md
    ├── CUSTOMER_FOLDER_STRUCTURE_DESIGN.md
    └── WALLET_DATA_AND_RUN.md
```

**Main entrypoints:** `root/run_reference.py`, `root/run_wallet.py`, `root/run_customer.py`, `root/download_wallet.py`, `root/main.py`, `root/main_evm.py`  
**Pipeline module:** `root/pipeline.py` (`run_pipeline`)  
**Price providers:** `prices/price_provider.py`, `prices/provider_master.py`, `prices/provider_csv.py`, `prices/coingecko_price_provider.py`, `prices/fx_price_provider.py`

---

## 2. Full Implementation of run_pipeline() (with comments)

```python
def run_pipeline(
    wallet_data: List[WalletDataItem],
    tax_year: int,
    config: Optional[PipelineConfig] = None,
) -> PipelineResult:
    config = config or {}
    output_dir = config.get("output_dir")
    primary_wallet = config.get("primary_wallet")
    extra_debug_info = config.get("debug_info") or {}
    if primary_wallet is None:
        primary_wallet = (wallet_data[0].get("wallet") or "").lower()

    if not wallet_data:
        raise ValueError("wallet_data is empty")

    # ---------- STAGE 1: Load transactions ----------
    # For each wallet_data item: load from base_dir (normal.csv, erc20.csv, internal.csv)
    # or from files[]. Year filter: ts_from <= timestamp < ts_to. Normalize to dicts with chain_id.
    raw_rows, filtered_dicts = _load_transactions(wallet_data, tax_year)

    if not filtered_dicts:
        return {"economic_gains": [], "classified_dicts": [], "gains": [], "totals": {},
                "reward_summary": {}, "debug_info": {...}}

    # ---------- STAGE 2: Normalize ----------
    # Build tx_hash -> chain_id for downstream; filtered_dicts already have top-level chain_id.
    tx_to_chain = {r.get("tx_hash"): r.get("chain_id") or "" for r in filtered_dicts if r.get("tx_hash")}

    # ---------- STAGE 3: Classify ----------
    # evaluate_batch: method, direction, counterparty -> category (swap, reward, lp_add, etc.).
    # Returns list of ClassifiedItem (and debug_info).
    classified, debug_info = evaluate_batch(filtered_dicts, primary_wallet)

    # ---------- STAGE 4: Resolve prices (batch) ----------
    # Collect unique (symbol, ts, chain) from classified (base tokens, fees, rewards).
    # resolve_prices_batch deduplicates and calls get_price once per unique key.
    # _build_price_map: (normalized_symbol, ts) -> price.
    price_queries = _collect_price_queries_from_classified(classified)
    if price_queries:
        price_results = resolve_prices_batch(price_queries)
        price_map = _build_price_map(price_queries, price_results)
    else:
        price_map = {}

    # ---------- STAGE 5: Compute eur_value and fee_eur ----------
    # Base tokens (ETH, WETH, USDC, ...): eur_value = amount * price from price_map.
    _fill_base_token_eur_value(classified, price_map)
    # Convert ClassifiedItem list to list of dicts; inject chain_id.
    classified_dicts = [c.to_dict() if hasattr(c, "to_dict") else c for c in classified]
    for d in classified_dicts:
        d.setdefault("chain_id", tx_to_chain.get(d.get("tx_hash", ""), ""))
    # Fee in EUR per row: fee_eur = fee_amount * price(fee_token, ts).
    _fee_eur_on_classified_dicts(classified_dicts, price_map)
    # LP/Vault mint: funding OUT (base assets) eur_value -> proportional eur_value for vault/LP IN.
    _lp_vault_mint_eur_value(classified)
    # Sync eur_value from classified back into classified_dicts (without rebuilding to keep fee_eur).
    for i, c in enumerate(classified):
        if i < len(classified_dicts):
            classified_dicts[i]["eur_value"] = float(getattr(c, "eur_value", 0.0) or 0.0)
            classified_dicts[i].setdefault("chain_id", tx_to_chain.get(getattr(c, "tx_hash", ""), ""))

    # ---------- STAGE 6: FIFO gain calculation ----------
    # compute_gains(classified): sort by time; add_lot on inflows, remove_lot on outflows;
    # one GainRow per consumed lot (proceeds, cost_basis, pnl, hold_days, taxable).
    gains, totals = compute_gains(classified)

    # ---------- STAGE 7: Economic grouping ----------
    # One economic event per tx; priority: position_exit, vault_exit, lp_remove, pendle_redeem, swap, sell.
    # Aggregates proceeds_eur, cost_basis_eur, pnl_eur, fees_eur, net_pnl_eur per event.
    economic_gains = group_gains_economic([g.to_dict() for g in gains])

    # ---------- STAGE 8: Resolve vault exits ----------
    # apply_vault_exits: build position ledger from vault-token mints; match OUT vault + IN base -> position_exit.
    # _cleanup_vault_exit_per_tx: per tx, if any vault_exit exists, keep only vault_exit rows (§23).
    economic_gains = apply_vault_exits(economic_gains, classified_dicts, [g.to_dict() for g in gains])
    _cleanup_vault_exit_per_tx(economic_gains)

    # ---------- STAGE 9: Apply tax logic ----------
    # Aggregate fee_eur per tx; set fees_eur and net_pnl_eur on each economic gain.
    _apply_fees_net_pnl(classified_dicts, economic_gains)
    # Fill reward eur_value from price_map where missing.
    _reward_eur_value(classified_dicts, price_map)
    # Fallback: use usd_value / USDValueDayOfTx as eur_value if still missing.
    _usd_fallback_eur_value(classified_dicts)

    # ---------- STAGE 10: Compute rewards ----------
    # group_rewards(classified_dicts): aggregate by token for reward categories.
    reward_summary = group_rewards(classified_dicts)

    debug_info_out = {"wallet": primary_wallet, "tax_year": tax_year, "from": f"{tax_year}-01-01",
                      "to": f"{tax_year}-12-31", **(debug_info or {}), **extra_debug_info}

    # ---------- STAGE 11: Generate report ----------
    if output_dir:
        _write_audit_csv(economic_gains, gains, audit_file, tax_year)
        build_pdf(economic_records=economic_gains, reward_records=classified_dicts,
                  summary=totals, debug_info=debug_info_out, outpath=str(pdf_file))

    return {
        "economic_gains": economic_gains,
        "classified_dicts": classified_dicts,
        "gains": gains,
        "totals": totals,
        "reward_summary": reward_summary,
        "debug_info": debug_info_out,
    }
```

---

## 3. Classification Engine

### evaluate_batch

- **Input:** `txs` (list of dicts with tx_hash, dt_iso, from/to, token, amount, method, direction, category, chain_id, …), `wallet` (primary wallet address).
- **Flow:** For each tx: derive `dirn` from wallet vs from/to; get `raw_cat` from tx; `base_cat = _basic_category(method, dirn)`; resolve counterparty via `label_address(to/from)`; `category = _refine_category(base_cat, raw_cat, method, dirn, wallet, from_addr, to_addr, cp_info)`. Look up tax rule and build `ClassifiedItem`; append to result. Then **postprocess**: `_postprocess_swaps(result)`, `_postprocess_lp(result)`, `_postprocess_pendle(result)`.
- **Output:** `(List[ClassifiedItem], debug_info)`.

### _basic_category(method, direction)

- **Purpose:** First-pass category from method string and direction only.
- **Logic:**
  - `"swap"` if "swap" or "trade" in method
  - `"sell"` if "sell" in method
  - `"buy"` if "buy" in method
  - `"deposit"` if "deposit" in method
  - `"withdraw"` if "withdraw" or "redeem" in method
  - `"reward"` if any of "reward", "claim", "harvest" in method
  - `"transfer"` → "receive" if direction=="in", else "withdraw"
  - Else `"unknown"`

### _refine_category(base_category, raw_category, method, direction, wallet, from_addr, to_addr, cp_info)

- **Purpose:** Override base/raw using counterparty (label, protocol, type, tags) and rules.
- **Order:**
  1. **Self-transfer / internal:** `is_self_transfer(wallet, from_addr, to_addr)` or `direction == "internal"` → `"internal_transfer"`.
  2. **Raw category:** If raw not in (erc20_transfer, native_transfer, transfer, unknown) → return raw.
  3. **Bridge:** "bridge" in joined (label+proto+type+tags) → `"bridge_out"` / `"bridge_in"`.
  4. **Pendle:** "pendle" in joined → withdraw/redeem → `"pendle_redeem"`; out → `"pendle_deposit"`; in → `"pendle_reward"`; else `"pendle_unknown"`.
  5. **DEX:** router or proto==dex → swap/out → `"swap"`.
  6. **Lending:** proto==lending → repay / deposit / withdraw.
  7. **Restake:** "restake" in joined → out → `"restake_in"`, else `"restake_out"`.
  8. **LP/Vault:** "vault"/"lp"/"liquidity" in joined → out → `"lp_add"`, else `"lp_remove"`.
  9. **Rewards:** method contains reward/claim/harvest → `"reward"`.
  10. **Fallback:** base if not unknown; else raw transfer → receive/withdraw; else `"transfer"`.

### Swap detection (_postprocess_swaps)

- **Purpose:** Mark gray-zone OUT legs as swap when tx has both IN and OUT flows.
- **Logic:** Group items by `tx_hash`. For each tx, if there is at least one IN and one OUT with positive amount: for each OUT row, if category is in (withdraw, transfer, receive, deposit, unknown, erc20_transfer, native_transfer_out) and not reward/claim/harvest/restake/bridge, and `can_override(category, "swap")`, set `r.category = "swap"`. Leaves swap, sell, lp_remove, pendle_redeem, restake_out, bridge_out, reward unchanged.

### LP detection (_postprocess_lp)

- **Purpose:** Mark gray-zone rows as lp_add or lp_remove from method and counterparty.
- **Logic:** Group by tx_hash. For each row: skip if already swap, pendle_*, restake_*, bridge_*, reward. **LP ADD:** if method contains "add liquidity", "increase", "modify liquidity", "mint" or counterparty contains "pool", and category in (transfer, withdraw, receive, deposit, unknown, erc20_transfer), and `can_override(category, "lp_add")` → `lp_add`. **LP REMOVE:** if method contains "remove liquidity", "decrease", "collect", "burn" and same gray categories and `can_override(category, "lp_remove")` → `lp_remove`.

### Pendle logic (_postprocess_pendle)

- **Purpose:** Mark PENDLE-LPT OUT flows as pendle_redeem when category is still generic.
- **Logic:** For each item: if token not in {"PENDLE-LPT", "PENDLE_LPT"} or direction != "out", skip. If category already pendle_*, lp_*, restake_*, bridge_*, reward, swap, sell → skip. If category in (transfer, withdraw, receive, deposit, unknown, erc20_transfer) and `can_override(category, "pendle_redeem")` → `r.category = "pendle_redeem"`.

---

## 4. Gains Engine Implementation

### Lot (lot_tracker.py)

```python
@dataclass
class Lot:
    token: str
    amount: float
    cost_eur: float
    timestamp: int
    reinvest: bool = False
```

### add_lot(lots, token, amount, cost_eur, timestamp, reinvest=False)

- **Purpose:** Add an acquisition to the FIFO queue for that token.
- **Logic:** If amount <= 0 return. Else append `Lot(token, amount, cost_eur, timestamp, reinvest)` to `lots[token]`.

### remove_lot(lots, token, amount)

- **Purpose:** Consume amount from the FIFO queue for token; return list of lots (or partial lots) used.
- **Logic:** `remaining = amount`. While remaining > 0 and queue non-empty: take front lot. If lot.amount <= remaining: append full lot to used, remaining -= lot.amount, pop. Else: append partial lot (remaining, cost_eur * ratio), reduce lot.amount and lot.cost_eur by that part, remaining = 0. Return used.

### Gains loop (compute_gains in gains.py)

- **Setup:** `_normalize_liquidity_and_pendle(classified_items)` (e.g. set eur_value for pendle_redeem/restake_out from inflow sum). Sort classified by dt_iso. `lots = defaultdict(list)`, `gains = []`.
- **Inflow helper:** `inflow(token, amount, eur, ts, reinvest)` → `add_lot(lots, token, amount, eur, ts, reinvest)`.
- **Outflow helper:** `outflow(dt_iso, token, amount, eur_val, cat, txh)`: `used = remove_lot(lots, token, amount)`. For each lot in used: share = lot.amount / total_amt; proceeds = eur_val * share; hold_days = calc_holding_days(lot.timestamp, ts_sell); tax_type = classify_tax_type(cat); taxable = taxable_status(tax_type, hold_days); pnl = proceeds - lot.cost_eur. Append **GainRow**(dt_iso, token, amount_out=lot.amount, proceeds_eur, cost_basis_eur=lot.cost_eur, pnl_eur=pnl, method=cat, tx_hash=txh, buy_date_iso, hold_days, tax_type, taxable, is_reinvest=lot.reinvest).
- **Main loop over classified_items:**
  - Skip rewards (§22, no FIFO): staking_reward, reward, learning_reward, earn_reward.
  - Skip internal_transfer, self_transfer.
  - **INFLOWS:** buy, receive, bridge_in, deposit or dirn=="in" → inflow(..., reinvest=False). reward, staking_reward, reinvest, airdrop → inflow(..., reinvest=True). lp_add → process_lp_add → inflow. pendle_deposit → process_pendle_deposit → inflow. restake_in → process_restake_in → inflow.
  - **OUTFLOWS:** lp_remove → process_lp_remove (underlyings from same tx), then for each event: inflow for new underlyings, outflow for disposal. pendle_redeem → process_pendle_redeem → outflow. restake_out → process_restake_out → outflow. sell, swap, withdraw, trade, stable_swap or dirn=="out" → outflow.
- **Totals:** For each gain, totals[token] += pnl_eur. Optionally collect open_lp_positions from remaining lots. Return gains, totals.

### GainRow (gains.py)

```python
@dataclass
class GainRow:
    dt_iso: str
    token: str
    amount_out: float
    proceeds_eur: float
    cost_basis_eur: float
    pnl_eur: float
    method: str
    tx_hash: str
    buy_date_iso: str
    hold_days: int
    tax_type: str
    taxable: bool
    is_reinvest: bool
```

### Economic grouping (group_gains_economic in gain_grouping.py)

- **Input:** List of gain dicts (e.g. from GainRow.to_dict()).
- **Logic:**
  1. Group by `tx_hash`.
  2. For each tx, determine **main category** by ECONOMIC_PRIORITY: position_exit, vault_exit, lp_remove, pendle_redeem, restake_out, swap, sell (first match wins).
  3. If main_cat == "vault_exit", event_rows = all rows for that tx; else event_rows = rows whose method/category == main_cat.
  4. **Aggregate:** proceeds = sum(proceeds_eur), cost = sum(cost_basis_eur), pnl = sum(pnl_eur), fees = sum(fee_eur), net_pnl = pnl - fees; taxable = any(taxable); hold_days = min(hold_days); dt_iso = min(dt_iso); token = single token if same else "MULTI".
  5. Append one dict per tx with tx_hash, category, dt_iso, token, proceeds_eur, cost_basis_eur, pnl_eur, fees_eur, net_pnl_eur, taxable, hold_days, rows.
- **Output:** List of economic event dicts (one per tx with a recognized economic category).

---

*This document reflects the current taxtrack codebase. Line numbers and minor details may vary by commit.*
