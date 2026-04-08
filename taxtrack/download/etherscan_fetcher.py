# taxtrack/download/etherscan_fetcher.py
"""
Reusable Etherscan API V2 fetcher for wallet transaction data.

Uses `https://api.etherscan.io/v2/api` with `chainid` per CHAIN_CONFIG (see v2-migration).
Pagination uses `page` + `offset` with offset capped at SAFE_OFFSET_MAX (1000).

Supports all chains in CHAIN_CONFIG: eth, arb, op, base, avax, bnb, matic, ftm.
Writes normal.csv, erc20.csv, internal.csv into the given output_dir (expected:
taxtrack/data/inbox/<wallet>/<chain_id>/).
"""

from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    requests = None  # type: ignore

from taxtrack.data.config.chain_config import CHAIN_CONFIG, ETHERSCAN_V2_API_BASE

logger = logging.getLogger(__name__)

# Etherscan API V2: use modest page sizes to avoid "Result window is too large" (see docs)
SAFE_OFFSET_MAX = 1000
# Legacy alias — prefer SAFE_OFFSET_MAX for new code
PAGE_SIZE = SAFE_OFFSET_MAX
MAX_RETRIES = 5
INITIAL_BACKOFF = 2.0

# All chain_ids that have an explorer API in CHAIN_CONFIG
SUPPORTED_CHAIN_IDS = tuple(CHAIN_CONFIG.keys())

# Public Blockscout-compatible API (no key) — fallback when primary explorer fails or no key (eth only)
BLOCKSCOUT_ETH_API = "https://eth.blockscout.com/api"
# Blockscout: "PageNo x Offset must be <= 10000" — with fixed offset=1000, page 11 fails. Use a
# nominal page size; _fetch_paginated shrinks offset per page to satisfy page*offset<=10000.
BLOCKSCOUT_PAGE_SIZE = 1000
BLOCKSCOUT_MAX_PAGE_OFFSET_PRODUCT = 10000


def _blockscout_offset_for_page(page: int, nominal_page_size: int) -> Optional[int]:
    """
    Blockscout rejects when page * offset > 10000. Return offset for this page, or None if
    no valid offset exists (pagination cannot continue).
    """
    if page < 1:
        return None
    max_offset = BLOCKSCOUT_MAX_PAGE_OFFSET_PRODUCT // page
    if max_offset < 1:
        return None
    return max(1, min(nominal_page_size, max_offset))


class ExplorerIngestError(RuntimeError):
    """Explorer returned NOTOK, network failure, or no usable rows after retries."""


class MissingExplorerAPIKeyError(ExplorerIngestError):
    """Etherscan-family APIs require an API key; optional chain fallback also failed."""


def get_evm_chain_numeric_id(chain_id: str) -> int:
    """
    Return Etherscan API V2 `chainid` for the logical chain (e.g. eth -> 1, arb -> 42161).
    """
    c = CHAIN_CONFIG.get(chain_id.strip().lower())
    if not c:
        raise ValueError(
            f"Unsupported chain_id: {chain_id!r}. "
            f"Supported: {', '.join(SUPPORTED_CHAIN_IDS)}"
        )
    raw = c.get("chain_id")
    if raw is None:
        raise ValueError(f"Chain {chain_id!r} has no chain_id in CHAIN_CONFIG.")
    return int(raw)


def get_etherscan_v2_ingest_url() -> str:
    """Unified Etherscan API V2 base URL (all supported chains use chainid=...)."""
    return ETHERSCAN_V2_API_BASE


def _is_rate_limited(response: Any, data: Optional[Dict[str, Any]]) -> bool:
    """Detect rate limit: HTTP 429 or Etherscan-style NOTOK with rate-limit message."""
    if response is not None and getattr(response, "status_code", None) == 429:
        return True
    if not isinstance(data, dict):
        return False
    msg = (data.get("message") or "").lower()
    result = data.get("result")
    result_str = (result if isinstance(result, str) else "").lower()
    if "rate limit" in msg or "rate limit" in result_str:
        return True
    if "max rate" in result_str or "too many" in msg or "too many" in result_str:
        return True
    if data.get("status") == "0" and ("limit" in result_str or "limit" in msg):
        return True
    return False


def _benign_explorer_status_zero(result: Any, message: str) -> bool:
    """
    Etherscan-compatible APIs use status=0 for some non-error cases (e.g. empty list
    or 'No transactions found'). Do not treat those as fatal.
    """
    msg_l = (message or "").lower()
    if isinstance(result, list) and len(result) == 0:
        return True
    if isinstance(result, str):
        rs = result.lower().strip()
        if "no transactions found" in rs or rs == "no records found":
            return True
        if "no token transfer" in rs or "no token transfers found" in rs:
            return True
    if "no transactions found" in msg_l:
        return True
    return False


def _check_explorer_api_response(data: Dict[str, Any]) -> None:
    """
    Raise if the JSON body indicates NOTOK for a non-benign reason (missing key, etc.).
    """
    if not isinstance(data, dict):
        return
    if str(data.get("status", "1")) != "0":
        return
    msg = (data.get("message") or "").strip()
    res = data.get("result")
    if _benign_explorer_status_zero(res, msg):
        return
    res_str = res if isinstance(res, str) else repr(res)[:500]
    raise ExplorerIngestError(
        f"Explorer API NOTOK: message={msg!r} result={res_str!r}"
    )


def _is_non_retryable_explorer_error(exc: Exception) -> bool:
    """Do not backoff-retry: bad keys and similar will not succeed on repeat."""
    s = str(exc).lower()
    if "invalid api key" in s:
        return True
    if "missing api key" in s:
        return True
    return False


def _request_with_retry(
    api_url: str,
    params: Dict[str, Any],
    api_key: Optional[str] = None,
    max_retries: int = MAX_RETRIES,
    backoff: float = INITIAL_BACKOFF,
) -> Dict[str, Any]:
    """
    Call API with exponential backoff retry.
    Detects rate limits (HTTP 429 or API message); retries up to max_retries (default 5);
    logs each retry. Raises on final failure.
    """
    if requests is None:
        raise RuntimeError("requests library is required for download. pip install requests")

    if api_key:
        params = {**params, "apikey": api_key}

    last_err: Optional[Exception] = None
    for attempt in range(max_retries):
        try:
            r = requests.get(api_url, params=params, timeout=60)
            data = None
            try:
                data = r.json()
            except Exception:
                pass

            if _is_rate_limited(r, data):
                msg = (data or {}).get("message") or (data or {}).get("result") or "rate limit"
                raise ValueError(f"Rate limited (HTTP {r.status_code}): {msg}")

            r.raise_for_status()
            if data is None:
                data = r.json()
            if isinstance(data, dict):
                _check_explorer_api_response(data)
            result = data.get("result")
            result_str = (result if isinstance(result, str) else "").lower()
            msg_str = (data.get("message") or "").lower()
            if "rate limit" in result_str or "rate limit" in msg_str:
                raise ValueError("Rate limited (API message)")
            if "invalid api key" in result_str or "invalid api key" in msg_str:
                raise ValueError("Invalid API key")
            if "missing api key" in result_str or "missing api key" in msg_str:
                raise ValueError("Missing API key (endpoint may require apikey)")
            return data
        except Exception as e:
            last_err = e
            if _is_non_retryable_explorer_error(e):
                raise
            if attempt < max_retries - 1:
                sleep_time = backoff * (2 ** attempt)
                print(
                    f"[etherscan_fetcher] Retry {attempt + 1}/{max_retries} after {e!r} "
                    f"(sleep {sleep_time:.1f}s)"
                )
                time.sleep(sleep_time)
            else:
                raise last_err or RuntimeError("Request failed")
    raise last_err or RuntimeError("Request failed")


def _fetch_paginated(
    api_url: str,
    base_params: Dict[str, Any],
    api_key: Optional[str] = None,
    max_retries: int = MAX_RETRIES,
    page_size: int = PAGE_SIZE,
    evm_chain_id: Optional[int] = None,
    log_label: str = "tx",
) -> List[Dict[str, Any]]:
    """
    Fetch all pages (Etherscan API V2: add chainid; page + offset capped at SAFE_OFFSET_MAX).

    Loops until a page returns fewer than `offset` rows or is empty.
    """
    if page_size > SAFE_OFFSET_MAX:
        raise ValueError(
            f"page_size ({page_size}) exceeds SAFE_OFFSET_MAX ({SAFE_OFFSET_MAX}); "
            "use smaller offset and paginate."
        )
    use_blockscout_window = "blockscout" in (api_url or "").lower()
    all_results: List[Dict[str, Any]] = []
    page = 1
    while True:
        if use_blockscout_window:
            offset = _blockscout_offset_for_page(page, page_size)
            if offset is None:
                logger.warning(
                    "[%s] Blockscout pagination cap (page*offset<=%s): stopping at page=%s total=%s",
                    log_label,
                    BLOCKSCOUT_MAX_PAGE_OFFSET_PRODUCT,
                    page,
                    len(all_results),
                )
                break
        else:
            offset = page_size
        params = {**base_params, "page": page, "offset": offset, "sort": "asc"}
        if evm_chain_id is not None:
            params["chainid"] = evm_chain_id
        data = _request_with_retry(api_url, params, api_key=api_key, max_retries=max_retries)
        result = data.get("result")
        if result is None:
            logger.info("[%s] page=%s offset=%s batch=0 total=%s (no result)", log_label, page, offset, len(all_results))
            break
        if isinstance(result, str):
            # e.g. "No transactions found" or error string — stop pagination
            logger.info("[%s] page=%s offset=%s non-list result=%r total=%s", log_label, page, offset, result[:200] if len(result) > 200 else result, len(all_results))
            break
        if isinstance(result, list):
            batch_n = len(result)
            all_results.extend(result)
            logger.info(
                "[%s] page=%s offset=%s batch=%s total_so_far=%s",
                log_label,
                page,
                offset,
                batch_n,
                len(all_results),
            )
            if batch_n == 0:
                break
            if batch_n < offset:
                break
        else:
            break
        page += 1
    logger.info("[%s] fetch complete: total_count=%s", log_label, len(all_results))
    return all_results


def _parse_int(val: Any) -> int:
    """Parse int from hex string (0x...) or decimal string/int."""
    if val is None:
        return 0
    if isinstance(val, int):
        return val
    s = str(val).strip()
    if not s:
        return 0
    return int(s, 16) if s.startswith("0x") else int(s)


def _wei_to_eth(wei_val: Any) -> float:
    """Accept hex string or decimal string/int."""
    try:
        if wei_val is None:
            return 0.0
        if isinstance(wei_val, int):
            return wei_val / 1e18
        s = str(wei_val).strip()
        if not s:
            return 0.0
        val = int(s, 16) if s.startswith("0x") else int(s)
        return val / 1e18
    except (ValueError, TypeError):
        return 0.0


def _ts_to_utc_str(ts: Any) -> str:
    try:
        t = int(ts)
        return datetime.utcfromtimestamp(t).strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return ""


def fetch_normal_txs(
    api_url: str,
    address: str,
    api_key: Optional[str] = None,
    start_block: int = 0,
    end_block: int = 99999999,
    max_retries: int = MAX_RETRIES,
    page_size: int = PAGE_SIZE,
    evm_chain_id: Optional[int] = None,
    log_label: str = "normal",
) -> List[Dict[str, Any]]:
    """Fetch normal (native) transaction list. Returns list of API result items."""
    params = {
        "module": "account",
        "action": "txlist",
        "address": address.strip(),
        "startblock": start_block,
        "endblock": end_block,
    }
    return _fetch_paginated(
        api_url,
        params,
        api_key=api_key,
        max_retries=max_retries,
        page_size=page_size,
        evm_chain_id=evm_chain_id,
        log_label=log_label,
    )


def fetch_erc20_txs(
    api_url: str,
    address: str,
    api_key: Optional[str] = None,
    start_block: int = 0,
    end_block: int = 99999999,
    max_retries: int = MAX_RETRIES,
    page_size: int = PAGE_SIZE,
    evm_chain_id: Optional[int] = None,
    log_label: str = "erc20",
) -> List[Dict[str, Any]]:
    """Fetch ERC20 token transfer list."""
    params = {
        "module": "account",
        "action": "tokentx",
        "address": address.strip(),
        "startblock": start_block,
        "endblock": end_block,
    }
    return _fetch_paginated(
        api_url,
        params,
        api_key=api_key,
        max_retries=max_retries,
        page_size=page_size,
        evm_chain_id=evm_chain_id,
        log_label=log_label,
    )


def fetch_internal_txs(
    api_url: str,
    address: str,
    api_key: Optional[str] = None,
    start_block: int = 0,
    end_block: int = 99999999,
    max_retries: int = MAX_RETRIES,
    page_size: int = PAGE_SIZE,
    evm_chain_id: Optional[int] = None,
    log_label: str = "internal",
) -> List[Dict[str, Any]]:
    """Fetch internal transaction list."""
    params = {
        "module": "account",
        "action": "txlistinternal",
        "address": address.strip(),
        "startblock": start_block,
        "endblock": end_block,
    }
    return _fetch_paginated(
        api_url,
        params,
        api_key=api_key,
        max_retries=max_retries,
        page_size=page_size,
        evm_chain_id=evm_chain_id,
        log_label=log_label,
    )


# ---------- CSV writing (columns matching existing loaders) ----------

def _normal_row_to_csv_record(tx: Dict[str, Any], wallet_lower: str) -> Dict[str, str]:
    from_ = (tx.get("from") or "").lower()
    to_ = (tx.get("to") or "").lower()
    value_wei = tx.get("value", "0")
    value_eth = _wei_to_eth(value_wei)
    ts = tx.get("timeStamp", 0)
    try:
        gu = _parse_int(tx.get("gasUsed", "0"))
        gp = _parse_int(tx.get("gasPrice", "0"))
        fee_eth = gu * gp / 1e18
    except (ValueError, TypeError):
        fee_eth = 0.0

    if to_ == wallet_lower and from_ != wallet_lower:
        value_in, value_out = value_eth, 0.0
    elif from_ == wallet_lower and to_ != wallet_lower:
        value_in, value_out = 0.0, value_eth
    else:
        value_in, value_out = 0.0, 0.0

    return {
        "Transaction Hash": tx.get("hash", ""),
        "Blockno": str(tx.get("blockNumber", "")),
        "UnixTimestamp": str(ts),
        "DateTime (UTC)": _ts_to_utc_str(ts),
        "From": from_,
        "To": to_,
        "ContractAddress": tx.get("contractAddress", ""),
        "Value_IN(ETH)": str(value_in),
        "Value_OUT(ETH)": str(value_out),
        "TxnFee(ETH)": f"{fee_eth:.18f}".rstrip("0").rstrip("."),
        "Method": tx.get("functionName") or tx.get("methodId") or "",
        "Status": "1" if tx.get("isError") == "0" else "0",
        "ErrCode": tx.get("isError", ""),
    }


def _erc20_row_to_csv_record(tx: Dict[str, Any]) -> Dict[str, str]:
    value_raw = tx.get("value", "0")
    try:
        decimals = _parse_int(tx.get("tokenDecimal")) or 18
    except (ValueError, TypeError):
        decimals = 18
    try:
        val = _parse_int(value_raw)
        value_human = val / (10 ** decimals)
    except (ValueError, TypeError):
        value_human = 0.0
    ts = tx.get("timeStamp", 0)
    return {
        "Transaction Hash": tx.get("hash", ""),
        "Blockno": str(tx.get("blockNumber", "")),
        "UnixTimestamp": str(ts),
        "DateTime (UTC)": _ts_to_utc_str(ts),
        "From": (tx.get("from") or "").lower(),
        "To": (tx.get("to") or "").lower(),
        "ContractAddress": (tx.get("contractAddress") or "").lower(),
        "TokenName": tx.get("tokenName", ""),
        "TokenSymbol": tx.get("tokenSymbol", ""),
        "TokenValue": str(value_human),
        "TokenDecimal": str(decimals),
        "FunctionName": tx.get("functionName") or "",
    }


def _internal_row_to_csv_record(tx: Dict[str, Any], wallet_lower: str) -> Dict[str, str]:
    from_ = (tx.get("from") or "").lower()
    to_ = (tx.get("to") or "").lower()
    value_eth = _wei_to_eth(tx.get("value", "0"))
    ts = tx.get("timeStamp", 0)
    if to_ == wallet_lower and from_ != wallet_lower:
        value_in, value_out = value_eth, 0.0
    elif from_ == wallet_lower and to_ != wallet_lower:
        value_in, value_out = 0.0, value_eth
    else:
        value_in, value_out = 0.0, 0.0
    return {
        "Transaction Hash": tx.get("hash", ""),
        "Blockno": str(tx.get("blockNumber", "")),
        "UnixTimestamp": str(ts),
        "DateTime (UTC)": _ts_to_utc_str(ts),
        "From": from_,
        "To": to_,
        "TxTo": to_,
        "Value_IN(ETH)": str(value_in),
        "Value_OUT(ETH)": str(value_out),
        "Type": tx.get("type", "call"),
    }


def _write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _format_download_error(exc: Exception) -> str:
    """Turn common API/download failures into a short CLI message."""
    raw = str(exc)
    msg = raw.lower()
    if "rate limit" in msg or "429" in msg:
        return "API rate limit hit. Wait and retry or use an API key."
    if "invalid api key" in msg:
        return "Invalid API key. Check your Etherscan (or chain) API key."
    if "missing api key" in msg:
        return "This endpoint requires an API key (set --api-key or env)."
    if "result window" in msg or "too large" in msg:
        return (
            "Explorer rejected the request window (pagination offset too large or similar). "
            f"Details: {raw}"
        )
    if "deprecated" in msg and "v1" in msg:
        return (
            "Deprecated API (V1) response. Ensure Etherscan API V2 is used with chainid. "
            f"Details: {raw}"
        )
    if "unsupported chain" in msg or "no 'api' endpoint" in msg:
        return raw
    if "connection" in msg or "timeout" in msg or "resolve" in msg:
        return "Network error: " + raw
    return raw


def _write_chain_csvs(
    api_url: str,
    chain_id: str,
    address: str,
    output_dir: Path,
    api_key: Optional[str],
    *,
    source_label: str,
    evm_chain_id: Optional[int] = None,
    start_block: int = 0,
    end_block: int = 99999999,
    max_retries: int = MAX_RETRIES,
) -> Dict[str, Any]:
    """
    Fetch and write the three CSVs. Returns paths, per-type counts, total_raw, source_label.

    For Etherscan API V2 (`evm_chain_id` set), `chainid` is sent on every request.
    For Blockscout (`evm_chain_id` None), no `chainid` parameter is used.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    ps = BLOCKSCOUT_PAGE_SIZE if "blockscout" in (api_url or "").lower() else PAGE_SIZE
    if ps > SAFE_OFFSET_MAX:
        ps = SAFE_OFFSET_MAX

    normal_list = fetch_normal_txs(
        api_url,
        address,
        api_key=api_key,
        start_block=start_block,
        end_block=end_block,
        max_retries=max_retries,
        page_size=ps,
        evm_chain_id=evm_chain_id,
        log_label=f"{chain_id}/normal",
    )
    print(f"  transactions fetched: {len(normal_list)}")
    normal_records = [_normal_row_to_csv_record(t, address) for t in normal_list]
    normal_fields = [
        "Transaction Hash", "Blockno", "UnixTimestamp", "DateTime (UTC)",
        "From", "To", "ContractAddress", "Value_IN(ETH)", "Value_OUT(ETH)",
        "TxnFee(ETH)", "Method", "Status", "ErrCode",
    ]
    normal_path = output_dir / "normal.csv"
    _write_csv(normal_path, normal_records, normal_fields)

    erc20_list = fetch_erc20_txs(
        api_url,
        address,
        api_key=api_key,
        start_block=start_block,
        end_block=end_block,
        max_retries=max_retries,
        page_size=ps,
        evm_chain_id=evm_chain_id,
        log_label=f"{chain_id}/erc20",
    )
    print(f"  erc20 transfers: {len(erc20_list)}")
    erc20_records = [_erc20_row_to_csv_record(t) for t in erc20_list]
    erc20_fields = [
        "Transaction Hash", "Blockno", "UnixTimestamp", "DateTime (UTC)",
        "From", "To", "ContractAddress", "TokenName", "TokenSymbol",
        "TokenValue", "TokenDecimal", "FunctionName",
    ]
    erc20_path = output_dir / "erc20.csv"
    _write_csv(erc20_path, erc20_records, erc20_fields)

    internal_list = fetch_internal_txs(
        api_url,
        address,
        api_key=api_key,
        start_block=start_block,
        end_block=end_block,
        max_retries=max_retries,
        page_size=ps,
        evm_chain_id=evm_chain_id,
        log_label=f"{chain_id}/internal",
    )
    print(f"  internal transfers: {len(internal_list)}")
    internal_records = [_internal_row_to_csv_record(t, address) for t in internal_list]
    internal_fields = [
        "Transaction Hash", "Blockno", "UnixTimestamp", "DateTime (UTC)",
        "From", "To", "TxTo", "Value_IN(ETH)", "Value_OUT(ETH)", "Type",
    ]
    internal_path = output_dir / "internal.csv"
    _write_csv(internal_path, internal_records, internal_fields)

    total_raw = len(normal_list) + len(erc20_list) + len(internal_list)
    print(f"  files written: normal.csv, erc20.csv, internal.csv")
    print(f"  [INGEST] raw rows total (normal+erc20+internal): {total_raw}")
    if total_raw == 0:
        print(
            "  [INGEST] NOTE: 0 rows — may be an inactive wallet, or API returned empty lists "
            "(if this address should have history, check API key / network)."
        )

    return {
        "normal": normal_path,
        "erc20": erc20_path,
        "internal": internal_path,
        "counts": {
            "normal": len(normal_list),
            "erc20": len(erc20_list),
            "internal": len(internal_list),
        },
        "total_raw": total_raw,
        "source": source_label,
        "api_url": api_url,
        "api_status": "OK",
    }


def download_chain(
    chain_id: str,
    address: str,
    output_dir: Path,
    api_key: Optional[str] = None,
    start_block: int = 0,
    end_block: int = 99999999,
    max_retries: int = MAX_RETRIES,
) -> Dict[str, Any]:
    """
    Download normal, ERC20, and internal transactions for one chain and write
    normal.csv, erc20.csv, internal.csv into output_dir.

    Requires an Etherscan-compatible API key for the configured explorer (see CHAIN_CONFIG),
    except for Ethereum mainnet where a Blockscout public API fallback may be used if the
    primary call fails or no key is set.

    Returns dict with paths, counts, total_raw, source, api_status.
    """
    chain_id = chain_id.strip().lower()
    address = address.strip().lower()
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    cfg = CHAIN_CONFIG.get(chain_id)
    if not cfg:
        err = (
            f"Unsupported chain_id: {chain_id!r}. "
            f"Supported: {', '.join(SUPPORTED_CHAIN_IDS)}"
        )
        print(f"[etherscan_fetcher] ERROR: {err}")
        raise ValueError(err)
    primary_url = cfg.get("api") or ""
    if not primary_url:
        raise ValueError(f"Chain {chain_id!r} has no 'api' endpoint in CHAIN_CONFIG.")
    evm_primary_id = get_evm_chain_numeric_id(chain_id)

    print(f"Chain: {chain_id} (Etherscan API V2 chainid={evm_primary_id}, base={primary_url})")
    key = (api_key or "").strip() or None

    def _try_blockscout_eth(reason: str) -> Dict[str, Any]:
        warn_msg = (
            "INGEST FALLBACK WARNING: Using Ethereum Blockscout public API instead of "
            f"Etherscan API V2 (chainid=1). Reason: {reason}. "
            "Data source and coverage may differ from Etherscan; this fallback is explicit, not silent."
        )
        logger.warning(warn_msg)
        print(f"[WARNING] {warn_msg}")
        try:
            out = _write_chain_csvs(
                BLOCKSCOUT_ETH_API,
                chain_id,
                address,
                output_dir,
                None,
                source_label="blockscout_eth",
                evm_chain_id=None,
                start_block=start_block,
                end_block=end_block,
                max_retries=max_retries,
            )
            print("[INGEST] Blockscout fallback succeeded.")
            return out
        except Exception as e:
            print(f"[INGEST] Blockscout fallback failed: {e!r}")
            raise

    try:
        if not key:
            if chain_id == "eth":
                print(
                    "[INGEST] No API key — trying Ethereum Blockscout public API "
                    "(primary Etherscan API requires an API key; register at https://etherscan.io/apis)."
                )
                try:
                    return _try_blockscout_eth("no API key")
                except Exception:
                    print(
                        "[INGEST] NO DATA SOURCE AVAILABLE — set ETHERSCAN_API_KEY or pass api_key "
                        "to the explorer fetcher; Blockscout fallback also failed."
                    )
                    raise MissingExplorerAPIKeyError(
                        "Set ETHERSCAN_API_KEY (or chain-specific key) or pass --api-key. "
                        "Etherscan-family APIs require a key. Ethereum Blockscout fallback failed."
                    ) from None
            print(
                f"[INGEST] NO DATA SOURCE AVAILABLE — chain {chain_id!r} has no public fallback; "
                "an API key is required."
            )
            raise MissingExplorerAPIKeyError(
                f"Explorer API key required for chain {chain_id!r}. "
                "Set ETHERSCAN_API_KEY or pass --api-key (see chain docs for your explorer)."
            )

        out = _write_chain_csvs(
            primary_url,
            chain_id,
            address,
            output_dir,
            key,
            source_label="primary_explorer",
            evm_chain_id=evm_primary_id,
            start_block=start_block,
            end_block=end_block,
            max_retries=max_retries,
        )
        return out
    except MissingExplorerAPIKeyError:
        raise
    except ExplorerIngestError as e:
        if chain_id == "eth" and key:
            try:
                return _try_blockscout_eth(f"primary explorer error: {e}")
            except Exception:
                print("[INGEST] NO DATA SOURCE AVAILABLE for eth after primary + Blockscout failure.")
                raise
        friendly = _format_download_error(e)
        print(f"[etherscan_fetcher] ERROR: {friendly}")
        raise
    except Exception as e:
        if chain_id == "eth" and key:
            try:
                return _try_blockscout_eth(f"primary explorer exception: {e!r}")
            except Exception:
                print("[INGEST] NO DATA SOURCE AVAILABLE for eth after primary + Blockscout failure.")
                raise
        friendly = _format_download_error(e)
        print(f"[etherscan_fetcher] ERROR: {friendly}")
        raise
