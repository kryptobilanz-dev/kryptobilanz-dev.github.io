# taxtrack/loaders/etherscan/erc20_loader.py
# ZenTaxCore – Multi-Chain ERC20 Loader (Etherscan / Polygonscan / Arbiscan / etc.)

import csv
import io
from pathlib import Path
from decimal import Decimal, InvalidOperation
from dateutil import parser as dtparser

from taxtrack.schemas.RawRow import RawRow
from taxtrack.utils.time import iso_from_unix
from taxtrack.utils.num import to_float
from taxtrack.utils.token_normalize import normalize_token_symbol
from taxtrack.utils.direction import derive_direction, assert_direction_derivation
from taxtrack.utils.csv_utils import get_field
from taxtrack.validation.raw_row import validate_raw_row, DEBUG_VALIDATION
from taxtrack.loaders.generic.generic_loader import _read_file_text_auto
from taxtrack.data.config.chain_config import CHAIN_CONFIG
from taxtrack.prices.token_mapper import map_token

ZERO_ADDR = "0x0000000000000000000000000000000000000000"

def _looks_like_lp_receipt_symbol(sym: str) -> bool:
    """
    High-confidence LP/vault receipt symbol patterns for debug logging only.
    We intentionally keep this strict to avoid noisy logs (many protocols mint/burn non-LP tokens).
    """
    s = (sym or "").strip().upper()
    if not s or s in {"UNKNOWN", "<EMPTY>"}:
        return False
    # common receipt patterns
    if "UNI-V2" in s or "SLP" in s or "CAKE-LP" in s:
        return True
    if s.startswith("MOO") or "BEEFY" in s:
        return True
    if "LP" in s and len(s) >= 4:
        return True
    if "PAIR" in s and len(s) >= 7 and s != "PAIR":
        return True
    return False


# Standard-Decimals – human readable already
DECIMALS_MAP = {
    "ETH": 18, "WETH": 18,
    "USDT": 6, "USDC": 6,
    "DAI": 18, "FTM": 18,
    "PENDLE": 18, "PENDLE_LPT": 18,
    "MON": 18, "TARA": 18,
    "MATIC": 18, "WMATIC": 18,
}

def _normalize_amount(raw_val: str, token: str):
    raw_str = (raw_val or "0").strip()

    try:
        dec = Decimal(raw_str.replace(",", "."))
    except InvalidOperation:
        dec = Decimal("0")

    token_base = map_token(token)
    decimals = DECIMALS_MAP.get(token_base, 18)

    return float(dec), raw_str, decimals


def load_erc20(path: Path, wallet: str, chain_id: str = "eth"):
    """
    Universeller ERC20 Loader für alle EVM-Chains.
    Unterstützt Etherscan / Polygonscan / Arbiscan / BSCSCAN Format.
    """

    chain_info = CHAIN_CONFIG.get(chain_id, CHAIN_CONFIG["eth"])
    native_symbol = chain_info.get("native_symbol", "ETH")

    text = _read_file_text_auto(path)
    f = io.StringIO(text)
    reader = csv.DictReader(f)

    rows = []

    wallet_l = (wallet or "").strip().lower()

    for line in reader:
        # ---------------------------------------------------
        # 1. Timestamp
        # ---------------------------------------------------
        ts_raw = (line.get("UnixTimestamp") or "").strip()
        if ts_raw.isdigit():
            ts = int(ts_raw)
        else:
            dt_str = (
                line.get("DateTime (UTC)")
                or line.get("DateTime")
                or line.get("Date")
                or ""
            )
            ts = int(dtparser.parse(dt_str).timestamp()) if dt_str else 0

        dt_iso = iso_from_unix(ts) if ts > 0 else ""
        if ts <= 0:
            continue  # skip rows without valid timestamp (dt_iso would be empty; validation requires it)

        # ---------------------------------------------------
        # 2. Addresses (case-insensitive: Etherscan uses "From"/"To")
        # ---------------------------------------------------
        from_addr = get_field(line, "From", "from").strip().lower()
        to_addr = get_field(line, "To", "to").strip().lower()
        direction = derive_direction(wallet, from_addr, to_addr)

        # ---------------------------------------------------
        # 3. Token & Value
        # ---------------------------------------------------
        raw_token = (
            line.get("TokenSymbol")
            or line.get("symbol")
            or line.get("Token")
            or ""
        ).strip()
        raw_token_clean = normalize_token_symbol(raw_token)
        token = map_token(raw_token_clean) or "UNKNOWN"

        raw_val = (
            line.get("TokenValue")
            or line.get("value")
            or line.get("Amount")
            or "0"
        )

        amount, amount_raw, decimals = _normalize_amount(raw_val, token)

        if amount <= 0:
            continue  # skip zero token movement (approve, call, multicall noise)

        # ---------------------------------------------------
        # 4. Contract
        # ---------------------------------------------------
        contract_address = (line.get("ContractAddress") or "").lower().strip()

        # ---------------------------------------------------
        # 5. Tx Hash
        # ---------------------------------------------------
        tx_hash = (
            line.get("Transaction Hash")
            or line.get("Txn Hash")
            or line.get("Txhash")
            or line.get("Hash")
            or ""
        ).strip()

        if not tx_hash:
            continue

        # ---------------------------------------------------
        # 6. RawRow erzeugen
        # ---------------------------------------------------
        row = RawRow(
            source="evm_erc20",
            tx_hash=tx_hash,
            timestamp=ts,
            dt_iso=dt_iso,
            from_addr=from_addr,
            to_addr=to_addr,
            token=token,
            amount=amount,
            direction=direction,
            method="ERC20_TRANSFER",
            amount_raw=amount_raw,
            decimals=decimals,
            contract_addr=contract_address,
            fee_token=native_symbol,
            fee_amount=0.0,
            category="erc20_transfer",
            chain_id=chain_id,
            meta={
                "source_file": str(path),
                "raw_token": raw_token_clean,
                "chain_id": chain_id,
                "loader": "evm_erc20",
            },
        )
        assert_direction_derivation(row, wallet)
        if DEBUG_VALIDATION:
            validate_raw_row(row)
        rows.append(row)

        # Debug only for real on-chain LP receipt mint/burn transfers (zero-address).
        # Mint: from zero -> wallet (direction in)
        # Burn: wallet -> zero (direction out)
        if _looks_like_lp_receipt_symbol(token):
            if from_addr == ZERO_ADDR and to_addr == wallet_l and direction == "in":
                print("[LP RAW DETECTED]")
                print(f"tx_hash={tx_hash}")
                print(f"token={token}")
                print("direction=in")
                print(f"amount={amount}")
            elif to_addr == ZERO_ADDR and from_addr == wallet_l and direction == "out":
                print("[LP RAW DETECTED]")
                print(f"tx_hash={tx_hash}")
                print(f"token={token}")
                print("direction=out")
                print(f"amount={amount}")

    return rows
