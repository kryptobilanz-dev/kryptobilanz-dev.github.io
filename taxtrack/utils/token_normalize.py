"""
Normalize token symbols for RawRow: remove scam suffixes, Cyrillic lookalikes, ensure non-empty.
Used by EVM loaders before creating RawRow.
"""

import re


def normalize_token_symbol(raw: str) -> str:
    """
    Clean a token symbol for storage and validation.

    - Strip and upper-case
    - Take first word only (drops scam suffixes like "STETH [WWW.STETH.VIP]" or "USDC | T.ME/...")
    - Preserve common on-chain symbol separators used by LP receipt tokens and bridged tokens:
      '-' and '.' (e.g. "UNI-V2", "Cake-LP", "USDCE.E")
    - Remove any other character that is not A-Z, 0-9, underscore, '-' or '.'
    - If result is empty, return "UNKNOWN"
    """
    if raw is None:
        return "UNKNOWN"
    token = str(raw).strip().upper()
    token = token.split()[0] if token else ""
    token = re.sub(r"[^A-Z0-9_.-]", "", token)
    return token if token else "UNKNOWN"
