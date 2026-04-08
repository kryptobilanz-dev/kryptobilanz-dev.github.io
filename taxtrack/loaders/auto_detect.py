from __future__ import annotations
from pathlib import Path
import csv


def _extract_chain_id_from_path(path: Path) -> str:
    parts = path.parts
    chains = {"eth", "arb", "op", "base", "bnb", "matic", "ftm", "avax"}
    for p in parts:
        pl = p.lower()
        if pl in chains:
            return pl
    return "eth"


def detect_loader(path: Path) -> str:
    """
    Endgültige Erkennung für:
      - Coinbase PDF
      - EVM normal
      - EVM internal
      - EVM ERC20
      - Fallback generic
    """

    # ------------------------------------------------------
    # 0) PDF Detection
    # ------------------------------------------------------
    if path.suffix.lower() == ".pdf":
        try:
            import pdfplumber
            with pdfplumber.open(path) as pdf:
                first = (pdf.pages[0].extract_text() or "").lower()
            if "transaction history report" in first and "coinbase" in first:
                return "coinbase_pdf"
        except Exception:
            # fällt unten auf generic zurück
            pass

    # ------------------------------------------------------
    # 1) Datei lesen
    # ------------------------------------------------------
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        text = path.read_text(errors="ignore")

    lines = [l.strip() for l in text.splitlines() if l.strip()]
    if not lines:
        return "generic"

    header = lines[0].lower()

    # ------------------------------------------------------
    # 2) INTERNAL (muss VOR normal kommen!)
    #    Erkennung über typische Trace-Felder
    # ------------------------------------------------------
    internal_trace_fields = [
        "parenttxfrom",
        "parenttxto",
        "parenttxeth_value",
        "traceaddress",
        "traceaddress[]",
        "calltype",
        "type"  # bei deinen Beispielen: "call"
    ]
    if any(k in header for k in internal_trace_fields):
        return "evm_internal"

    # ------------------------------------------------------
    # 3) ERC20 (Token-Transfers)
    # ------------------------------------------------------
    if any(k in header for k in ["tokensymbol", "tokenvalue", "tokenname", "token name"]):
        return "evm_erc20"

    # ------------------------------------------------------
    # 4) NORMAL (native / mixed)
    # ------------------------------------------------------
    if (
        "transaction hash" in header
        and (
            "value_in(eth)" in header
            or "value_out(eth)" in header
            or "value_in" in header
            or "value_out" in header
        )
    ):
        return "evm_normal"

    # ------------------------------------------------------
    # 5) Coinbase CSV (Transaction Type + Timestamp + Asset)
    # ------------------------------------------------------
    if "transaction type" in header and "timestamp" in header and "asset" in header:
        return "coinbase"

    # ------------------------------------------------------
    # 6) Fallback
    # ------------------------------------------------------
    return "generic"


def load_auto(path: Path, wallet: str, chain_id: str = None, allow_coinbase_csv: bool = False):
    """
    Haupt-Router: wählt passenden Loader und gibt RawRow-Liste zurück.
    Der Parameter chain_id überschreibt die automatische Chain-Erkennung.
    """
    # Falls chain_id nicht gesetzt → aus Pfad extrahieren
    if chain_id is None:
        chain_id = _extract_chain_id_from_path(path)

    loader = detect_loader(path)

    print(f"[auto_detect] {path.name}: loader={loader}, chain={chain_id}")

    # Coinbase PDF
    if loader == "coinbase_pdf":
        from taxtrack.loaders.coinbase.pdf_loader import load_coinbase_pdf
        return load_coinbase_pdf(path, wallet=wallet or "")

    # Coinbase Rewards CSV
    if loader == "coinbase_rewards":
        if not allow_coinbase_csv:
            print(f"[auto_detect] IGNORE Coinbase Rewards CSV: {path.name}")
            return []
        from taxtrack.loaders.coinbase.rewards_loader import load_coinbase_rewards
        return load_coinbase_rewards(path, wallet=wallet or "")

    # Coinbase Normal CSV
    if loader == "coinbase":
        if not allow_coinbase_csv:
            print(f"[auto_detect] IGNORE Coinbase Transactions CSV: {path.name}")
            return []
        from taxtrack.loaders.coinbase.loader import load_coinbase
        return load_coinbase(path, wallet=wallet or "")

    # EVM Normal
    if loader == "evm_normal":
        from taxtrack.loaders.etherscan.normal_loader import load_etherscan
        return load_etherscan(path, wallet, chain_id)

    # EVM Internal
    if loader == "evm_internal":
        from taxtrack.loaders.etherscan.internal_loader import load_internal_etherscan
        return load_internal_etherscan(path, wallet, chain_id)

    # EVM ERC20
    if loader == "evm_erc20":
        from taxtrack.loaders.etherscan.erc20_loader import load_erc20
        return load_erc20(path, wallet, chain_id)

    # Fallback
    from taxtrack.loaders.generic.generic_loader import load_generic
    return load_generic(path, wallet, chain_id=chain_id or "generic")
