# taxtrack/utils/gas.py
# ZenTaxCore – Einheitlicher GasFee Normalizer für ALLE EVM Loader

from taxtrack.utils.num import to_float


def unify_gas_fee(line: dict, chain_info: dict):
    """
    Liefert (fee_token, fee_amount) für jede Loader-Zeile.

    Priorität:
    1) Explizite Fee-Felder wie TxnFee, TxnFee(ETH)
    2) Berechnung über gasUsed * gasPrice/effectiveGasPrice
    3) Fallback: 0.0

    line: dict einer CSV-Zeile
    chain_info: dict aus CHAIN_CONFIG[chain_id]
    """

    native = chain_info.get("native_symbol", "ETH")

    # -------------------------------------
    # 1️⃣ Explizite Fee in CSV vorhanden?
    # -------------------------------------
    fee_fields = [
        "TxnFee",
        "TxnFee(ETH)",
        "TxnFee(BNB)",
        "TxnFee(FTM)",
        "TxnFee(MATIC)",
        "TxnFee(AVAX)",
        "Fee",
        "fee"
    ]

    for f in fee_fields:
        if f in line and line[f]:
            try:
                return native, to_float(line[f])
            except:
                pass

    # -------------------------------------
    # 2️⃣ GasUsed × (gasPrice/effectiveGasPrice)
    # -------------------------------------
    gas_used = line.get("gasUsed") or line.get("GasUsed") or None
    gas_price = (
        line.get("effectiveGasPrice")
        or line.get("gasPrice")
        or line.get("GasPrice")
    )

    if gas_used and gas_price:
        try:
            gas_used = float(gas_used)
            gas_price = float(gas_price)
            fee_amount = (gas_used * gas_price) / 1e18
            return native, fee_amount
        except:
            pass

    # -------------------------------------
    # 3️⃣ Fallback – keine Fee
    # -------------------------------------
    return native, 0.0
