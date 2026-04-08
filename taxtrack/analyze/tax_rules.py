# taxtrack/analyze/tax_rules.py

from __future__ import annotations

from taxtrack.rules.taxlogic import TaxLogic

_logic = TaxLogic("de")


def calc_holding_days(ts_buy: int, ts_sell: int) -> int:
    """Sekunden → Tage, nie negativ."""
    if not ts_buy or not ts_sell:
        return 0
    diff = ts_sell - ts_buy
    if diff <= 0:
        return 0
    return diff // 86400  # ganze Tage


def classify_tax_type(category: str) -> str:
    """
    Liefert den 'type' aus taxlogic_de.json, z.B.
    'Privates Veräußerungsgeschäft' oder 'Einkünfte aus sonstigen Leistungen'.
    """
    rule = _logic.get_rule(category or "")
    return rule.get("type") or "Unbekannt"


def taxable_status(tax_type: str, hold_days: int) -> bool:
    """
    Entscheidet, ob ein Vorgang steuerpflichtig ist.
    - PVG (§23 EStG): innerhalb 1 Jahr steuerpflichtig, danach steuerfrei
    - Einkünfte (§22 EStG): immer steuerpflichtig
    - sonst: konservativ steuerfrei
    """
    t = (tax_type or "").lower()

    # Private Veräußerungsgeschäfte (§23)
    if "privates veräußerungsgeschäft" in t:
        return hold_days <= 365

    # Einkünfte aus Leistungen / sonstige Einkünfte (§22)
    if "einkünfte" in t or "sonstige einkünfte" in t:
        return True

    # Default: eher steuerfrei
    return False
