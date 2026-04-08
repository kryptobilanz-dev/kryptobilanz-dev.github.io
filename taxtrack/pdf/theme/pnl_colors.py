# taxtrack/pdf/theme/pnl_colors.py
# Einheitliche Farben für Nettoergebnis (Gewinn / Verlust / neutral) im PDF.

from __future__ import annotations

from typing import Any, Dict

EPS = 1e-6

# Zeilen-Hintergrund (sanft, gut lesbar mit schwarzer Schrift)
ROW_BG_GAIN = "#dcfce7"  # green-100
ROW_BG_LOSS = "#fee2e2"  # red-100
ROW_BG_NEUTRAL = "#f1f5f9"  # slate-100

_SWAP_NEUTRAL_ABS_EUR = 10.0
_SWAP_NEUTRAL_REL = 0.01

# KPI-Boxen: Hintergrund + Rahmen
KPI_BG_GAIN = "#ecfdf5"
KPI_BORDER_GAIN = "#10b981"
KPI_BG_LOSS = "#fef2f2"
KPI_BORDER_LOSS = "#ef4444"
KPI_BG_NEUTRAL = "#f8fafc"
KPI_BORDER_NEUTRAL = "#94a3b8"


def row_bg_for_net_gain(gain: float) -> str:
    """Hintergrundfarbe für eine Tabellenzeile nach Nettoergebnis (gain / Netto €)."""
    try:
        g = float(gain)
    except (TypeError, ValueError):
        g = 0.0
    if g > EPS:
        return ROW_BG_GAIN
    if g < -EPS:
        return ROW_BG_LOSS
    return ROW_BG_NEUTRAL


def pnl_tier(gain: float) -> str:
    """'gain' | 'loss' | 'neutral' für Styling."""
    try:
        g = float(gain)
    except (TypeError, ValueError):
        g = 0.0
    if g > EPS:
        return "gain"
    if g < -EPS:
        return "loss"
    return "neutral"


def row_bg_for_tax_row(row: Dict[str, Any]) -> str:
    """
    Zeilenfarbe für tax-ready Zeilen.

    - Nicht-Swap: wie bisher nach Vorzeichen des Netto (gain).
    - Swap: Grün/Rot nur, wenn das Netto **nennenswert** ist (absolut oder relativ zu
      Anschaffung/Erlös). Kleine FIFO-/Kursrundungs-Differenzen bleiben **neutral** –
      ein Swap ist steuerlich eine Realisierung, aber nicht automatisch ein „großer Gewinn“
      in der optischen Hervorhebung.
    """
    try:
        gain = float(row.get("gain") if row.get("gain") is not None else 0.0)
    except (TypeError, ValueError):
        gain = 0.0
    cat = (row.get("category") or "").lower()
    if cat != "swap":
        return row_bg_for_net_gain(gain)
    try:
        cb = abs(float(row.get("cost_basis") or 0.0))
        pr = abs(float(row.get("proceeds") or 0.0))
    except (TypeError, ValueError):
        cb, pr = 0.0, 0.0
    scale = max(cb, pr, 1.0)
    thr = max(_SWAP_NEUTRAL_ABS_EUR, _SWAP_NEUTRAL_REL * scale)
    if abs(gain) <= thr:
        return ROW_BG_NEUTRAL
    return row_bg_for_net_gain(gain)
