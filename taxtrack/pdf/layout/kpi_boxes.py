from __future__ import annotations

from reportlab.platypus import Table, TableStyle, Paragraph
from reportlab.lib import colors

from taxtrack.pdf.theme.pnl_colors import (
    KPI_BG_GAIN,
    KPI_BORDER_GAIN,
    KPI_BG_LOSS,
    KPI_BORDER_LOSS,
    KPI_BG_NEUTRAL,
    KPI_BORDER_NEUTRAL,
)


def kpi_box(title: str, value: str, styles, tone: str | None = None):
    """
    KPI-Kachel. tone: 'gain' | 'loss' | 'neutral' | None — färbt Hintergrund und Rahmen
    nach Netto-Vorzeichen (Gewinn / Verlust / ~0).
    """
    if tone == "gain":
        bg, border = KPI_BG_GAIN, KPI_BORDER_GAIN
    elif tone == "loss":
        bg, border = KPI_BG_LOSS, KPI_BORDER_LOSS
    elif tone == "neutral":
        bg, border = KPI_BG_NEUTRAL, KPI_BORDER_NEUTRAL
    else:
        bg, border = colors.white, colors.grey

    return Table(
        [
            [Paragraph(f"<b>{title}</b>", styles["KPILabel"])],
            [Paragraph(f"<b>{value}</b>", styles["KPIValue"])],
        ],
        colWidths=[120],
        rowHeights=[20, 28],
        style=TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), bg),
                ("BOX", (0, 0), (-1, -1), 1.0 if tone else 0.5, border),
                ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("TOPPADDING", (0, 0), (-1, -1), 6),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
            ]
        ),
    )


def kpi_row(kpis):
    return Table([[kpis]])
