from typing import Any, List
from reportlab.platypus import Paragraph, Spacer, PageBreak
from taxtrack.pdf.utils import group_lp_gains, get, short_hash, make_table

def section_lp(records: List[Any], styles, eur_of, normal_style):
    story = []
    h1 = styles["Heading1"]

    lp_groups = group_lp_gains(records)
    if not lp_groups:
        return story

    story.append(Paragraph("<b>Liquiditätspools (AMM)</b>", h1))
    story.append(Spacer(1, 10))

    lp_rows = []
    for txh, rows in lp_groups.items():
        total_eur = sum(eur_of(r) for r in rows if eur_of(r) > 0)
        lp_rows.append(
            {
                "datetime": get(rows[0], "dt_iso", ""),
                "tx": short_hash(txh),
                "lp_position": get(rows[0], "token", ""),
                "eur_value": total_eur,
                "category": "lp_remove",
                "counterparty": get(rows[0], "counterparty", ""),
            }
        )

    make_table(
        "LP-Burns (Auflösung von Liquiditätspool-Positionen)",
        lp_rows,
        ["datetime", "tx", "lp_position", "eur_value", "category", "counterparty"],
        styles,
        story,
    )

    story.append(
        Paragraph(
            "Hinweis: Bei AMM-Liquiditätspools wird steuerlich ausschließlich die "
            "Auflösung der LP-Position als Veräußerung behandelt. Die erhaltenen "
            "Token gelten als neue Anschaffung.",
            normal_style,
        )
    )
    story.append(PageBreak())
    return story
