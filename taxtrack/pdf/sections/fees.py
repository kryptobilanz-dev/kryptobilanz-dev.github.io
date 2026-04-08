from typing import Any, Dict, List
from reportlab.platypus import Paragraph, Spacer, PageBreak
from taxtrack.pdf.utils import make_table

def section_fees(records: List[Any], styles, fee_amount_of, fee_origin_of):
    story = []
    h1 = styles["Heading1"]

    fee_stats: Dict[str, Dict[str, float | int]] = {}
    for r in records:
        fa = float(fee_amount_of(r) or 0.0)
        if fa == 0:
            continue
        fo = (fee_origin_of(r) or "unknown").lower()
        st = fee_stats.setdefault(fo, {"count": 0, "sum": 0.0})
        st["count"] += 1
        st["sum"] += fa

    if not fee_stats:
        return story

    story.append(Paragraph("<b>Gebührenanalyse</b>", h1))
    story.append(Spacer(1, 10))

    rows = [{"type": k, "count": int(v["count"]), "sum_fee": float(v["sum"])} for k, v in sorted(fee_stats.items())]
    make_table("Fees nach Typ", rows, ["type", "count", "sum_fee"], styles, story)
    story.append(PageBreak())
    return story
