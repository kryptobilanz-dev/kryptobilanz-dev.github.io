# taxtrack/pdf/theme/tables.py
from reportlab.platypus import TableStyle
from taxtrack.pdf.theme import colors as C

BASE_TABLE = TableStyle(
    [
        ("GRID", (0, 0), (-1, -1), 0.25, C.BORDER),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
    ]
)

HEADER_TABLE = TableStyle(
    [
        ("BACKGROUND", (0, 0), (-1, 0), C.BG_SOFT),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ]
)
