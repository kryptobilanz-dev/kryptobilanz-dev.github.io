# taxtrack/pdf/theme/typography.py
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER

def apply_typography(styles):
    styles["Heading1"].fontSize = 16
    styles["Heading1"].leading = 20
    styles["Heading1"].spaceAfter = 10

    styles["Heading2"].fontSize = 12
    styles["Heading2"].leading = 15
    styles["Heading2"].spaceAfter = 6

    styles["BodyText"].fontSize = 9
    styles["BodyText"].leading = 12

    styles.add(
        ParagraphStyle(
            name="KPIValue",
            parent=styles["Heading2"],
            alignment=TA_CENTER,
        )
    )

    styles.add(
        ParagraphStyle(
            name="KPILabel",
            parent=styles["BodyText"],
            alignment=TA_CENTER,
        )
    )

    styles.add(
        ParagraphStyle(
            name="AuditTableCell",
            parent=styles["BodyText"],
            fontSize=6,
            leading=7.5,
            spaceBefore=0,
            spaceAfter=0,
            leftIndent=0,
            rightIndent=0,
        )
    )

    return styles
