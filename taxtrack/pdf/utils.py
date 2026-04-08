# taxtrack/pdf/utils.py

from __future__ import annotations
from typing import Any, Dict, Iterable, List
from xml.sax.saxutils import escape

from reportlab.platypus import Paragraph, Spacer, Table, LongTable, TableStyle
from reportlab.lib import colors
from reportlab.lib.styles import ParagraphStyle


def as_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(x)
    except Exception:
        return default


def get(item: Any, key: str, default: Any = None) -> Any:
    """Robust für dicts & Objekte (ClassifiedItem)."""
    if hasattr(item, key):
        return getattr(item, key)

    if isinstance(item, dict):
        if key in item:
            return item.get(key, default)
        if key == "from":
            return item.get("from_addr", default)
        if key == "to":
            return item.get("to_addr", default)
        if key == "fee_origin":
            meta = item.get("meta") or {}
            return meta.get("fee_origin", default)

    if key == "from" and hasattr(item, "from_addr"):
        return getattr(item, "from_addr")
    if key == "to" and hasattr(item, "to_addr"):
        return getattr(item, "to_addr")

    return default


def fmt_eur(value: float) -> str:
    return f"{value:,.2f} €".replace(",", " ").replace(".", ",")


def short_hash(tx_hash: str, length: int = 10) -> str:
    if not tx_hash:
        return ""
    if len(tx_hash) <= length:
        return tx_hash
    return tx_hash[:length] + "…"


# Interne Spalten-Schlüssel → lesbare PDF-Überschriften (nur bekannte Keys übersetzen).
PDF_TABLE_HEADER_DE: Dict[str, str] = {
    "datetime": "Datum / Zeit",
    "kategorie": "Vorgang",
    "token": "Token",
    "brutto_eur": "Brutto (€)",
    "gebuehren_eur": "Gebühren (€)",
    "netto_eur": "Netto (€)",
    "bis_365d_eur": "Netto bis 365 Tage (€)",
    "ueber_365d_eur": "Netto über 365 Tage (€)",
    "kurzfristig": "Kurzfristig steuerpflichtig",
    "halt_min": "Haltedauer min (Tage)",
    "halt_max": "Haltedauer max (Tage)",
    "konfidenz": "Preis-Konfidenz",
    "preis_quelle": "Preis-Quelle",
    "quelle_tx": "Transaktion",
    "quelle_zeilen": "Anz. Quellzeilen",
    "kurz_erklärung": "Erläuterung",
    "display_category": "Vorgang",
    "pnl_brutto": "Brutto (€)",
    "fees": "Gebühren (€)",
    "pnl_netto": "Netto (€)",
    "taxable": "Kurzfristiger Bucket (steuerpflichtig)",
    "tx": "Tx (Kurz)",
    "lp_position": "Position",
    "eur_value": "EUR-Wert",
    "category": "Kategorie",
    "counterparty": "Gegenpartei",
    "amount": "Menge",
    "type": "Typ",
    "count": "Anzahl",
    "sum_fee": "Summe Gebühr",
}


def pdf_table_header_label(column_key: str) -> str:
    """Überschrift für die erste Tabellenzeile; unbekannte Keys unverändert (z. B. schon deutsch)."""
    return PDF_TABLE_HEADER_DE.get(column_key, column_key)


def group_lp_gains(records):
    """Gruppiert LP-Burns (category == 'lp_remove') nach tx_hash."""
    by_tx = {}
    for r in records:
        if (get(r, "category", "") or "").lower() != "lp_remove":
            continue
        txh = get(r, "tx_hash", "")
        if not txh:
            continue
        by_tx.setdefault(txh, []).append(r)
    return by_tx


def make_table(
    title: str,
    rows: Iterable[Dict[str, Any]] | Iterable[Any],
    columns: List[str],
    styles,
    story: List[Any],
    max_rows: int | None = None,
    empty_text: str = "Keine Einträge",
):
    """Tabellenaufbau für dicts & Objekte."""
    rows = list(rows)
    story.append(Paragraph(f"<b>{title}</b>", styles["Heading2"]))
    story.append(Spacer(1, 6))

    if not rows:
        story.append(Paragraph(empty_text, styles["BodyText"]))
        story.append(Spacer(1, 12))
        return

    data = [[pdf_table_header_label(c) for c in columns]]
    count = 0

    for r in rows:
        row = []
        for col in columns:
            if isinstance(r, dict):
                val = r.get(col)
            else:
                val = get(r, col, "")
            if isinstance(val, float):
                val = f"{val:,.6f}".replace(",", " ").replace(".", ",")
            elif val is None:
                val = ""
            row.append(str(val))
        data.append(row)

        count += 1
        if max_rows is not None and count >= max_rows:
            break

    tbl_cls = LongTable if len(data) > 1 else Table
    table = tbl_cls(data, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dbeafe")),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
            ]
        )
    )
    story.append(table)
    story.append(Spacer(1, 14))


def _make_audit_cell(val: Any, wrap_style: ParagraphStyle) -> Paragraph:
    if isinstance(val, float):
        s = f"{val:,.6f}".replace(",", " ").replace(".", ",")
    elif val is None:
        s = ""
    elif isinstance(val, (list, dict)):
        s = str(val)[:500]
    else:
        s = str(val)
    if len(s) > 1800:
        s = s[:1797] + "…"
    return Paragraph(escape(s).replace("\n", "<br/>"), wrap_style)


def make_table_with_row_backgrounds(
    title: str,
    rows: List[Dict[str, Any]],
    columns: List[str],
    styles,
    story: List[Any],
    bg_key: str = "audit_row_bg",
    max_rows: int | None = None,
    empty_text: str = "Keine Einträge",
    col_widths: List[float] | None = None,
    wrap_cells: bool = False,
):
    """
    Like make_table, but applies a per-row background color from rows[i][bg_key] (hex, e.g. #dcfce7).
    If wrap_cells=True, cells use Paragraph (AuditTableCell style) for word wrap.
    """
    story.append(Paragraph(f"<b>{title}</b>", styles["Heading2"]))
    story.append(Spacer(1, 6))

    rows = list(rows)
    if not rows:
        story.append(Paragraph(empty_text, styles["BodyText"]))
        story.append(Spacer(1, 12))
        return

    wrap_style = styles.get("AuditTableCell") or styles["BodyText"] if wrap_cells else None

    header_labels = [pdf_table_header_label(c) for c in columns]
    header = [
        Paragraph(f"<b>{escape(lab)}</b>", wrap_style) if wrap_cells else lab for lab in header_labels
    ]
    data: List[List[Any]] = [header]
    bg_colors: List[Any] = [None]
    count = 0

    for r in rows:
        row = []
        for col in columns:
            if isinstance(r, dict):
                val = r.get(col)
            else:
                val = get(r, col, "")
            if wrap_cells and wrap_style is not None:
                row.append(_make_audit_cell(val, wrap_style))
            else:
                if isinstance(val, float):
                    val = f"{val:,.6f}".replace(",", " ").replace(".", ",")
                elif val is None:
                    val = ""
                elif isinstance(val, (list, dict)):
                    val = str(val)[:500]
                row.append(str(val))
        data.append(row)
        bg_colors.append(r.get(bg_key) if isinstance(r, dict) else None)

        count += 1
        if max_rows is not None and count >= max_rows:
            break

    table_kw: Dict[str, Any] = {"repeatRows": 1}
    if col_widths:
        table_kw["colWidths"] = col_widths

    tbl_cls = LongTable if len(data) > 1 else Table
    table = tbl_cls(data, **table_kw)
    valign = "TOP" if wrap_cells else "MIDDLE"
    ts: List[Any] = [
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#dbeafe")),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), valign),
    ]
    if not wrap_cells:
        ts.append(("FONTSIZE", (0, 0), (-1, -1), 7))
    for ri in range(1, len(data)):
        bg = bg_colors[ri]
        if bg:
            try:
                ts.append(("BACKGROUND", (0, ri), (-1, ri), colors.HexColor(bg)))
            except Exception:
                pass
    table.setStyle(TableStyle(ts))
    story.append(table)
    story.append(Spacer(1, 14))
