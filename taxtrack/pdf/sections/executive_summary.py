# taxtrack/pdf/sections/executive_summary.py

from reportlab.platypus import Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from taxtrack.pdf.layout.kpi_boxes import kpi_box, kpi_row
from taxtrack.pdf.theme.pnl_colors import pnl_tier


def _kpi_tone(raw: float | None) -> str | None:
    """None = Standard-Kachel; sonst gain/loss/neutral nach Vorzeichen."""
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return pnl_tier(v)


def section_executive_summary(summary, aggregates, styles, tax_summary=None):
    """
    Executive summary. If tax_summary is set, §23 split matches tax_interpreter / backend.
    """

    story = []

    h1 = styles["Heading1"]
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    story.append(Paragraph("<b>Executive Summary – Steuerübersicht</b>", h1))
    story.append(Spacer(1, 14))

    wallet = summary.get("wallet", "")
    chain = summary.get("chain_id", "")
    year = summary.get("year", "")
    period = summary.get("period", "")

    context_parts = []
    if wallet:
        context_parts.append(f"Wallet <b>{wallet}</b>")
    if chain:
        context_parts.append(f"Blockchain <b>{chain.upper()}</b>")
    if year:
        context_parts.append(f"Steuerjahr <b>{year}</b>")
    if period:
        context_parts.append(f"Zeitraum <b>{period}</b>")

    context_text = (
        "Dieser Report fasst die steuerlich relevanten Krypto-Transaktionen "
        "der " + ", ".join(context_parts) +
        " zusammen. Grundlage sind automatisiert ausgewertete Transaktions- "
        "und Marktdaten."
    )

    story.append(Paragraph(context_text, body))
    story.append(Spacer(1, 16))

    story.append(Paragraph("<b>Steuerliche Kernaussagen</b>", h2))
    story.append(Spacer(1, 8))

    if tax_summary:
        is_us = str((tax_summary or {}).get("jurisdiction") or aggregates.get("jurisdiction") or "").upper() == "US"
        # EUR-Beträge nur in den KPI-Boxen; die Tabelle darunter nur Anzahlen/Erläuterung
        # (vermeidet doppelte Wiederholung derselben Netto-Zeilen).
        if is_us:
            rows = [
                ["Tax-ready events (count)", aggregates.get("total_txs", 0)],
                ["Events flagged taxable (capital)", aggregates.get("taxable_count", 0)],
                ["Events flagged non-taxable / zero PnL", aggregates.get("non_taxable_count", 0)],
                ["Special cases", aggregates.get("special_cases", "None")],
            ]
            kpis = [
                kpi_box("Events", str(aggregates["total_txs"]), styles, tone=None),
                kpi_box(
                    "Short-term (≤1y) net EUR",
                    aggregates["taxable_sum_eur"],
                    styles,
                    tone=_kpi_tone(aggregates.get("taxable_net_eur_raw")),
                ),
                kpi_box(
                    "Long-term (>1y) net EUR",
                    aggregates["taxfree_bucket_eur"],
                    styles,
                    tone=_kpi_tone(aggregates.get("taxfree_net_eur_raw")),
                ),
                kpi_box(
                    "Ordinary income (rewards) EUR",
                    aggregates["reward_sum_eur"],
                    styles,
                    tone=_kpi_tone(aggregates.get("reward_eur_raw")),
                ),
            ]
        else:
            rows = [
                ["Wirtschaftliche Ereignisse (tax-ready), Anzahl", aggregates.get("total_txs", 0)],
                [
                    "davon: mit kurzfristigem §23-Bucket-Anteil (taxable)",
                    aggregates.get("taxable_count", 0),
                ],
                [
                    "davon: ohne kurzfristigen §23-Bucket-Anteil (nicht taxable)",
                    aggregates.get("non_taxable_count", 0),
                ],
                ["Enthaltene Spezialfälle", aggregates.get("special_cases", "Keine")],
            ]
            kpis = [
                kpi_box("Ereignisse", str(aggregates["total_txs"]), styles, tone=None),
                kpi_box(
                    "§23 ≤365d netto",
                    aggregates["taxable_sum_eur"],
                    styles,
                    tone=_kpi_tone(aggregates.get("taxable_net_eur_raw")),
                ),
                kpi_box(
                    "§23 >365d netto",
                    aggregates["taxfree_bucket_eur"],
                    styles,
                    tone=_kpi_tone(aggregates.get("taxfree_net_eur_raw")),
                ),
                kpi_box(
                    "§22 Rewards",
                    aggregates["reward_sum_eur"],
                    styles,
                    tone=_kpi_tone(aggregates.get("reward_eur_raw")),
                ),
            ]
    else:
        rows = [
            ["Gesamtzahl Transaktionen", aggregates.get("total_txs", 0)],
            ["Steuerpflichtige Vorgänge (§23 EStG)", aggregates.get("taxable_count", 0)],
            ["Summe steuerpflichtige Gewinne", aggregates.get("taxable_sum_eur", "–")],
            ["Rewards & sonstige Einkünfte (§22 Nr.3 EStG)", aggregates.get("reward_sum_eur", "–")],
            ["Steuerfreie Vorgänge", aggregates.get("non_taxable_count", 0)],
            ["Enthaltene Spezialfälle", aggregates.get("special_cases", "Keine")],
        ]
        kpis = [
            kpi_box("Transaktionen", str(aggregates["total_txs"]), styles, tone=None),
            kpi_box("§23 Vorgänge", str(aggregates["taxable_count"]), styles, tone=None),
            kpi_box(
                "§23 Summe",
                aggregates["taxable_sum_eur"],
                styles,
                tone=_kpi_tone(aggregates.get("taxable_net_eur_raw")),
            ),
            kpi_box(
                "§22 Rewards",
                aggregates["reward_sum_eur"],
                styles,
                tone=_kpi_tone(aggregates.get("reward_eur_raw")),
            ),
        ]

    story.append(kpi_row(kpis))
    story.append(Spacer(1, 18))

    tbl = Table(rows, colWidths=[320, 140])
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.whitesmoke),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
                ("FONTNAME", (0, 0), (-1, -1), "Helvetica"),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story.append(tbl)
    excl_note = aggregates.get("excluded_from_totals_note") or ""
    if excl_note:
        story.append(Spacer(1, 10))
        story.append(Paragraph(excl_note, body))
    story.append(Spacer(1, 16))

    story.append(Paragraph("<b>Methodischer Hinweis</b>", h2))
    story.append(Spacer(1, 8))

    if tax_summary:
        is_us = str((tax_summary or {}).get("jurisdiction") or "").upper() == "US"
        if is_us:
            try:
                st = float((tax_summary or {}).get("short_term_capital_net_eur") or 0.0)
            except Exception:
                st = 0.0
            neg_note = ""
            if st < 0:
                neg_note = (
                    " A negative short-term total means a <b>net loss</b> in the short-term bucket "
                    "(FIFO), not that no short-term disposals occurred."
                )
            method_text = (
                "US-oriented view: realized gains/losses are split by holding period "
                "(short-term ≤1 year vs long-term >1 year). Both buckets are reportable components; "
                "this is not US tax advice. Rewards/airdrops are shown as ordinary-style income. "
                + neg_note
                + " Amounts are in EUR (pipeline unit); convert to USD if required for filing."
            )
        else:
            try:
                spec = float((tax_summary or {}).get("taxable_gains_net_eur") or 0.0)
            except Exception:
                spec = 0.0
            neg_note = ""
            if spec < 0:
                neg_note = (
                    " Ein negativer Wert bei „§23 ≤365d netto“ bedeutet einen "
                    "<b>Nettoverlust</b> im kurzfristigen Halte-Bucket (nach FIFO-Zuordnung), "
                    "nicht dass „keine“ kurzfristigen Vorgänge vorliegen."
                )
            method_text = (
                "Die Auswertung zu §23 EStG trennt realisierte Gewinne nach Haltedauer "
                "(Spekulationsfrist vs. mehr als ein Jahr). Staking-Rewards und vergleichbare "
                "Einnahmen werden als sonstige Einkünfte (§22 Nr.3 EStG) behandelt. "
                + neg_note
                + " Dieser Report ersetzt keine steuerliche Beratung."
            )
    else:
        method_text = (
            "Die steuerliche Bewertung erfolgt auf Basis historischer Marktpreise "
            "unter Anwendung der FIFO-Methode. Staking-Rewards, Airdrops und "
            "vergleichbare Einnahmen werden als sonstige Einkünfte (§22 Nr.3 EStG) "
            "behandelt. Dieser Report stellt keine steuerliche Beratung dar und "
            "dient als technische Entscheidungsgrundlage."
        )

    story.append(Paragraph(method_text, body))

    return story
