# taxtrack/pdf/sections/cover.py

from reportlab.platypus import Paragraph, Spacer
from reportlab.lib.units import mm


def _xml_escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def section_cover(summary, styles):
    """
    Clean Cover-Seite.
    Erwartet:
      summary = {
        "title": str,
        "year": int|str,
        "wallet": str,
        "chain_id": str,
        "period": str,
        "version": str,
        "generated_at": str,
        "client_name": str,      # optional (Kunde)
        "client_address": str,  # optional, may contain newlines
      }
    """
    story = []

    h1 = styles["Heading1"]
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    # Großer Abstand oben (Weißraum)
    story.append(Spacer(1, 35 * mm))

    client_name = (summary.get("client_name") or "").strip()
    client_address = (summary.get("client_address") or "").strip()
    report_year = summary.get("year", "")

    if client_name:
        story.append(
            Paragraph(f"Kunde: <b>{_xml_escape(client_name)}</b>", body)
        )
    if client_address:
        addr_html = _xml_escape(client_address).replace("\n", "<br/>")
        story.append(Paragraph(f"Adresse:<br/>{addr_html}", body))
    if report_year:
        story.append(
            Paragraph(f"Jahr: <b>{_xml_escape(str(report_year))}</b>", body)
        )
    if client_name or client_address or report_year:
        story.append(Spacer(1, 8 * mm))

    # Titel
    title = summary.get("title", "KryptoBilanz Steuerreport")
    story.append(Paragraph(f"<b>{title}</b>", h1))
    story.append(Spacer(1, 8 * mm))

    # Untertitel / Jahr
    year = summary.get("year", "")
    if year:
        story.append(Paragraph(f"Steuerjahr <b>{year}</b>", h2))
        story.append(Spacer(1, 6 * mm))

    # Meta-Infos
    wallet = summary.get("wallet", "")
    chain = summary.get("chain_id", "")
    period = summary.get("period", "")
    version = summary.get("version", "")
    generated_at = summary.get("generated_at", "")

    if wallet:
        story.append(Paragraph(f"Wallet: <b>{wallet}</b>", body))
    if chain:
        story.append(Paragraph(f"Blockchain: <b>{chain.upper()}</b>", body))
    if period:
        story.append(Paragraph(f"Zeitraum: <b>{period}</b>", body))

    story.append(Spacer(1, 10 * mm))

    if version:
        story.append(Paragraph(f"Report-Version: <b>{version}</b>", body))
    if generated_at:
        story.append(Paragraph(f"Erstellt am: <b>{generated_at}</b>", body))

    # Footer-Disclaimer auf dem Cover (kurz)
    story.append(Spacer(1, 25 * mm))
    story.append(
        Paragraph(
            "Technischer Übersichtsreport zur steuerlichen Einordnung von Krypto-Transaktionen. "
            "Keine steuerliche Beratung.",
            body,
        )
    )

    return story
