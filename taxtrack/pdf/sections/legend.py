# taxtrack/pdf/sections/legend.py
from reportlab.platypus import Paragraph, Spacer, PageBreak


def section_legend(styles):
    story = []
    h1 = styles["Heading1"]
    body = styles["BodyText"]

    story.append(Paragraph("<b>Legende & Methodik (Kurz)</b>", h1))
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            "Ausführlichere Abschnitte zu <b>Swaps</b> und <b>Gebühren</b> (steuerliche "
            "Einordnung und konkrete Rechenlogik in diesem Report) folgen auf den "
            "nächsten Seiten.",
            body,
        )
    )
    story.append(Spacer(1, 10))

    # --- Kern: Realisierung vs Bewertung ---
    story.append(Paragraph("<b>1) Realisierung vs. Bewertung</b>", body))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "Dieser Report bildet steuerlich relevante <b>Realisierungen</b> ab. "
        "Reine Wertveränderungen (z. B. durch Rebase- oder Vault-Mechaniken) "
        "werden <b>nicht</b> als tägliche Einnahme angesetzt, solange kein tatsächlicher Zufluss "
        "oder keine Veräußerung stattfindet.",
        body
    ))
    story.append(Spacer(1, 10))

    # --- §23 ---
    story.append(Paragraph("<b>2) §23 EStG – Private Veräußerungsgeschäfte</b>", body))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "§23-Ereignisse entstehen bei <b>Swap/Verkauf</b>, <b>LP-Remove</b>, <b>Redeem/Unstake</b> "
        "oder vergleichbaren Vorgängen, bei denen ein wirtschaftlicher Tausch/Exit stattfindet. "
        "Ein Vorgang kann steuerlich relevant sein, auch wenn der berechnete Gewinn <b>0,00 €</b> beträgt "
        "(z. B. bei wertneutralen Umbauten oder Wrapper-Transaktionen).",
        body
    ))
    story.append(Spacer(1, 10))

    # --- §22 ---
    story.append(Paragraph("<b>3) §22 Nr. 3 EStG – Sonstige Einkünfte (Rewards)</b>", body))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "Rewards werden nur berücksichtigt, wenn ein <b>tatsächlicher Zufluss</b> "
        "stattfindet (z. B. Claim/Harvest/Ausschüttung). "
        "Ohne Zufluss werden keine täglichen Einnahmen angenommen.",
        body
    ))
    story.append(Spacer(1, 10))

    # --- Kategorien ---
    story.append(Paragraph("<b>4) Kategorie-Übersicht</b>", body))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "• <b>swap</b>: Token-Tausch<br/>"
        "• <b>lp_add</b>/<b>lp_remove</b>: Eintritt/Austritt aus Liquidity Pool<br/>"
        "• <b>pendle_deposit</b>/<b>pendle_redeem</b>/<b>pendle_reward</b>: Pendle Vorgänge<br/>"
        "• <b>restake_in</b>/<b>restake_out</b>: Restaking Ein-/Austritt<br/>"
        "• <b>reward</b>/<b>staking_reward</b>/<b>vault_reward</b>: Zuflüsse (Einkünfte)<br/>"
        "• <b>transfer</b>/<b>internal_transfer</b>: Bewegungen/Interne Transfers (steuerneutral)",
        body
    ))

    story.append(Spacer(1, 10))
    story.append(Paragraph("<b>5) Farben: Gewinn / Verlust / neutral</b>", body))
    story.append(Spacer(1, 6))
    story.append(Paragraph(
        "• <b>KPI-Boxen</b> (Executive Summary): grüner Rahmen/Hintergrund = positiver "
        "Nettobetrag im jeweiligen Kasten; rot = negativer Nettobetrag; grau = etwa null.<br/>"
        "• <b>Tabellen</b> „Realisierte Vorgänge“: Zeilenfarbe nach <b>Netto €</b> – bei "
        "<b>Swaps</b> werden nur <b>nennenswerte</b> Beträge grün/rot hervorgehoben "
        "(Schwelle ca. 10 € oder 1 % des größeren Werts aus Anschaffung/Erlös); kleine "
        "Rundungs-/Kursdifferenzen bleiben <b>grau</b> (Swap = steuerliche Realisierung, "
        "aber nicht automatisch „großer Gewinn“ in der Darstellung).<br/>"
        "• Lange Tabellen werden <b>über mehrere Seiten</b> fortgesetzt (Kopfzeile wiederholt).<br/>"
        "• <b>Preis-Konfidenz</b> (HIGH/MEDIUM/LOW) steht in der Audit-Tabelle in der "
        "Spalte „konfidenz“ und beschreibt nur die Bewertungsqualität in EUR.",
        body,
    ))

    story.append(PageBreak())
    return story
