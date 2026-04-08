# taxtrack/pdf/sections/gebuehren_steuer.py
# Gebühren: steuerliche Einordnung + wie diese Auswertung sie rechnerisch einbezieht.

from __future__ import annotations

from typing import Any, List

from reportlab.platypus import Paragraph, Spacer

from taxtrack.pdf.sections.swap_steuer_grundlagen import BMF_KRYPTO_2022_HTML


def section_fee_tax_basics(styles) -> List[Any]:
    """
    Erklärt Gebühren im Krypto-Kontext (DE) und die konkrete Logik dieses Systems.
    """
    story: List[Any] = []
    h1 = styles["Heading1"]
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    story.append(Paragraph("<b>Gebühren – steuerliche Rolle und Einordnung in diesem Report</b>", h1))
    story.append(Spacer(1, 8))

    story.append(Paragraph("<b>1) Steuerliche Grundidee (vereinfacht)</b>", h2))
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "Gebühren (Tauschgebühr, teils auch Netzwerk-/Gasgebühren) können die "
            "<b>wirtschaftliche Belastung</b> einer Transaktion erhöhen. In der "
            "Literatur und in BMF-Ausführungen zu virtuellen Währungen werden sie "
            "häufig in den Zusammenhang von <b>Anschaffungs- und Veräußerungskosten</b> "
            "gestellt. Je nach Sachverhalt kann eine Gebühr den <b>Veräußerungserlös "
            "netto</b> schmälern oder einen <b>eigenen Abgang</b> eines Tokens "
            "(z. B. Gas in ETH) bedeuten – der Einzelfall entscheidet.",
            body,
        )
    )
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            f'<b>Offizielle Einordnung (BMF):</b> '
            f'<a href="{BMF_KRYPTO_2022_HTML}" color="blue">BMF-Schreiben zu virtuellen Währungen und Token</a> '
            f"(siehe dort zu Einkünften, Veräußerungen und verwandten Fragen).",
            body,
        )
    )
    story.append(Spacer(1, 8))

    story.append(Paragraph("<b>2) Typische Fälle (konzeptionell)</b>", h2))
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "• <b>Gebühr im selben Token</b> wie der abgegebene Tausch (z. B. Swap, "
            "Anteil in ETH): wirkt wie ein zusätzlicher Abgang desselben Tokens "
            "und kann den Gewinn aus der Veräußerung <b>mindern</b> (über geringere "
            "wirtschaftliche Gegenleistung bzw. höhere Belastung).<br/>"
            "• <b>Gebühr in einem anderen Token</b>: kann einen <b>zusätzlichen</b> "
            "steuerlich relevanten Abgang dieses Tokens auslösen (eigener "
            "Vorgang neben dem Swap).<br/>"
            "• <b>Gas / Netzwerkgebühr</b> (typisch im nativen Chain-Token): "
            "stellt regelmäßig einen <b>Abgang</b> dieses Tokens dar und ist nicht "
            "„nur eine technische Nebensache“ ohne Belastung.",
            body,
        )
    )
    story.append(Spacer(1, 8))

    story.append(Paragraph("<b>3) Wie diese Auswertung Gebühren rechnerisch einbezieht</b>", h2))
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "<b>a)</b> Aus den Rohdaten werden <b>fee_amount</b> und <b>fee_token</b> "
            "(sofern vorhanden) mit dem zum Zeitpunkt der Transaktion ermittelten "
            "Kurs in <b>EUR</b> umgerechnet (<b>fee_eur</b> je erfasster Zeile).",
            body,
        )
    )
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "<b>b)</b> Für jede <b>Transaktion (tx_hash)</b> werden die <b>fee_eur</b> "
            "aller zugehörigen importierten Zeilen <b>summiert</b>.",
            body,
        )
    )
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "<b>c)</b> Das <b>wirtschaftliche Ereignis</b> (z. B. gruppierte Swap-FIFO-Zeilen) "
            "enthält einen <b>Brutto-PnL</b> aus FIFO (Veräußerungserlös abzüglich "
            "Anschaffungskosten der verbrauchten Lots) sowie eine Spalte "
            "<b>Gebühren (EUR)</b>. Das <b>Nettoergebnis</b> wird als "
            "<b>Brutto-PnL abzüglich der gesamten Transaktionsgebühren in EUR</b> "
            "gebildet – pauschal <b>pro Transaktion</b>, nicht durch Erhöhung der "
            "veräußerten Tokenmenge in der FIFO-Berechnung.",
            body,
        )
    )
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            "<i>Grenze der Modellierung:</i> Eine vollständige Abbildung „jede Gebühr "
            "als eigene Lot-Entnahme mit exakter Tokenmenge“ wäre feiner, erfordert "
            "aber durchgängig getrennte Erfassung aller Abgänge inkl. Gas als "
            "eigener Buchung. Dieses System wählt die <b>pragmatische Netto-Abzugslogik "
            "in EUR pro Tx</b> für nachvollziehbare Summen im Report.",
            body,
        )
    )
    story.append(Spacer(1, 8))

    story.append(Paragraph("<b>4) Häufige Missverständnisse</b>", h2))
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "• <b>Gebühr ignorieren</b> → künstlich zu hoher Gewinn; hier werden "
            "bekannte Gebühren in EUR abgezogen, soweit Kursdaten vorliegen.<br/>"
            "• <b>Gebühr doppelt zählen</b> → wird vermieden, indem die Summe "
            "<b>pro tx_hash</b> einmal angesetzt wird.<br/>"
            "• <b>Erwartung „Gebühr steht schon in der Swap-Menge“</b> → in dieser "
            "Pipeline ist die Netto-Logik <b>explizit</b> über den EUR-Abzug "
            "beschrieben (siehe oben).",
            body,
        )
    )
    story.append(Spacer(1, 12))

    story.append(
        Paragraph(
            "<i>Hinweis: Keine individuelle Rechtsberatung; bei Grenzfällen "
            "(Betriebsvermögen, komplexe DeFi-Strukturen) Steuerberater einbeziehen.</i>",
            body,
        )
    )
    story.append(Spacer(1, 8))
    return story
