# taxtrack/pdf/sections/swap_steuer_grundlagen.py
# Bildungsinhalt: Wie Swaps in der Ertragsteuer (Privatvermögen, §23 EStG) eingeordnet werden.

from __future__ import annotations

from typing import Any, List

from reportlab.platypus import Paragraph, Spacer

# Öffentliche BMF-Seite zum Schreiben vom Mai 2022 (virtuelle Währungen / Token)
BMF_KRYPTO_2022_HTML = (
    "https://www.bundesfinanzministerium.de/Content/DE/Pressemitteilungen/"
    "Finanzpolitik/2022/05/2022-05-09-einzelfragen-zur-ertragsteuerrechtlichen-behandlung-"
    "von-virtuellen-waehrungen-und-von-sonstigen-token-bmf-schreiben.html"
)


def section_swap_tax_basics(styles) -> List[Any]:
    """
    Erklärt Nutzern, warum ein „Swap“ steuerlich nicht „neutraler Tausch ohne Folgen“ ist,
    und nennt die offizielle BMF-Quelle (ohne Rechtsberatung zu ersetzen).
    """
    story: List[Any] = []
    h1 = styles["Heading1"]
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    story.append(Paragraph("<b>Wie werden Swaps bei Krypto steuerlich bewertet?</b>", h1))
    story.append(Spacer(1, 8))

    story.append(
        Paragraph(
            "Viele sehen nur: „Ich habe Token A gegen Token B getauscht – kein Euro auf dem Konto.“ "
            "In der <b>Ertragsteuer</b> für das <b>Privatvermögen</b> wird ein solcher Vorgang "
            "typischerweise <b>nicht</b> als wirkungsloser Tausch behandelt, sondern als "
            "<b>Veräußerung</b> der abgegebenen Wirtschaftsgüter mit gleichzeitigem "
            "<b>Erwerb</b> der neuen Wirtschaftsgüter (Anschaffung neuer Position).",
            body,
        )
    )
    story.append(Spacer(1, 8))

    story.append(Paragraph("<b>Kernidee: Tausch = Verkauf + Kauf (zugleich)</b>", h2))
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "<b>Beispiel (vereinfacht):</b> Du tauschst 1 ETH in 2.000 USDC. "
            "Zum Bewertungszeitpunkt entspricht 1 ETH z. B. 2.000 €. "
            "Die früheren <b>Anschaffungskosten</b> der 1 ETH (FIFO) seien 1.000 €. "
            "Dann beträgt das <b>private Veräußerungsgeschäft</b> nach §23 EStG "
            "in der Regel einen <b>Gewinn von 1.000 €</b> (Veräußerungserlös abzüglich "
            "Anschaffungskosten der veräußerten Einheiten). "
            "Die erworbenen USDC haben eine neue <b>Anschaffung</b> (hier z. B. 2.000 €).",
            body,
        )
    )
    story.append(Spacer(1, 8))

    story.append(Paragraph("<b>Warum wirken Beträge oft sehr groß (+ / −)?</b>", h2))
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "• <b>Fehlende oder unsichere EUR-Bewertung</b> einzelner Token → falsche oder extreme Nettoergebnisse.<br/>"
            "• <b>DeFi / LP / Vault</b>: je nach Vorgang keine einfache „Spot-Swap“-Logik; "
            "Klassifikation und Bewertung müssen zur Transaktion passen.<br/>"
            "• <b>Abgeleitete Preise</b> (z. B. Gegenseite des Swaps) vs. direkte Kurse → Abweichungen.<br/>"
            "• <b>FIFO</b>: es werden ältere, günstig angeschaffte Lots zuerst „verkauft“ → "
            "kann zu hohem Buchgewinn führen, auch wenn du subjektiv „nur umschichtest“.",
            body,
        )
    )
    story.append(Spacer(1, 8))

    story.append(Paragraph("<b>Einordnung (ohne Rechtsberatung)</b>", h2))
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "Für die steuerliche Einordnung virtueller Währungen und Token hat das "
            "<b>Bundesministerium der Finanzen</b> u. a. ein BMF-Schreiben veröffentlicht "
            "(Stand der öffentlichen Diskussion: Mai 2022). Dort werden u. a. "
            "Veräußerungen und Tauschvorgänge im Privatvermögen erläutert. "
            "Verbindlich ist für dich immer der Einzelfall und ggf. die Abstimmung mit "
            "einem Steuerberater oder der Finanzverwaltung.",
            body,
        )
    )
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            f'<b>Offizielle Quelle (BMF):</b> '
            f'<a href="{BMF_KRYPTO_2022_HTML}" color="blue">'
            f"Einzelfragen zur ertragsteuerrechtlichen Behandlung von virtuellen Währungen "
            f"und von sonstigen Token (BMF)</a>",
            body,
        )
    )
    story.append(Spacer(1, 4))
    story.append(Paragraph(f"Vollständige URL: {BMF_KRYPTO_2022_HTML}", body))
    story.append(Spacer(1, 10))

    story.append(Paragraph("<b>Kurzüberblick: Vorgang und typische steuerliche Lesart</b>", h2))
    story.append(Spacer(1, 4))
    story.append(
        Paragraph(
            "• <b>ETH → USDC</b>: Veräußerung der ETH-Position (Erlös vs. FIFO-Anschaffung); "
            "Anschaffung der USDC.<br/>"
            "• <b>USDC → BTC</b>: Veräußerung der USDC-Position; Anschaffung BTC.<br/>"
            "• <b>LP hinzufügen / entfernen</b>: oft <b>kein</b> einfacher „ein Swap = ein Verkauf“ – "
            "abhängig von Klassifikation und wirtschaftlichem Gehalt.<br/>"
            "• <b>Staking-Rewards / ähnliche Zuflüsse</b>: häufig <b>sonstige Einkünfte</b> "
            "(z. B. §22 EStG), nicht dasselbe wie ein Spot-Swap.",
            body,
        )
    )
    story.append(Spacer(1, 8))

    story.append(
        Paragraph(
            "<b>Was dieser Report zeigt:</b> Für als <b>Swap</b> erkannte Vorgänge "
            "weist die Auswertung das <b>Nettoergebnis</b> aus Veräußerungserlös und "
            "FIFO-Anschaffungskosten aus – das ist <b>kein</b> „Spielgeld-Gewinn“, sondern "
            "die übliche Darstellung einer <b>realisierten</b> Position. "
            "Ob ein Einzelfall anders zu qualifizieren ist, bleibt außerhalb einer rein "
            "technischen Auswertung.",
            body,
        )
    )
    story.append(Spacer(1, 12))
    return story
