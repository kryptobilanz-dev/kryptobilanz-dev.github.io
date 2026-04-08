# taxtrack/pdf/sections/positioning.py
# Produktpositionierung & Abgrenzung (Marketing / Transparenz im Report)

from __future__ import annotations

from typing import Any, List

from reportlab.platypus import Paragraph, Spacer


def section_positioning(styles) -> List[Any]:
    """
    Kurz erklären, wodurch sich die Auswertung von generischen Portfolio-Tools unterscheidet
    und welche technischen Prinzipien gelten (ohne Steuerberatung zu ersetzen).
    """
    story: List[Any] = []
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    story.append(Paragraph("<b>Abgrenzung und Nutzen dieser Auswertung</b>", h2))
    story.append(Spacer(1, 8))

    bullets = [
        "<b>Transparenz statt Blackbox:</b> Jede realisierte Zeile ist bis zur Transaktion "
        "und zur Preislogik (Konfidenz, Quelle) nachvollziehbar – kein „Ergebnis ohne Herkunft“.",
        "<b>On-Chain &amp; DeFi-Fokus (EVM):</b> Klassifikation und FIFO sind auf komplexe "
        "Swaps, LP, Vault-/Restake-Muster ausgelegt – nicht nur Börsen-CSV-Import.",
        "<b>Bewertung über Swap-Gegenstände:</b> Fehlt für ein Vault-Receipt-Token ein "
        "Marktpreis, werden EUR-Werte wo möglich aus der bewertbaren Swap-Gegenseite "
        "abgeleitet (z. B. Liquid-Staking-Derivate wie EZETH über ETH-Kurs). "
        "Dafür werden Kurse für alle Swap-Beine angefragt, nicht nur für das Zeilen-Token.",
        "<b>Ehrliche Grenzen:</b> Fehlen beide Seiten einer Bewertung, bleiben Fälle "
        "als LOW/Prüfpunkt sichtbar – statt stiller Fantasiebeträge.",
        "<b>Audit &amp; Lesbarkeit:</b> Zeilenfarben nach <b>Netto €</b>; bei <b>Swaps</b> "
        "nur starke Abweichungen farbig (kleine Beträge grau). "
        "„Konfidenz“ = Bewertungsqualität der Kurse, nicht „ob du gewonnen hast“.",
        "<b>Modell:</b> Einmaliger Report und nachvollziehbare Datenlage statt "
        "undifferenziertem Jahresabo – Sie können gezielt entscheiden, wann ein Lauf Sinn ergibt.",
    ]

    for b in bullets:
        story.append(Paragraph(f"• {b}", body))
        story.append(Spacer(1, 5))

    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            "<i>Hinweis: Diese Punkte beschreiben die Arbeitsweise der Software; "
            "sie ersetzen keine individuelle steuerliche Beratung.</i>",
            body,
        )
    )
    story.append(Spacer(1, 12))
    return story
