# taxtrack/pdf/sections/audit_report.py
# Audit trail: confidence summary, warnings, colored economic table (reporting only).

from __future__ import annotations

from typing import Any, Dict, List

from reportlab.platypus import Paragraph, Spacer

from taxtrack.pdf.utils import get, make_table_with_row_backgrounds, short_hash


def section_gewinn_verlust_lesehilfe(styles) -> List[Any]:
    """
    Kurzer Leitfaden vor der Tabelle „realisierte Vorgänge“:
    Eingang (Kostenseite) vs. Veräußerung (Differenz Brutto/Netto).
    """
    story: List[Any] = []
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    story.append(Paragraph("<b>Gewinn / Verlust – wie die Zahlen zusammenhängen</b>", h2))
    story.append(Spacer(1, 6))
    story.append(
        Paragraph(
            "<b>1) Eingang</b> (Zugang von Krypto, z. B. Kauf, Einzahlung, was Sie bei einem <i>Swap erhalten</i>, "
            "oder Rewards als Zugang): "
            "Diese Vorgänge legen die <b>Anschaffungskosten</b> Ihrer FIFO-Lots fest (Kostenbasis). "
            "Sie erscheinen in der folgenden Tabelle <b>nicht</b> als eigene Gewinn-Zeile "
            "— das ist die <b>Basis (X)</b> für spätere Realisierungen.<br/><br/>"
            "<b>2) Veräußerung / Realisierung</b> (z. B. <i>Verkauf</i>, <i>Swap-Abgang</i> (was Sie abgeben), "
            "Vault-Exit, LP-Remove u. Ä.): "
            "Hier wird der <b>Veräußerungserlös</b> der abgebauten Position den <b>FIFO-Anschaffungskosten</b> "
            "gegenübergestellt. "
            "Die <b>Differenz</b> ist das realisierte Ergebnis: Spalte <b>Brutto (€)</b> (vor Gebühr), "
            "nach Abzug der Gebühr <b>Netto (€)</b>. "
            "Das ist <b>(Y)</b> — nicht das gesamte Tauschvolumen als Gewinn.<br/><br/>"
            "<i>Kurz: Eingang → Kostenseite / Anschaffung (X). Swap, Verkauf, Exit usw. → Realisierung: "
            "Erlös minus Anschaffung = Brutto/Netto (Y).</i>",
            body,
        )
    )
    story.append(Spacer(1, 10))
    return story


def section_audit_summary(audit_report: Dict[str, Any], styles) -> List[Any]:
    story: List[Any] = []
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    conf = audit_report.get("confidence_distribution") or {}
    counts = conf.get("counts") or {}
    story.append(Paragraph("<b>Audit: Preis-Konfidenz (wirtschaftliche Ereignisse)</b>", h2))
    story.append(Spacer(1, 6))

    lines = [
        f"Anteil HIGH (direkte Preise): {conf.get('pct_high', 0)} %",
        f"Anteil MEDIUM (abgeleitet / gemischt): {conf.get('pct_medium', 0)} %",
        f"Anteil LOW (fehlend / Fallback): {conf.get('pct_low', 0)} %",
        f"Bewertung fehlend (classified, valuation_missing): {audit_report.get('valuation_missing_count', 0)}",
    ]
    for ln in lines:
        story.append(Paragraph(ln, body))
    story.append(Spacer(1, 8))

    story.append(
        Paragraph(
            "<i>Legende Zeilenfarbe: <b>grün</b> = Nettogewinn, <b>rot</b> = Nettoverlust, "
            "<b>grau</b> = neutral oder Swap mit nur kleinem Netto (unter Schwelle). "
            "Die Spalte „konfidenz“ (HIGH/MEDIUM/LOW) = <b>Preisqualität</b> in EUR.</i>",
            body,
        )
    )
    story.append(Spacer(1, 6))
    return story


def section_audit_warnings(audit_report: Dict[str, Any], styles) -> List[Any]:
    story: List[Any] = []
    h2 = styles["Heading2"]
    body = styles["BodyText"]

    tokens = audit_report.get("problematic_tokens") or []
    txs = audit_report.get("unresolved_tx_hashes") or []
    val = audit_report.get("validation") or {}

    if not tokens and not txs and val.get("ok"):
        return story

    story.append(Paragraph("<b>Hinweise / Prüfpunkte</b>", h2))
    story.append(Spacer(1, 4))

    if tokens:
        story.append(Paragraph("<b>Tokens mit niedriger Konfidenz oder fehlender Bewertung (Top):</b>", body))
        for t in tokens[:20]:
            story.append(Paragraph(f"• {t.get('token', '?')}: {t.get('count', 0)} Vorkommen", body))
        story.append(Spacer(1, 4))

    if txs:
        story.append(Paragraph("<b>Tx-Hashes mit unvollständiger Bewertung (Auszug):</b>", body))
        for txh in txs[:25]:
            story.append(Paragraph(f"• {short_hash(txh, 16)}", body))
        story.append(Spacer(1, 4))

    errs = val.get("errors") or []
    warns = val.get("warnings") or []
    if errs:
        story.append(Paragraph("<b>Validierung: Fehler</b>", body))
        for e in errs[:15]:
            story.append(Paragraph(f"• {e}", body))
    if warns:
        story.append(Paragraph("<b>Validierung: Hinweise</b>", body))
        for w in warns[:15]:
            story.append(Paragraph(f"• {w}", body))

    story.append(Spacer(1, 10))
    return story


def section_audit_economic_table(economic_records: List[Dict[str, Any]], styles) -> List[Any]:
    """Extended tax-ready table with confidence, source, trace fields."""
    story: List[Any] = []
    if not economic_records:
        return story

    rows = []
    for r in economic_records:
        txh = get(r, "tx_hash", "") or ""
        gross = float(get(r, "pnl_gross_eur", 0.0) or 0.0)
        if gross == 0 and r.get("proceeds") is not None and r.get("cost_basis") is not None:
            gross = float(get(r, "proceeds", 0.0)) - float(get(r, "cost_basis", 0.0))

        disp = (
            "Pendle Exit"
            if r.get("subtype") == "pendle"
            else "Vault Exit"
            if (get(r, "category", "") or "").lower() == "position_exit"
            else get(r, "category", "")
        )

        rows.append(
            {
                **r,
                "datetime": get(r, "dt_iso", ""),
                "kategorie": disp,
                "brutto_eur": round(gross, 2),
                "gebuehren_eur": float(get(r, "fees_eur", 0.0)),
                "netto_eur": float(get(r, "gain", 0.0)),
                "bis_365d_eur": float(get(r, "speculative_bucket_net_eur", 0.0)),
                "ueber_365d_eur": float(get(r, "long_term_bucket_net_eur", 0.0)),
                "konfidenz": (get(r, "price_confidence", "") or "").upper(),
                "preis_quelle": get(r, "price_source", ""),
                "quelle_tx": short_hash(txh, 14),
                "quelle_zeilen": get(r, "source_rows_count", ""),
                "kurz_erklärung": (get(r, "explanation_short", "") or "")[:200],
            }
        )

    # Querformat nutzen: feste Spaltenbreiten + Zeilenumbruch (lange Token/Erklärungen)
    audit_cols = [
        "datetime",
        "kategorie",
        "token",
        "brutto_eur",
        "gebuehren_eur",
        "netto_eur",
        "bis_365d_eur",
        "ueber_365d_eur",
        "konfidenz",
        "preis_quelle",
        "quelle_tx",
        "quelle_zeilen",
        "kurz_erklärung",
    ]
    col_widths = [
        50,
        46,
        52,
        42,
        38,
        42,
        42,
        42,
        34,
        46,
        54,
        30,
        281,
    ]
    make_table_with_row_backgrounds(
        "Realisierte Vorgänge mit Audit-Spalten (FIFO / §23)",
        rows,
        audit_cols,
        styles,
        story,
        bg_key="audit_row_bg",
        max_rows=250,
        col_widths=col_widths,
        wrap_cells=True,
    )
    return story
