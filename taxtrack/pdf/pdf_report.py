# taxtrack/pdf/pdf_report.py
# KryptoBilanz PDF Engine – finale, konsistente Version

from __future__ import annotations
from datetime import datetime

from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, PageBreak
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import mm

from taxtrack.pdf.theme.typography import apply_typography
from taxtrack.pdf.sections.cover import section_cover
from taxtrack.pdf.sections.executive_summary import section_executive_summary
from taxtrack.pdf.sections.lp import section_lp
from taxtrack.pdf.sections.rewards import section_rewards
from taxtrack.pdf.sections.fees import section_fees
from taxtrack.pdf.sections.counterparties import section_counterparties
from taxtrack.pdf.sections.transactions import section_transactions
from taxtrack.pdf.utils import as_float, get, fmt_eur, make_table, make_table_with_row_backgrounds, group_lp_gains
from taxtrack.pdf.theme.pnl_colors import row_bg_for_tax_row
from taxtrack.pdf.sections.audit_report import (
    section_audit_economic_table,
    section_audit_summary,
    section_audit_warnings,
    section_gewinn_verlust_lesehilfe,
)
from taxtrack.pdf.sections.legend import section_legend
from taxtrack.pdf.sections.positioning import section_positioning
from taxtrack.pdf.sections.swap_steuer_grundlagen import section_swap_tax_basics
from taxtrack.pdf.sections.gebuehren_steuer import section_fee_tax_basics


PDF_DISCLAIMER = (
    "Dieses Dokument stellt keine steuerliche Beratung dar und ersetzt keine "
    "Auskunft eines Steuerberaters oder der zuständigen Finanzverwaltung. "
    "Alle Auswertungen erfolgen auf Basis der übermittelten Daten; "
    "für die Richtigkeit und Vollständigkeit wird keine Haftung übernommen."
)

REWARD_CATS = {
    "reward",
    "staking_reward",
    "vault_reward",
    "pendle_reward",
    "restake_reward",
    "airdrop",
    "learning_reward",
    "earn_reward",
}


def _period_text(debug_info, year):
    f = (debug_info or {}).get("from")
    t = (debug_info or {}).get("to")
    if f and t:
        # "bis" statt Pfeil: Helvetica/PDF rendert manche Unicode-Pfeile inkonsistent.
        return f"{f} bis {t}"
    if year:
        return f"Kalenderjahr {year}"
    return "unbekannt"


def _footer_fn(wallet: str, chain: str, period: str, now_str: str):
    def on_page(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        left = f"Wallet: {wallet} · Chain: {chain.upper() if chain else ''} · Zeitraum: {period}"
        right = f"Erstellt am {now_str} · KryptoBilanz"
        canvas.drawString(12 * mm, 8 * mm, left)
        canvas.drawRightString(285 * mm, 8 * mm, right)
        canvas.restoreState()
    return on_page


def build_pdf(*args, **kwargs):
    """
    economic_records: tax-ready dicts (pipeline) OR legacy grouped gains (pnl_eur).
    tax_summary: optional; when set, executive summary matches tax_interpreter totals.
    """

    if kwargs:
        economic_records = kwargs.get("economic_records")
        reward_records = kwargs.get("reward_records")
        summary = kwargs.get("summary")
        debug_info = kwargs.get("debug_info")
        outpath = kwargs.get("outpath")
        tax_summary = kwargs.get("tax_summary")
    else:
        if len(args) == 4:
            tax_summary = None
            economic_records, summary, debug_info, outpath = args
            reward_records = []
        elif len(args) == 5:
            tax_summary = None
            economic_records, reward_records, summary, debug_info, outpath = args
        else:
            raise TypeError(
                "build_pdf() expects (economic_records, summary, debug_info, outpath) "
                "or (economic_records, reward_records, summary, debug_info, outpath)"
            )

    economic_records = list(economic_records or [])
    reward_records = list(reward_records or [])
    summary = summary or {}
    debug_info = debug_info or {}

    styles = apply_typography(getSampleStyleSheet())
    h1 = styles["Heading1"]

    story = []

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    wallet = debug_info.get("wallet") or summary.get("wallet") or ""
    chain = debug_info.get("chain") or summary.get("chain_id") or ""
    year = debug_info.get("year") or summary.get("year")
    period = _period_text(debug_info, year)

    client_name = (
        debug_info.get("customer_name")
        or debug_info.get("name")
        or debug_info.get("customer")
        or ""
    )
    if isinstance(client_name, str):
        client_name = client_name.strip()
    client_address = debug_info.get("customer_address") or debug_info.get("address") or ""
    if isinstance(client_address, str):
        client_address = client_address.strip()

    def cat_of(r):
        return (get(r, "category", "") or "").lower()

    def eur_of(r):
        return as_float(get(r, "eur_value", 0.0))

    reward_events = [r for r in reward_records if cat_of(r) in REWARD_CATS]
    total_rewards = sum(eur_of(r) for r in reward_events)
    lp_groups = group_lp_gains(economic_records)

    if tax_summary is not None:
        is_us = str(tax_summary.get("jurisdiction") or "").upper() == "US"
        if is_us:
            speculative = float(tax_summary.get("short_term_capital_net_eur") or 0)
            long_term = float(tax_summary.get("long_term_capital_net_eur") or 0)
        else:
            speculative = float(tax_summary.get("taxable_gains_net_eur") or 0)
            long_term = float(tax_summary.get("taxfree_gains_net_eur") or 0)
        total_net = float(tax_summary.get("total_gains_net_eur") or 0)
        taxable_evt = sum(1 for r in economic_records if r.get("taxable"))
        only_long = sum(1 for r in economic_records if not r.get("taxable"))
        excl_cnt = int(tax_summary.get("excluded_from_totals_count") or 0)
        excl_note = ""
        if excl_cnt > 0:
            excl_raw = float(tax_summary.get("excluded_from_totals_net_eur") or 0.0)
            if is_us:
                excl_note = (
                    f"<b>Note:</b> {excl_cnt} event(s) with missing or low-confidence pricing "
                    f"are excluded from headline capital-gain totals "
                    f"(raw sum of those rows: {fmt_eur(excl_raw)}). They may still appear in detail."
                )
            else:
                excl_note = (
                    f"<b>Hinweis:</b> {excl_cnt} Vorgänge mit fehlender oder niedrig konfidenter "
                    f"Bewertung sind in die §23-Netto-Kennzahlen nicht eingerechnet "
                    f"(Rohsumme dieser Zeilen: {fmt_eur(excl_raw)}). Sie erscheinen weiter in der "
                    f"Detail-Auswertung."
                )
        aggregates = {
            "total_txs": int(tax_summary.get("rows") or len(economic_records)),
            "taxable_count": taxable_evt,
            "non_taxable_count": only_long,
            "taxable_sum_eur": fmt_eur(speculative),
            "taxfree_bucket_eur": fmt_eur(long_term),
            "total_net_eur": fmt_eur(total_net),
            "reward_sum_eur": fmt_eur(total_rewards),
            "taxable_net_eur_raw": speculative,
            "taxfree_net_eur_raw": long_term,
            "reward_eur_raw": total_rewards,
            "special_cases": "LP, Vaults, Restaking" if lp_groups else "Keine",
            "excluded_from_totals_note": excl_note,
            "jurisdiction": "US" if is_us else "DE",
        }
    else:
        taxable_events = [r for r in economic_records if bool(r.get("taxable", False))]
        non_taxable_events = [r for r in economic_records if not bool(r.get("taxable", False))]
        total_taxable = sum(as_float(r.get("pnl_eur", 0.0)) for r in taxable_events)
        aggregates = {
            "total_txs": len(economic_records),
            "taxable_count": len(taxable_events),
            "non_taxable_count": len(non_taxable_events),
            "taxable_sum_eur": fmt_eur(total_taxable),
            "taxfree_bucket_eur": "–",
            "total_net_eur": "–",
            "reward_sum_eur": fmt_eur(total_rewards),
            "taxable_net_eur_raw": total_taxable,
            "taxfree_net_eur_raw": 0.0,
            "reward_eur_raw": total_rewards,
            "special_cases": "LP, Vaults, Restaking" if lp_groups else "Keine",
        }

    story.extend(
        section_cover(
            {
                "title": "KryptoBilanz Steuerreport",
                "year": year,
                "wallet": wallet,
                "chain_id": chain,
                "period": period,
                "version": "v0.9",
                "generated_at": now,
                "client_name": client_name,
                "client_address": client_address,
            },
            styles,
        )
    )
    story.append(PageBreak())

    story.extend(
        section_executive_summary(
            {"wallet": wallet, "chain_id": chain, "year": year, "period": period},
            aggregates,
            styles,
            tax_summary=tax_summary,
        )
    )
    story.append(PageBreak())

    story.extend(section_legend(styles))
    story.extend(section_swap_tax_basics(styles))
    story.extend(section_fee_tax_basics(styles))
    story.append(PageBreak())
    story.extend(section_positioning(styles))

    story.extend(
        section_transactions(
            records=reward_records,
            styles=styles,
            eur_of=lambda r: eur_of(r),
            limit=250,
        )
    )

    story.append(Paragraph("<b>Wirtschaftliche Ereignisse (§23 EStG, tax-ready)</b>", h1))
    story.append(Spacer(1, 10))
    story.extend(section_gewinn_verlust_lesehilfe(styles))

    audit_report = dict((debug_info or {}).get("audit_report") or {})
    if tax_summary is not None and economic_records and any(
        isinstance(r, dict) and "price_confidence" in r for r in economic_records
    ):
        if not audit_report.get("confidence_distribution"):
            from taxtrack.pdf.audit_validation import confidence_distribution

            audit_report["confidence_distribution"] = confidence_distribution(economic_records)
        story.extend(section_audit_summary(audit_report, styles))
        story.extend(section_audit_economic_table(economic_records, styles))
        story.extend(section_audit_warnings(audit_report, styles))
    elif tax_summary is not None:
        econ_rows = []
        for r in economic_records:
            cat = get(r, "category", "")
            disp = (
                "Pendle Exit"
                if r.get("subtype") == "pendle"
                else "Vault Exit"
                if cat == "position_exit"
                else cat
            )
            gross = as_float(r.get("pnl_gross_eur"))
            if gross == 0 and r.get("proceeds") is not None and r.get("cost_basis") is not None:
                gross = as_float(r.get("proceeds")) - as_float(r.get("cost_basis"))
            net = as_float(r.get("gain"))
            econ_rows.append(
                {
                    "datetime": get(r, "dt_iso", ""),
                    "kategorie": disp,
                    "token": get(r, "token", ""),
                    "brutto_eur": round(gross, 2),
                    "gebuehren_eur": as_float(r.get("fees_eur")),
                    "netto_eur": net,
                    "bis_365d_eur": as_float(r.get("speculative_bucket_net_eur")),
                    "ueber_365d_eur": as_float(r.get("long_term_bucket_net_eur")),
                    "kurzfristig": "ja" if r.get("taxable") else "nein",
                    "halt_min": r.get("holding_period_days_min", ""),
                    "halt_max": r.get("holding_period_days_max", ""),
                    "audit_row_bg": row_bg_for_tax_row(
                        {
                            "category": get(r, "category", ""),
                            "gain": net,
                            "cost_basis": r.get("cost_basis"),
                            "proceeds": r.get("proceeds"),
                        }
                    ),
                }
            )
        make_table_with_row_backgrounds(
            "Realisierte Vorgänge (FIFO-Haltedauer / §23)",
            econ_rows,
            [
                "datetime",
                "kategorie",
                "token",
                "brutto_eur",
                "gebuehren_eur",
                "netto_eur",
                "bis_365d_eur",
                "ueber_365d_eur",
                "kurzfristig",
                "halt_min",
                "halt_max",
            ],
            styles,
            story,
            bg_key="audit_row_bg",
        )
    else:
        econ_rows = []
        for r in economic_records:
            pnl_net = as_float(get(r, "net_pnl_eur", 0.0))
            econ_rows.append(
                {
                    "datetime": get(r, "dt_iso", ""),
                    "display_category": (
                        "Pendle Exit"
                        if r.get("subtype") == "pendle"
                        else "Vault Exit"
                        if r.get("category") == "position_exit"
                        else get(r, "category", "")
                    ),
                    "token": get(r, "token", ""),
                    "pnl_brutto": as_float(get(r, "pnl_eur", 0.0)),
                    "fees": as_float(get(r, "fees_eur", 0.0)),
                    "pnl_netto": pnl_net,
                    "taxable": "ja" if bool(r.get("taxable", False)) else "nein",
                    "audit_row_bg": row_bg_for_tax_row(
                        {
                            "category": get(r, "category", ""),
                            "gain": pnl_net,
                            "cost_basis": r.get("cost_basis"),
                            "proceeds": r.get("proceeds"),
                        }
                    ),
                }
            )
        make_table_with_row_backgrounds(
            "Realisierte Vorgänge",
            econ_rows,
            ["datetime", "display_category", "token", "pnl_brutto", "fees", "pnl_netto", "taxable"],
            styles,
            story,
            bg_key="audit_row_bg",
        )

    story.append(PageBreak())

    story.extend(
        section_lp(
            records=economic_records,
            styles=styles,
            eur_of=lambda r: as_float(
                r.get("gain") if tax_summary is not None else get(r, "pnl_eur", 0.0)
            ),
            normal_style=styles["BodyText"],
        )
    )

    story.extend(
        section_rewards(
            records=reward_records,
            styles=styles,
            eur_of=eur_of,
        )
    )

    story.extend(
        section_fees(
            records=reward_records,
            styles=styles,
            fee_amount_of=lambda r: as_float(get(r, "fee_amount", 0.0)),
            fee_origin_of=lambda r: (get(r, "fee_origin", "") or "unknown").lower(),
        )
    )

    story.extend(
        section_counterparties(
            records=reward_records,
            styles=styles,
            eur_of=eur_of,
            limit=20,
        )
    )

    story.append(Spacer(1, 12 * mm))
    story.append(Paragraph("<b>Disclaimer</b>", styles["Heading2"]))
    story.append(Spacer(1, 4 * mm))
    story.append(Paragraph(PDF_DISCLAIMER, styles["BodyText"]))

    doc = SimpleDocTemplate(
        str(outpath),
        pagesize=landscape(A4),
        leftMargin=10 * mm,
        rightMargin=10 * mm,
        topMargin=11 * mm,
        bottomMargin=13 * mm,
    )
    footer = _footer_fn(wallet, chain, period, now)
    doc.build(story, onFirstPage=footer, onLaterPages=footer)
    return outpath
