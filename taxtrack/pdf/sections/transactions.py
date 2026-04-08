# taxtrack/pdf/sections/transactions.py
# ------------------------------------------------------------
# PDF Section: Transaktionen (Detailansicht)
# Ziel:
# - steuerlich korrekt
# - für deutsche Steuerberater / Finanzamt verständlich
# - keine Logik, nur Darstellung
# ------------------------------------------------------------

from typing import Any, List
from reportlab.platypus import Paragraph, Spacer, PageBreak
from taxtrack.pdf.utils import get, make_table, short_hash


# ------------------------------------------------------------
# Anzeige-Hilfen
# ------------------------------------------------------------

def _short_addr(addr: str) -> str:
    a = (addr or "").strip()
    if not a:
        return ""
    if len(a) <= 12:
        return a
    return a[:6] + "…" + a[-4:]


# Deutsche Labels (NUR Anzeige)
CATEGORY_LABELS_DE = {
    "swap": "Tausch",
    "lp_add": "LP-Einzahlung",
    "lp_remove": "LP-Auszahlung",
    "reward": "Reward (Einkommen)",
    "staking_reward": "Staking-Reward",
    "vault_reward": "Vault-Reward",
    "pendle_reward": "Pendle-Reward",
    "restake_reward": "Restaking-Reward",
    "restake_in": "Restaking (Einzahlung)",
    "restake_out": "Restaking (Auszahlung)",
    "withdraw": "Auszahlung",
    "deposit": "Einzahlung",
    "transfer": "Übertragung",
    "internal_transfer": "Interne Umbuchung",
    "native_transfer_in": "Einzahlung (Native)",
    "native_transfer_out": "Auszahlung (Native)",
    "bridge_out": "Bridge-Auszahlung",
    "bridge_in": "Bridge-Einzahlung",
}

DIR_LABELS_DE = {
    "in": "Zugang",
    "out": "Abgang",
    "internal": "Intern",
}


# ------------------------------------------------------------
# Section Builder
# ------------------------------------------------------------

def section_transactions(
    records: List[Any],
    styles,
    eur_of,
    limit: int | None = 250,
):
    """
    Detailtabelle aller Transaktionen.
    records: classified_dicts oder ähnliche dict-Struktur
    eur_of: Funktion zur EUR-Wert-Ermittlung
    """

    story = []
    h1 = styles["Heading1"]

    if not records:
        return story

    story.append(Paragraph("<b>Transaktionen (Detailansicht)</b>", h1))
    story.append(Spacer(1, 10))

    rows = []
    count = 0

    for r in records:
        dt = get(r, "dt_iso", "") or ""
        token = get(r, "token", "") or ""
        dir_raw = (get(r, "direction", "") or "").lower()
        cat_raw = (get(r, "category", "") or "").lower()

        direction = DIR_LABELS_DE.get(dir_raw, dir_raw)
        category = CATEGORY_LABELS_DE.get(cat_raw, cat_raw)

        amount = get(r, "amount", 0.0) or 0.0
        eur_val = eur_of(r)

        fee_amt = get(r, "fee_amount", 0.0) or 0.0
        fee_tok = get(r, "fee_token", "") or ""
        fee_eur = get(r, "fee_eur", 0.0) or 0.0

        from_addr = _short_addr(get(r, "from", ""))
        to_addr = _short_addr(get(r, "to", ""))
        counterparty = get(r, "counterparty", "") or ""
        txh = short_hash(get(r, "tx_hash", "") or "")

        rows.append({
            "Datum / Uhrzeit": dt,
            "Asset": token,
            "Richtung": direction,
            "Vorgang": category,
            "Menge": amount,
            "EUR-Wert": eur_val,
            "Gebühr": f"{fee_amt} {fee_tok}".strip(),
            "Gebühr EUR": fee_eur,
            "Von": from_addr,
            "Nach": to_addr,
            "Plattform / Gegenpartei": counterparty,
            "Tx-Hash": txh,
        })

        count += 1
        if limit is not None and count >= limit:
            break

    make_table(
        "Einzeltransaktionen",
        rows,
        [
            "Datum / Uhrzeit",
            "Asset",
            "Richtung",
            "Vorgang",
            "Menge",
            "EUR-Wert",
            "Gebühr",
            "Gebühr EUR",
            "Von",
            "Nach",
            "Plattform / Gegenpartei",
            "Tx-Hash",
        ],
        styles,
        story,
        max_rows=limit,
    )

    story.append(PageBreak())
    return story
