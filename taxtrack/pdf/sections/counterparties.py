from __future__ import annotations

from typing import Any, Dict, List

from reportlab.platypus import Paragraph, Spacer, PageBreak

from taxtrack.pdf.utils import get, make_table


def _normalize_counterparty(cp: str) -> str:
    s = (cp or "").strip()
    if not s or s.lower() in ("null", "none", "nan", "-"):
        return "(unbekannt)"
    return s


def _tx_key(r: Any) -> str:
    tx = get(r, "tx_hash", "") or ""
    if isinstance(tx, str) and tx.strip():
        return tx.strip().lower()
    # ohne tx_hash: jede Zeile zählt für sich (keine Deduplizierung möglich)
    return f"__row__{id(r)}"


def section_counterparties(records: List[Any], styles, eur_of, limit: int = 20):
    """
    Summiert „Volumen“ pro Gegenpartei ohne typische Swap-Doppelzählung:

    Pro (Gegenpartei, Transaktion) wird nur der größte |EUR-Betrag| einer Zeile
    genommen (mehrere CSV-Zeilen pro Swap/Transfer). Anschließend Summe über alle Tx.

    count = Anzahl eindeutiger Transaktionen (tx_hash) pro Gegenpartei.
    """
    story = []
    h1 = styles["Heading1"]
    body = styles["BodyText"]

    # (cp_norm, tx_key) -> max |eur|
    per_tx: Dict[tuple[str, str], float] = {}
    for r in records:
        cp = _normalize_counterparty(str(get(r, "counterparty", "") or ""))
        txk = _tx_key(r)
        try:
            v = abs(float(eur_of(r) or 0.0))
        except Exception:
            v = 0.0
        key = (cp, txk)
        per_tx[key] = max(per_tx.get(key, 0.0), v)

    stats: Dict[str, Dict[str, float | int]] = {}
    for (cp, _txk), vmax in per_tx.items():
        st = stats.setdefault(cp, {"tx_count": 0, "volume": 0.0})
        st["tx_count"] += 1
        st["volume"] += float(vmax)

    if not stats:
        return story

    story.append(Paragraph("<b>Top Gegenparteien</b>", h1))
    story.append(Spacer(1, 10))
    story.append(
        Paragraph(
            "Hinweis: <i>volume_eur</i> ist die Summe der größten |EUR-Beträge| "
            "pro Transaktion und Gegenpartei (reduziert Doppelzählung bei Swaps mit "
            "mehreren Zeilen). Das ist <b>kein</b> identisches Abbild des steuerlichen "
            "Netto-PnL (FIFO).",
            body,
        )
    )
    story.append(Spacer(1, 8))

    rows = []
    for cp, st in sorted(stats.items(), key=lambda x: float(x[1]["volume"]), reverse=True)[:limit]:
        rows.append(
            {
                "counterparty": cp,
                "tx_count": int(st["tx_count"]),
                "volume_eur": float(st["volume"]),
            }
        )

    make_table(
        f"Wichtigste Gegenparteien (Top {limit})",
        rows,
        ["counterparty", "tx_count", "volume_eur"],
        styles,
        story,
    )
    story.append(PageBreak())
    return story
