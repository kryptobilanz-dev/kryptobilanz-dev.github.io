from typing import Any, List
from reportlab.platypus import Paragraph, Spacer, PageBreak
from taxtrack.pdf.utils import get, make_table, short_hash

def section_rewards(records: List[Any], styles, eur_of):
    story = []
    h1 = styles["Heading1"]

    reward_cats = {
        "reward", "staking_reward", "vault_reward",
        "pendle_reward", "restake_reward",
        "airdrop", "learning_reward", "earn_reward",
    }

    def cat_of(r): return (get(r, "category", "") or "").lower()
    reward_events = [r for r in records if cat_of(r) in reward_cats]
    if not reward_events:
        return story

    story.append(Paragraph("<b>Rewards & Airdrops (§22 Nr.3 EStG)</b>", h1))
    story.append(Spacer(1, 10))

    rows = []
    for r in reward_events:
        rows.append(
            {
                "datetime": get(r, "dt_iso", ""),
                "tx": short_hash(get(r, "tx_hash", "")),
                "token": get(r, "token", ""),
                "amount": get(r, "amount", 0.0),
                "eur_value": eur_of(r),
                "category": get(r, "category", ""),
                "counterparty": get(r, "counterparty", ""),
            }
        )

    make_table("Reward-Einträge", rows, ["datetime", "tx", "token", "amount", "eur_value", "category", "counterparty"], styles, story)
    total = sum(float(eur_of(r) or 0.0) for r in reward_events)
    story.append(Paragraph(f"<b>Summe Rewards: {total:,.2f} €</b>".replace(",", " ").replace(".", ","), styles["BodyText"]))
    story.append(Spacer(1, 8))

    story.append(PageBreak())
    return story
