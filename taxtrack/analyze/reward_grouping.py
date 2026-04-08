from collections import defaultdict
from typing import List, Dict, Any

REWARD_CATEGORIES = {
    "reward",
    "staking_reward",
    "vault_reward",
    "pendle_reward",
    "restake_reward",
    "airdrop",
    "learning_reward",
    "earn_reward",
}

def group_rewards(classified: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Gruppiert Rewards nach Token (und optional Zeitraum).
    Eingabe: ClassifiedItems als dict
    Ausgabe: Liste mit {token, total_amount, total_eur, rows}
    """
    acc = defaultdict(lambda: {
        "token": None,
        "total_amount": 0.0,
        "total_eur": 0.0,
        "rows": 0,
    })

    for c in classified:
        cat = (c.get("category") or "").lower()
        if cat not in REWARD_CATEGORIES:
            continue

        token = c.get("token")
        if not token:
            continue

        key = token
        acc[key]["token"] = token
        acc[key]["total_amount"] += float(c.get("amount") or 0.0)
        acc[key]["total_eur"] += float(c.get("eur_value") or 0.0)
        acc[key]["rows"] += 1

    # aufräumen & runden
    out = []
    for v in acc.values():
        out.append({
            "token": v["token"],
            "total_amount": round(v["total_amount"], 8),
            "total_eur": round(v["total_eur"], 2),
            "rows": v["rows"],
        })

    # sortiert nach EUR (absteigend)
    out.sort(key=lambda x: x["total_eur"], reverse=True)
    return out
