# taxtrack/loaders/coinbase_rules.py
from __future__ import annotations
from dataclasses import asdict
from taxtrack.utils.debug_log import log


ALLOWED_REWARD_METHODS = {
    "staking_reward",
    "staking reward",
    "staking rewards",
    "staking income",
    "reward",
    "rewards",
    "learning_reward",
    "earn_reward"
}
STABLES = {"usdt","usdc","dai","tusd","usde"}

def apply_coinbase_rules(rows: list[dict]) -> list[dict]:
    cleaned = []
    for r in rows:
        token      = (r.get("token") or "").lower()
        method     = (r.get("method") or "").lower()
        notes      = (r.get("notes") or r.get("description") or "").lower()
        category   = (r.get("category") or "").lower()
        direction  = (r.get("direction") or "").lower()
        eur_value  = float(r.get("eur_value") or 0)
        fee_eur    = float(r.get("fee_eur") or 0)
        amount     = float(r.get("amount") or 0)

        # --- Richtung fixen für Transfers ---
        is_send    = "send" in method or category == "transfer_out"
        is_receive = "receive" in method or category == "transfer_in"
        if is_send:
            direction = "out"
        elif is_receive:
            direction = "in"

        # --- ETH<->ETH2 convert = staking_transfer (steuerfrei) ---
        is_eth_stake_convert = (method in {"convert","trade"}) and token in {"eth","eth2"}

        # --- Swap/Trade (ohne ETH<->ETH2) ---
        is_swap_trade = (("trade" in method) or ("convert" in method) or ("swap" in method)) and not is_eth_stake_convert

        # --- Reward NUR wenn Methode eindeutig Staking ist ---
        is_reward = method in ALLOWED_REWARD_METHODS or ("staking" in notes and "reward" in notes)

        # --- Stable? ---
        is_stable = token in STABLES

        taxable = False

        if is_reward:
            category = "staking_reward"
            direction = "in" if not direction else direction
            taxable = True
            # Rewards typischerweise ohne explizite extra Fee
            fee_eur = 0.0

        elif is_eth_stake_convert:
            category = "staking_transfer"
            taxable = False
            eur_value = 0.0
            r["gain_eur"] = 0.0
            # Richtung neutral lassen (oft nur eine Seite in CSV)

        elif is_swap_trade:
            if is_stable:
                category = "stable_swap"
                taxable = False
                # (optional) eur_value = 0.0  # falls du Erlös komplett neutralisieren willst
            else:
                category = "swap"
                taxable = True

        elif is_send or is_receive:
            category = "transfer"
            taxable = False
            eur_value = 0.0
            r["gain_eur"] = 0.0
            if direction != "out":
                fee_eur = 0.0  # Fee nur bei Abfluss behalten

        else:
            # alles andere als "other" → steuerfrei, sicherheitshalber eur_value neutralisieren
            category = "other"
            taxable = False
            if direction in {"other",""}:
                eur_value = 0.0

        # --- Outlier-Guard (keine Phantom-Riesenwerte bei Nicht-Rewards) ---
        if category not in {"staking_reward"} and eur_value > 10_000:
            log(f"[CLAMP] {token.upper()} {method} unrealistisch: {eur_value:.2f} → 0")
            eur_value = 0.0

        # Rückschreiben
        r["token"]     = token.upper()
        r["category"]  = category
        r["direction"] = direction
        r["taxable"]   = taxable
        r["eur_value"] = round(eur_value, 2)
        r["fee_eur"]   = round(fee_eur, 2)
        cleaned.append(r)

    log(f"[RULES] Coinbase-Postprocessing abgeschlossen: {len(cleaned)} gültige Zeilen")
    return cleaned