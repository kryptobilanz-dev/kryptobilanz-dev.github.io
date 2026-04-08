# taxtrack/utils/contract_autoupdater.py
import json
import requests
from pathlib import Path

KNOWN_PATH = Path("data/known_contracts.json")
ETHERSCAN_API = "https://api.etherscan.io/api"
ETHERSCAN_KEY = "8M8AK7TVQM3PAR1RVQA9C4SGPKYYARUSN1"

def load_known():
    if not KNOWN_PATH.exists():
        return {}
    with KNOWN_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_known(data: dict):
    KNOWN_PATH.parent.mkdir(parents=True, exist_ok=True)
    with KNOWN_PATH.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def fetch_label_from_etherscan(address: str) -> dict | None:
    try:
        r = requests.get(ETHERSCAN_API, params={
            "module": "contract",
            "action": "getsourcecode",
            "address": address,
            "apikey": ETHERSCAN_KEY
        }, timeout=8)
        js = r.json()
        result = js.get("result", [{}])[0]
        name = result.get("ContractName") or result.get("SourceCode")
        if not name:
            return None
        label_type = "protocol" if "router" in name.lower() or "pool" in name.lower() else "contract"
        return {"label": name.strip(), "type": label_type}
    except Exception as e:
        print(f"[AutoUpdater] Fehler bei {address}: {e}")
        return None

def update_known_contracts(new_addresses: list[str]):
    data = load_known()
    updated = 0
    for addr in new_addresses:
        a = addr.lower()
        if a in data:
            continue
        label = fetch_label_from_etherscan(a)
        if label:
            data[a] = label
            updated += 1
            print(f"[AutoUpdater] {a} → {label['label']} ({label['type']})")
    if updated:
        save_known(data)
        print(f"[AutoUpdater] {updated} neue Labels gespeichert → {KNOWN_PATH}")
    else:
        print("[AutoUpdater] Keine neuen Labels gefunden.")
