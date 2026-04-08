# taxtrack/utils/merge_known_contracts.py
import json
from pathlib import Path
from datetime import datetime

LOCAL_PATH = Path("data/known_contracts.json")
MASTER_PATH = Path("data/known_contracts_master.json")
LOG_PATH = Path("data/logs/contracts_merge.log")

def load_json(p: Path) -> dict:
    if not p.exists():
        return {}
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

def save_json(p: Path, data: dict):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def merge_known_contracts():
    """
    Vereinigt lokale und Master-Contracts.
    - Neue Einträge aus master werden übernommen
    - Bestehende nicht überschrieben
    - Log wird erstellt
    """
    local = load_json(LOCAL_PATH)
    master = load_json(MASTER_PATH)
    if not master:
        print("[MERGE] ⚠️ Keine Master-Liste gefunden.")
        return 0

    added = {}
    for addr, info in master.items():
        addr_l = addr.lower()
        if addr_l not in local:
            local[addr_l] = info
            added[addr_l] = info

    if added:
        save_json(LOCAL_PATH, local)
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LOG_PATH.open("a", encoding="utf-8") as log:
            log.write(f"[{datetime.now().isoformat()}] {len(added)} neue Contracts übernommen\n")
            for a, i in added.items():
                log.write(f"  {a} → {i.get('label')} ({i.get('type')})\n")
        print(f"[MERGE] ✅ {len(added)} neue Contracts hinzugefügt.")
    else:
        print("[MERGE] Keine neuen Contracts gefunden.")

    return len(added)
