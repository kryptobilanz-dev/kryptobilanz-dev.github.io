# taxtrack/rules/taxlogic.py

import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

class TaxLogic:
    def __init__(self, country_code: str = "de"):
        self.path = BASE_DIR / f"taxlogic_{country_code}.json"
        self.logic = self._load_logic()

    def _load_logic(self) -> dict:
        if not self.path.exists():
            print(f"[WARN] Steuerlogik {self.path} fehlt.")
            return {}
        with self.path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def get_rule(self, category: str) -> dict:
        key = (category or "").lower()
        return self.logic.get(key, {
            "taxable": False,
            "paragraph": None,
            "type": "Unbekannt",
            "description": "Keine Regel gefunden"
        })

    def describe(self, category: str) -> str:
        rule = self.get_rule(category)
        taxable = "steuerpflichtig" if rule.get("taxable") else "steuerfrei"
        paragraph = rule.get("paragraph")
        par = f" ({paragraph})" if paragraph else ""
        return f"{rule.get('type', 'Unbekannt')} – {taxable}{par}: {rule.get('description', '')}"

