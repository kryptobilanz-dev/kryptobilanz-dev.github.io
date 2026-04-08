# taxtrack/utils/path.py

from pathlib import Path

# ============================================================
# Zentrierte Pfade für das gesamte Projekt
# Egal von wo aus TaxTrack gestartet wird!
# ============================================================

# taxtrack/
ROOT = Path(__file__).resolve().parents[1]

# taxtrack/data/
DATA_DIR = ROOT / "data"

# taxtrack/data/config/
CONFIG_DIR = DATA_DIR / "config"

# taxtrack/data/prices/
PRICES_DIR = DATA_DIR / "prices"

# Einzeldateien
ADDRESS_MAP_FILE = DATA_DIR / "address_map.json"
COUNTERPARTY_FILE = DATA_DIR / "counterparty_patterns.json"
PDF_HEADER_FILE = CONFIG_DIR / "pdf_header.json"

def config_file(name: str) -> Path:
    """
    Hilfsfunktion für taxlogic etc.
    z.B. config_file("taxlogic_de.json")
    """
    return CONFIG_DIR / name
