
import sys
from datetime import datetime

def log(msg, level="INFO"):
    ts = datetime.utcnow().strftime("[%H:%M:%S]")
    print(f"{ts} {msg}", file=sys.stdout)
