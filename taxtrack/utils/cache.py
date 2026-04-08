from pathlib import Path
import json
import hashlib

CACHE_ROOT = Path(__file__).resolve().parents[2] / "data" / "cache" / "evm"
CACHE_ROOT.mkdir(parents=True, exist_ok=True)

def _key(wallet: str, chain: str, ts_start: int, ts_end: int) -> str:
    s = f"{wallet}|{chain}|{ts_start}|{ts_end}"
    return hashlib.sha256(s.encode()).hexdigest()[:16]

def cache_path(wallet: str, chain: str, ts_start: int, ts_end: int) -> Path:
    return CACHE_ROOT / f"{_key(wallet, chain, ts_start, ts_end)}.json"

def load_cache(wallet: str, chain: str, ts_start: int, ts_end: int):
    p = cache_path(wallet, chain, ts_start, ts_end)
    if p.exists():
        return json.loads(p.read_text())
    return None

def save_cache(wallet: str, chain: str, ts_start: int, ts_end: int, payload: dict):
    p = cache_path(wallet, chain, ts_start, ts_end)
    p.write_text(json.dumps(payload, default=str))
