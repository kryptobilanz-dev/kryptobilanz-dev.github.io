"""
Price audit tooling (Phase 3).

Generates:
- Missing price report (token -> count, chain breakdown)
- Price anomaly report (swap imbalance beyond threshold, eur_value spikes)
- Simple cache / CSV validation checks

Does not change classification/swap logic; it reads harvested data and/or runs the pipeline.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from taxtrack.prices.token_mapper import map_token


def _repo_taxtrack_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _harvest_root() -> Path:
    return _repo_taxtrack_root() / "data" / "harvest"


def _prices_dir() -> Path:
    return _repo_taxtrack_root() / "data" / "prices"


def _price_cache_db() -> Path:
    return Path(__file__).resolve().parents[1] / "prices" / "cache" / "eur_price_cache.sqlite"


def iter_harvested_classified(year: int, *, wallets: Optional[List[str]] = None) -> Iterable[Dict[str, Any]]:
    root = _harvest_root()
    if not root.exists():
        return
    wallet_set = {w.lower() for w in wallets} if wallets else None
    for wallet_dir in root.iterdir():
        if not wallet_dir.is_dir():
            continue
        wallet = wallet_dir.name.lower()
        if wallet_set and wallet not in wallet_set:
            continue
        year_dir = wallet_dir / str(year)
        p = year_dir / "classified.json"
        if not p.exists():
            continue
        try:
            rows = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(rows, list):
            continue
        for d in rows:
            if isinstance(d, dict):
                yield d


def _meta(d: Dict[str, Any]) -> Dict[str, Any]:
    return d.get("meta") if isinstance(d.get("meta"), dict) else {}


def _ts_from_dt(dt_iso: str) -> int:
    if not dt_iso:
        return 0
    try:
        return int(datetime.fromisoformat(dt_iso.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def missing_price_report(rows: Iterable[Dict[str, Any]]) -> Dict[str, Any]:
    by_token = Counter()
    by_token_chain = defaultdict(Counter)

    for d in rows:
        tok_raw = (d.get("token") or "").strip().upper()
        tok = map_token(tok_raw) or tok_raw or "<empty>"
        chain = (d.get("chain_id") or "").strip().lower() or "<none>"
        try:
            amt = abs(float(d.get("amount") or 0.0))
        except Exception:
            amt = 0.0
        ev = d.get("eur_value")
        try:
            eur = float(ev) if ev is not None else None
        except Exception:
            eur = None

        if amt > 0 and (eur is None or eur <= 0):
            by_token[tok] += 1
            by_token_chain[tok][chain] += 1

    top = []
    for tok, cnt in by_token.most_common():
        top.append({"token": tok, "count": cnt, "chains": dict(by_token_chain[tok])})
    return {"tokens": top}


def anomaly_report(rows: Iterable[Dict[str, Any]], *, spike_threshold_eur: float = 1_000_000.0) -> Dict[str, Any]:
    anomalies: List[Dict[str, Any]] = []

    for d in rows:
        txh = (d.get("tx_hash") or "").strip()
        tok = (d.get("token") or "").strip().upper()
        chain = (d.get("chain_id") or "").strip().lower()
        cat = (d.get("category") or "").strip().lower()
        dirn = (d.get("direction") or "").strip().lower()
        meta = _meta(d)

        # eur_value spikes
        try:
            eur = float(d.get("eur_value") or 0.0)
        except Exception:
            eur = 0.0
        if eur >= spike_threshold_eur:
            anomalies.append({"tx_hash": txh, "token": tok, "chain": chain, "issue": f"eur_value_spike {eur}"})

        # swap imbalance check (if meta totals exist)
        if cat == "swap" and dirn == "swap" and isinstance(meta, dict):
            outv = meta.get("total_out_value_eur")
            inv = meta.get("total_in_value_eur")
            try:
                if outv is not None and inv is not None:
                    outv = float(outv)
                    inv = float(inv)
                    denom = max(outv, inv, 1e-9)
                    diff = abs(outv - inv) / denom
                    if diff > 0.10:
                        anomalies.append({"tx_hash": txh, "token": tok, "chain": chain, "issue": f"swap_imbalance {diff:.2%} out={outv} in={inv}"})
            except Exception:
                pass

    return {"anomalies": anomalies}


def cache_validation() -> Dict[str, Any]:
    db = _price_cache_db()
    if not db.exists():
        return {"db_exists": False}
    issues = []
    try:
        conn = sqlite3.connect(str(db))
        rows = conn.execute("SELECT key, payload FROM eur_prices").fetchall()
        conn.close()
        for key, payload in rows[:50000]:
            try:
                obj = json.loads(payload)
                price = obj.get("price")
                if price is not None and float(price) <= 0:
                    issues.append({"key": key, "issue": f"non_positive_cached_price {price}"})
            except Exception:
                issues.append({"key": key, "issue": "corrupt_payload"})
    except Exception as e:
        return {"db_exists": True, "error": str(e)}
    return {"db_exists": True, "issues_sample": issues[:50], "issues_count": len(issues)}


def csv_validation() -> Dict[str, Any]:
    root = _prices_dir()
    if not root.exists():
        return {"exists": False}
    bad_files = []
    for p in root.glob("*_eur_daily.csv"):
        try:
            txt = p.read_text(encoding="utf-8", errors="replace").splitlines()
            if not txt:
                bad_files.append({"file": p.name, "issue": "empty"})
                continue
            header = txt[0].lower()
            if "date" not in header or ("eur" not in header and "price" not in header):
                bad_files.append({"file": p.name, "issue": "bad_header"})
        except Exception:
            bad_files.append({"file": p.name, "issue": "read_error"})
    return {"exists": True, "bad_files": bad_files}


def main() -> None:
    p = argparse.ArgumentParser(description="Price audits: missing price + anomalies + cache validation.")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--wallet", default=None, help="Optional single wallet (address) to restrict scan")
    p.add_argument("--spike", type=float, default=1_000_000.0, help="EUR spike threshold")
    args = p.parse_args()

    wallets = [args.wallet.lower()] if args.wallet else None
    rows = list(iter_harvested_classified(int(args.year), wallets=wallets))

    miss = missing_price_report(rows)
    anom = anomaly_report(rows, spike_threshold_eur=float(args.spike))
    cache = cache_validation()
    csvv = csv_validation()

    print("MISSING PRICE REPORT")
    for it in miss["tokens"][:50]:
        print(f"- {it['token']}: {it['count']}  chains={it['chains']}")
    print()

    print("PRICE ANOMALY REPORT")
    for it in anom["anomalies"][:50]:
        print(f"- tx={it['tx_hash']} chain={it['chain']} token={it['token']} issue={it['issue']}")
    print(f"(total anomalies: {len(anom['anomalies'])})")
    print()

    print("PRICE CACHE VALIDATION")
    print(cache)
    print()

    print("PRICE CSV VALIDATION")
    print(csvv)


if __name__ == "__main__":
    main()

