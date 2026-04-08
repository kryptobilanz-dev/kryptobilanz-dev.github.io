# taxtrack/analyze/swap_engine.py

from __future__ import annotations
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple
from decimal import Decimal
from pathlib import Path
import json

from taxtrack.utils.contract_labeler import label_address as cl_label_address
from taxtrack.schemas.RawRow import RawRow  # dein bestehendes Datamodel :contentReference[oaicite:1]{index=1}
from taxtrack.analyze.restake_engine import lrt_symbol


# ------------------------------------------------------
# Datentypen
# ------------------------------------------------------

@dataclass
class SwapEvent:
    """
    Aggregiertes Swap-Ereignis für steuerliche Betrachtung.
    Eine Transaktion (tx_hash) → genau ein SwapEvent.
    """

    # ------------------------------------------
    # Pflichtfelder (ohne Defaults!)
    # ------------------------------------------
    tx_hash: str
    timestamp: int

    token_in: str
    amount_in: float

    token_out: str
    amount_out: float

    gas_token: Optional[str]
    gas_amount: float

    # ------------------------------------------
    # Default-Felder
    # ------------------------------------------
    eur_in: float = 0.0
    eur_out: float = 0.0
    eur_fee: float = 0.0

    pnl: float = 0.0
    taxable: bool = True
    category: str = "swap"

    meta: Optional[Dict] = None

    # Chain + Wallet Info
    chain: str = ""
    wallet: str = ""

    # vollständige Legs der TX
    legs: Optional[List[RawRow]] = None



# ------------------------------------------------------
# Hilfsfunktionen: Adress-Meta (Router / Bridge / LP / Restake)
# ------------------------------------------------------

def _load_address_map() -> Dict:
    """
    Lädt taxtrack/data/config/address_map.json
    und gibt das Dict zurück.
    """
    # taxtrack/analyze/swap_engine.py → /taxtrack → /taxtrack/data/config
    base = Path(__file__).resolve().parents[1]
    cfg_path = base / "data" / "config" / "address_map.json"
    if not cfg_path.exists():
        return {}
    with cfg_path.open("r", encoding="utf-8") as f:
        return json.load(f)


_ADDRESS_MAP: Dict = _load_address_map()


def _get_addr_info(chain_id: str, addr: str) -> Optional[Dict]:
    """
    Liefert die Meta-Info aus address_map.json für eine Adresse zurück,
    über den zentralen contract_labeler (NORMALIZED_MAP).
    Unterstützt sowohl chain_id="eth" als auch "1" usw.
    """
    if not addr:
        return None

    # contract_labeler erwartet: addr zuerst, dann chain
    info = cl_label_address(addr, chain_id or "eth")

    if not info:
        return None

    # label_address gibt immer ein dict zurück (oder {}), wir wollen None wenn leer
    if isinstance(info, dict) and info:
        return info

    return None


def _is_router(chain_id: str, addr: str) -> bool:
    info = _get_addr_info(chain_id, addr)
    if not info:
        return False
    proto = (info.get("protocol") or "").lower()
    typ = (info.get("type") or "").lower()
    tags = [t.lower() for t in info.get("tags", [])]
    return (
        proto in {"dex", "router", "aggregator"} 
        or typ in {"router", "aggregator"}
        or "swap" in tags
    )


def _is_bridge(chain_id: str, addr: str) -> bool:
    info = _get_addr_info(chain_id, addr)
    if not info:
        return False
    proto = (info.get("protocol") or "").lower()
    tags = [t.lower() for t in info.get("tags", [])]
    return proto == "bridge" or "bridge" in tags or "crosschain" in tags


def _is_lp_related(chain_id: str, addr: str) -> bool:
    info = _get_addr_info(chain_id, addr)
    if not info:
        return False
    proto = (info.get("protocol") or "").lower()
    typ = (info.get("type") or "").lower()
    tags = [t.lower() for t in info.get("tags", [])]
    return (
        proto in {"lp", "dex"}
        and (typ in {"pool", "lp_manager"} or "liquidity" in tags or "lp" in tags)
    )


# ------------------------------------------------------
# Gruppierung nach tx_hash
# ------------------------------------------------------

def group_by_tx(raw_rows: List[RawRow]) -> Dict[str, List[RawRow]]:
    """
    Gruppiert RawRows nach tx_hash.
    """
    groups: Dict[str, List[RawRow]] = {}
    for r in raw_rows:
        key = r.tx_hash or ""
        if not key:
            # Ohne Hash können wir keine TX-basierten Swaps erkennen
            continue
        groups.setdefault(key, []).append(r)
    return groups


# ------------------------------------------------------
# Swap-Erkennung
# ------------------------------------------------------

def _split_legs(events: List[RawRow]) -> Tuple[List[RawRow], List[RawRow]]:
    ins = [e for e in events if e.direction == "in" and e.amount > 0]
    outs = [e for e in events if e.direction == "out" and e.amount > 0]
    return ins, outs


def _has_router_involved(chain_id: str, events: List[RawRow]) -> bool:
    for e in events:
        if _is_router(chain_id, e.to_addr) or _is_router(chain_id, e.from_addr):
            return True
    # Fallback: Method / Category Heuristik
    for e in events:
        m = (e.method or "").lower()
        c = (e.category or "").lower()
        if any(x in m for x in ["swap", "multicall", "start via rubic", "execute order"]):
            return True
        if "swap" in c:
            return True
    return False


def _looks_like_bridge(chain_id: str, events: List[RawRow]) -> bool:
    """
    Grobe Heuristik: Bridge-Transaktionen NICHT als Swap behandeln.
    """
    for e in events:
        if _is_bridge(chain_id, e.to_addr) or _is_bridge(chain_id, e.from_addr):
            return True
        m = (e.method or "").lower()
        if "bridge" in m or "send to injective" in m:
            return True
        if "bridge_out" in (e.category or "").lower():
            return True
    return False


def detect_swap_tx(chain_id: str, events: List[RawRow]) -> bool:
    """
    Der zuverlässigste Swap-Detector:
    Ein Swap liegt vor, wenn innerhalb einer TX mindestens
    EIN in-Leg und EIN out-Leg existieren.
    """

    has_in  = any(e.direction == "in"  for e in events)
    has_out = any(e.direction == "out" for e in events)

    if has_in and has_out:
        return True

    # Fallback: wenn Methode im NORMAL-File Swap war
    if any((e.method or "").lower() == "swap" for e in events):
        return True


    # ---------------------------------------------------------
    # 1) Wenn category = "swap" → sicher ein Swap
    # ---------------------------------------------------------
    for e in events:
        cat = (e.category or "").lower()
        if cat == "swap":
            return True

        # Manche Loader tragen Swap in "method" ein
        method = (e.method or "").lower()
        if method.startswith("swap"):
            return True

    # ---------------------------------------------------------
    # 2) ETH-Swap → nur ein ERC20-Leg sichtbar
    # Beispiel: ETH → USDT oder USDT → ETH
    # ---------------------------------------------------------
    erc20_legs = [
        e for e in events 
        if e.token and e.token.upper() not in ("ETH", "WETH")
    ]
    if len(erc20_legs) == 1:
        return True

    # ---------------------------------------------------------
    # 3) Normaler Swap: mindestens ein IN und ein OUT
    # ---------------------------------------------------------
    ins =  [e for e in events if (e.direction or "") == "in"]
    outs = [e for e in events if (e.direction or "") == "out"]

    if ins and outs:
        return True

    # ---------------------------------------------------------
    # 4) Multi-Hop Detection (≥ 3 verschiedene Token)
    # ---------------------------------------------------------
    unique_tokens = { (e.token or "").upper() for e in events if e.token }
    if len(unique_tokens) >= 3:
        return True

    return False


def _restake_base_from_events(contract_addr: str, events: List[RawRow]) -> str:
    """
    Pick the row symbol for this contract with the largest absolute amount (deterministic).
    Fallback ``ETH`` matches the old generic RESTAKE_ETH bucket as ``LRT_ETH``.
    """
    addr_l = (contract_addr or "").lower()
    best_tok, best_amt = "", 0.0
    for e in events:
        if (e.contract_addr or "").lower() != addr_l:
            continue
        t = (e.token or "").strip().upper()
        if not t or t.startswith("UNKNOWN"):
            continue
        try:
            a = abs(float(e.amount or 0.0))
        except (TypeError, ValueError):
            a = 0.0
        if a >= best_amt:
            best_amt = a
            best_tok = t
    return best_tok or "ETH"


# ------------------------------------------------------
# SwapEvent-Konstruktion
# ------------------------------------------------------
def resolve_token(chain_id: str, contract_addr: str, events: List[RawRow]) -> str:
    """
    Oberste Token-Resolver-Logik:
      1. Restake / EigenLayer → canonical ``LRT_{base}`` (same as restake_engine / FIFO)
      2. Pendle Token (LP/PT/YT/SY) → kanonische Namen wie PENDLE_LPT
      3. Normale address_map-Labels
      4. Restake-Fallback
      5. Symbol-Fallback über RawRows
      6. Fallback UNKNOWN_CONTRACT
    """

    # 0) NULL-Check
    if not contract_addr:
        return "UNKNOWN_CONTRACT"

    addr_l = contract_addr.lower()

    # 1) Meta aus address_map / contract_labeler
    info = _get_addr_info(chain_id, contract_addr) or {}
    proto = (info.get("protocol") or "").lower()
    tags = [t.lower() for t in info.get("tags", [])]
    label_raw = info.get("label") or ""
    label = label_raw.upper()

    # 1A) Restake / Eigenlayer immer zuerst
    if proto == "restake" or "restake" in tags or "eigenlayer" in tags:
        return lrt_symbol(_restake_base_from_events(contract_addr, events))

    # 1B) Pendle → erst kanonische Pendle-Token-Namen
    is_pendle = "pendle" in tags or "pendle" in label_raw.lower()
    if is_pendle:
        pendle = resolve_pendle_token(events, contract_addr)
        if pendle:
            return pendle

    # 1C) Normales Label (nicht-Pendle)
    if label and not is_pendle:
        return label

    # 2) Pendle-Resolver auch ohne vollständige address_map
    pendle = resolve_pendle_token(events, contract_addr)
    if pendle:
        return pendle

    # 3) Restake-Fallback (falls oben nur Tags o.Ä. gesetzt waren)
    restake = resolve_restake_token(contract_addr, chain_id, events)
    if restake:
        return restake

    # 4) Symbol-Fallback über die Events
    #    Suche alle RawRows mit derselben contract_addr und sammle deren Token-Symbole.
    symbols = {
        (e.token or "").upper()
        for e in events
        if (e.contract_addr or "").lower() == addr_l
    }
    # Offensichtlichen Müll rausfiltern
    symbols = {s for s in symbols if s and not s.startswith("UNKNOWN")}

    if len(symbols) == 1:
        # Eindeutig – nimm dieses Symbol als Token
        return symbols.pop()

    # 5) Wenn mehrere Symbole gefunden wurden, aber eines dominiert,
    #    könnte man hier noch eine Heuristik ergänzen.
    #    Für den Moment bleiben wir konservativ und fallen auf UNKNOWN zurück.
    return "UNKNOWN_CONTRACT"


def build_swap_event(chain_id: str, events: List[RawRow]) -> Optional[SwapEvent]:
    """
    Baut aus einer Gruppe von RawRows (ein tx_hash) ein SwapEvent.
    detect_swap_tx() war bereits erfolgreich.
    """

    ins, outs = _split_legs(events)
    if not ins or not outs:
        return None

    # --------------------------------------------
    # 1) Timestamp bestimmen
    # --------------------------------------------
    ts = min(e.timestamp for e in events if e.timestamp)

    # --------------------------------------------
    # 2) Nur echte ERC20-Legs dürfen MainLeg werden
    # --------------------------------------------
    def is_real_token_leg(e: RawRow) -> bool:
        # Ein echtes Swap-Asset MUSS ein ERC20 sein:
        # 1. contract_addr != None
        # 2. token != ETH/WETH (native)
        return (
            bool(e.contract_addr)
            and e.token.upper() not in ("ETH", "WETH")
        )

    # OUT (verkauftes Asset)
    erc20_outs = [e for e in outs if is_real_token_leg(e)]
    if erc20_outs:
        main_out = max(erc20_outs, key=lambda e: e.amount)
    else:
        # Fallback: z. B. reiner ETH-Swap
        main_out = max(outs, key=lambda e: e.amount)

    # IN (erhaltenes Asset)
    erc20_ins = [e for e in ins if is_real_token_leg(e)]
    if erc20_ins:
        main_in = max(erc20_ins, key=lambda e: e.amount)
    else:
        main_in = max(ins, key=lambda e: e.amount)

    # --------------------------------------------
    # 3) Token-Auflösung
    # --------------------------------------------
    # OUT-Token
    if main_out.contract_addr:
        token_in = resolve_token(chain_id, main_out.contract_addr, events)
    else:
        token_in = main_out.token.upper()

    # IN-Token
    if main_in.contract_addr:
        token_out = resolve_token(chain_id, main_in.contract_addr, events)
    else:
        token_out = main_in.token.upper()
    
    token_in = normalize_token(token_in)
    token_out = normalize_token(token_out)
    
    # --------------------------------------------
    # 4) Gas aggregieren
    # --------------------------------------------
    gas_token = None
    gas_amount = 0.0
    for e in events:
        if e.fee_amount > 0:
            gas_token = e.fee_token or gas_token
            gas_amount += e.fee_amount
    
    # --------------------------------------------
    # 5) EUR-Werte
    # --------------------------------------------
    eur_out = sum((e.eur_value or 0.0) for e in outs)
    eur_in  = sum((e.eur_value or 0.0) for e in ins)
    eur_fee = sum((e.fee_eur  or 0.0) for e in events)

    pnl = eur_in - eur_out - eur_fee
    # --------------------------------------------------
    # Stake / Restake Detection (ETH → LST)
    # --------------------------------------------------
    _stake_bases = frozenset({"STETH", "RETH", "SWETH", "WEETH", "EZETH", "RSWETH", "EETH"})
    STAKE_TOKENS = set(_stake_bases) | {lrt_symbol(b) for b in _stake_bases} | {lrt_symbol("ETH")}

    if token_in in {"ETH", "WETH"} and token_out in STAKE_TOKENS:
        return SwapEvent(
            tx_hash=main_out.tx_hash,
            timestamp=ts,

            token_in=token_in,
            amount_in=float(main_out.amount),

            token_out=token_out,
            amount_out=float(main_in.amount),

            gas_token=gas_token,
            gas_amount=float(gas_amount),

            eur_in=eur_in,
            eur_out=eur_out,
            eur_fee=eur_fee,
            pnl=0.0,

            taxable=False,
            category="stake_deposit",

            meta={
                "type": "stake",
                "protocol": token_out,
                "tx_event_count": len(events),
            },

            chain=chain_id,
            wallet=main_out.from_addr,
            legs=events,
        )

    
    # --------------------------------------------------
    # No-Swap-Erkennung: gleicher Token IN == OUT
    # → Interest / Rebase / Internal
    # --------------------------------------------------
    if token_in == token_out:
        return None
    
    # --------------------------------------------------
    # Dust / Fake OUT-Leg eliminieren
    # --------------------------------------------------
    DUST_THRESHOLD = 1e-8

    if token_out == "UNKNOWN" or float(main_in.amount) < DUST_THRESHOLD:
        return None
    
    # --------------------------------------------------
    # UNKNOWN Resolver 2.0 (Majority Vote über Legs)
    # --------------------------------------------------
    if token_out == "UNKNOWN" and events:
        counts = {}
        for e in events:
            sym = normalize_token(e.token or "")
            if sym and sym != "UNKNOWN":
                counts[sym] = counts.get(sym, 0) + 1
        if counts:
            token_out = max(counts, key=counts.get)

    if token_in == "UNKNOWN" and events:
        counts = {}
        for e in events:
            sym = normalize_token(e.token or "")
            if sym and sym != "UNKNOWN":
                counts[sym] = counts.get(sym, 0) + 1
        if counts:
            token_in = max(counts, key=counts.get)

    
    # --------------------------------------------------
    # Steuer-Kategorie ableiten
    # --------------------------------------------------
    if token_in in {"USDC", "USDT"} and token_out in {"USDC", "USDT"}:
        category = "stable_swap"
    elif token_in.startswith("PENDLE") or token_out.startswith("PENDLE"):
        category = "pendle_swap"
    elif token_in in {"ETH", "WETH"} or token_out in {"ETH", "WETH"}:
        category = "eth_swap"
    else:
        category = "swap"


    # --------------------------------------------
    # 6) SwapEvent zurückgeben
    # --------------------------------------------
    return SwapEvent(
        tx_hash=main_out.tx_hash,
        timestamp=ts,
        token_in=token_in,
        amount_in=float(main_out.amount),
        token_out=token_out,
        amount_out=float(main_in.amount),
        gas_token=gas_token,
        gas_amount=float(gas_amount),
        eur_in=float(eur_in),
        eur_out=float(eur_out),
        eur_fee=float(eur_fee),
        pnl=float(pnl),
        taxable=True,
        category="swap",
        meta={
            "contract_in": main_out.contract_addr,
            "contract_out": main_in.contract_addr,
            "tx_event_count": len(events),
        },
        chain=chain_id,
        wallet=main_out.from_addr,
        legs=events      # 👈 KORREKT — KEINE TYPDEKLARATION HIER!
    )


    
def collapse_multihop(swaps: List[SwapEvent]) -> List[SwapEvent]:
    """
    Veredelte Multi-Hop-Erkennung:
    - erkennt Swap-Ketten innerhalb einer Transaktion
    - eliminiert reine Routing-Hops (Tokens, die sowohl IN als auch OUT sind)
    - übrig bleibt genau 1 finaler Swap pro TX
    """

    if not swaps:
        return swaps

    # Multi-Hop betrifft IMMER dieselbe Transaction
    tx_hash = swaps[0].tx_hash
    if any(s.tx_hash != tx_hash for s in swaps):
        return swaps

    # Extrahiere alle Hops
    hops = swaps
    
    # Safety: Tokens normalisieren, bevor wir Routing-Tokens bestimmen
    for s in hops:
        s.token_in = normalize_token(s.token_in)
        s.token_out = normalize_token(s.token_out)


    # Sammle IN- und OUT-Tokens aller Hops
    all_ins = [s.token_in for s in hops]
    all_outs = [s.token_out for s in hops]

    # Routing Tokens = Tokens, die IN und OUT sind → ignorieren
    routing_tokens = {t for t in all_ins if t in all_outs}

    final_ins  = [t for t in all_ins  if t not in routing_tokens]
    final_outs = [t for t in all_outs if t not in routing_tokens]

    # Kein Multi-Hop → Original
    if not final_ins or not final_outs:
        return swaps

    # Wähle ökonomischen Start & Ziel
    src = final_ins[0]
    dst = final_outs[-1]

    s0 = hops[0]
    sN = hops[-1]

    # Erzeuge ein konsolidiertes SwapEvent
    collapsed = SwapEvent(
        tx_hash=s0.tx_hash,
        timestamp=s0.timestamp,

        token_in=src,
        amount_in=s0.amount_in,

        token_out=dst,
        amount_out=sN.amount_out,

        gas_token=s0.gas_token or sN.gas_token,
        gas_amount=(s0.gas_amount or 0) + (sN.gas_amount or 0),

        eur_in=s0.eur_in,
        eur_out=sN.eur_out,
        eur_fee=s0.eur_fee + sN.eur_fee,
        pnl=s0.pnl + sN.pnl,

        taxable=True,
        category="swap",
        meta={"multi_hop": True, "route": [(s.token_in, s.token_out) for s in hops]},

        chain=s0.chain,
        wallet=s0.wallet,
    )

    return [collapsed]

import unicodedata

# ------------------------------------------------------
# Token Normalization (Fixes für CSV/Scanner/Unicode)
# ------------------------------------------------------

_TOKEN_FIX = {
    # Stablecoin-Quirks
    "USD": "USDC",       # dein Export zeigt "USD" obwohl USDC gemeint ist
    "USDT0": "USDT",
    "USDC.E": "USDC",
    "USDC.E ": "USDC",

    # Kyrillische Lookalikes / Scam-Symbole (aus deinen Logs)
    "UЅDС": "USDC",
    "UЅDТ": "USDT",
    "WЕТН": "WETH",
    "EТH": "ETH",

    # Platzhalter / Müll
    "ERC-20 TOKEN*": "UNKNOWN",
    "ERC20 ***": "UNKNOWN",
    "UNKNOWN_CONTRACT": "UNKNOWN",
    # Legacy swap resolver bucket → canonical LRT (FIFO)
    "RESTAKE_ETH": "LRT_ETH",
}

# minimale “confusable” Normalisierung (nur die Zeichen, die bei dir vorkommen)
_CONFUSABLES = str.maketrans({
    "Ѕ": "S", "Т": "T", "Н": "H", "Е": "E", "О": "O", "С": "C",
    "І": "I", "А": "A", "В": "B", "М": "M", "Р": "P", "К": "K", "Х": "X",
    "е": "e", "о": "o", "с": "c", "р": "p", "х": "x", "у": "y", "а": "a",
})

def normalize_token(sym: str) -> str:
    if not sym:
        return "UNKNOWN"
    s = sym.strip().upper()
    s = unicodedata.normalize("NFKC", s).translate(_CONFUSABLES).strip().upper()
    return _TOKEN_FIX.get(s, s)


def extract_swaps(chain_id: str, raw_rows: List[RawRow]) -> List[SwapEvent]:
    """
    High-Level API:
      - gruppiert nach tx_hash
      - erkennt Swaps
      - baut SwapEvents
    """
    result: List[SwapEvent] = []
    groups = group_by_tx(raw_rows)

    for tx_hash, events in groups.items():
        if detect_swap_tx(chain_id, events):
            se = build_swap_event(chain_id, events)
            if se:
                result.append(se)

    # Multi-Hop pro TX collapsen
    collapsed = []
    groups = {}

    # Gruppieren nach tx_hash
    for s in result:
        groups.setdefault(s.tx_hash, []).append(s)

    for tx_hash, swap_list in groups.items():
        collapsed.extend(collapse_multihop(swap_list))

    return collapsed

def resolve_contract_token(chain_id: str, addr: str) -> Optional[str]:
    """
    Erkennt Token anhand der ContractAddress via address_map.json.
    Wenn label vorhanden → nutze dieses als Token.
    """
    if not addr:
        return None

    info = _get_addr_info(chain_id, addr)
    if not info:
        return None

    # Falls string → einfacher Name
    if isinstance(info, str):
        return info.upper()

    # Falls dict mit label
    label = info.get("label")
    if label:
        return label.upper()

    return None

def resolve_pendle_token(events: List[RawRow], addr: str) -> Optional[str]:
    """
    Erkennt Pendle Tokenarten:
      - Pendle LP Token
      - Pendle PT
      - Pendle YT
      - Pendle SY
    """

    # 1) Chain sicher bestimmen
    chain_id = "eth"
    if events and events[0].meta:
        chain_id = events[0].meta.get("chain_id", "eth")

    # 2) Address-Info laden
    info = _get_addr_info(chain_id, addr)
    if not info:
        return None

    # 3) Label & Tags
    tags = [t.lower() for t in info.get("tags", [])]
    label = (info.get("label") or "")
    label_u = label.upper()

    # 4) Harte Pendle-Abkürzungen
    if "LPT" in label_u or "LP" in tags:
        return "PENDLE_LPT"

    if label_u.startswith("PT") or "principal" in tags:
        return "PENDLE_PT"

    if label_u.startswith("YT") or "yield" in tags:
        return "PENDLE_YT"

    if label_u.startswith("SY") or "sy" in tags:
        return "PENDLE_SY"

    return None

def resolve_restake_token(addr: str, chain_id: str, events: List[RawRow]) -> Optional[str]:
    info = _get_addr_info(chain_id, addr)
    if not info:
        return None

    proto = (info.get("protocol") or "").lower()
    tags = [t.lower() for t in info.get("tags", [])]

    if proto == "restake" or "restake" in tags or "eigenlayer" in tags:
        return lrt_symbol(_restake_base_from_events(addr, events))

    return None
    
def find_unknown_swaps(chain_id: str, raw_rows: List[RawRow]) -> List[Dict]:
    """
    Hilfsfunktion für Debug-Zwecke:
    - gruppiert RawRows nach tx_hash
    - erkennt Swaps über detect_swap_tx()
    - baut SwapEvents
    - gibt alle Swaps zurück, bei denen token_in oder token_out UNKNOWN_CONTRACT ist
    """
    result: List[Dict] = []

    groups = group_by_tx(raw_rows)

    for tx_hash, events in groups.items():
        # Nur echte Swaps betrachten
        if not detect_swap_tx(chain_id, events):
            continue

        se = build_swap_event(chain_id, events)
        if not se:
            continue

        if "UNKNOWN" in (se.token_in, se.token_out):
            # Sammle nützliche Debug-Infos
            contracts = sorted({
                (e.token or "", (e.contract_addr or "").lower())
                for e in events
                if e.contract_addr
            })
            tokens = sorted({(e.token or "").upper() for e in events if e.token})

            result.append({
                "tx_hash": se.tx_hash,
                "token_in": se.token_in,
                "token_out": se.token_out,
                "contract_in": se.meta.get("contract_in") if se.meta else None,
                "contract_out": se.meta.get("contract_out") if se.meta else None,
                "raw_tokens": tokens,
                "raw_contracts": contracts,
                "event_count": len(events),
            })

    return result


