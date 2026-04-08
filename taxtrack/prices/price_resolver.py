# taxtrack/prices/price_resolver.py
from __future__ import annotations
from typing import Iterable, List, Dict, Any, Tuple

from .price_provider import PriceQuery, get_price, _build_key  # _build_key ist absichtlich "intern"


def resolve_prices_batch(queries: Iterable[PriceQuery]) -> List[Dict[str, Any]]:
    """
    Nimmt eine Menge PriceQuery-Objekte, dedupliziert sie per Key
    und ruft den Cache/Provider nur einmal pro Unique-Key auf.

    Rückgabe: Liste von Preis-Dicts in derselben Reihenfolge wie input-Queries.
    """
    queries_list: List[PriceQuery] = list(queries)

    # 1) Keys vorbereiten
    key_list: List[Tuple[str, PriceQuery]] = []
    for q in queries_list:
        key = _build_key(q)
        key_list.append((key, q))

    # 2) Unique Keys bestimmen
    unique_map: Dict[str, PriceQuery] = {}
    for key, q in key_list:
        if key not in unique_map:
            unique_map[key] = q

    # 3) Für jeden Unique-Key einmal get_price() aufrufen
    result_map: Dict[str, Dict[str, Any]] = {}
    for key, uq in unique_map.items():
        result_map[key] = get_price(uq)

    # 4) Ergebnisse wieder in Original-Reihenfolge abbilden
    result_list: List[Dict[str, Any]] = []
    for key, _q in key_list:
        result_list.append(result_map[key])

    return result_list
