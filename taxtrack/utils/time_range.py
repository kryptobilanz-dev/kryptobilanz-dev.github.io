# taxtrack/utils/time_range.py
from datetime import datetime


def resolve_timerange(
    year: int | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
):
    """
    Liefert (ts_start, ts_end) als UNIX-Timestamps.

    Priorität:
    1) from_date + to_date
    2) year

    Raises:
        ValueError wenn nichts Gültiges angegeben ist
    """

    if from_date and to_date:
        try:
            start = int(datetime.fromisoformat(from_date).timestamp())
            end = int(datetime.fromisoformat(to_date).timestamp())
            if start >= end:
                raise ValueError("from_date >= to_date")
            return start, end
        except Exception as e:
            raise ValueError(f"Ungültiges Datum: {e}")

    if year is not None:
        try:
            start = int(datetime(year, 1, 1).timestamp())
            end = int(datetime(year + 1, 1, 1).timestamp())
            return start, end
        except Exception as e:
            raise ValueError(f"Ungültiges Jahr: {e}")

    raise ValueError("Kein gültiger Zeitraum angegeben (year oder from/to)")
