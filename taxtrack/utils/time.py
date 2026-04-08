from datetime import datetime, timezone

def iso_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
