from datetime import datetime, timezone

def align_to_interval(ts_ms: int, interval_min: int) -> int:
    interval_ms = interval_min * 60 * 1000
    return (ts_ms // interval_ms) * interval_ms


def now_ms():
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)