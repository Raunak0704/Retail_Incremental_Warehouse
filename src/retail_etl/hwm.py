from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


def lookback_lower_bound(last_success_ts: datetime, lookback_minutes: int) -> datetime:
    """Return the inclusive lower timestamp for overlap/replay extraction."""
    if last_success_ts.tzinfo is None:
        last_success_ts = last_success_ts.replace(tzinfo=timezone.utc)
    return last_success_ts - timedelta(minutes=lookback_minutes)


def tuple_is_after(left: tuple[datetime, int], right: tuple[datetime, int]) -> bool:
    """Lexicographic comparison used by the composite (updated_at, PK) HWM."""
    return left > right


def normalize_timestamp(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def serializable_watermark(value: dict[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    ts = value.get("updated_at")
    return {
        "updated_at": normalize_timestamp(ts).isoformat() if ts else None,
        "primary_key": int(value["primary_key"]) if value.get("primary_key") is not None else None,
    }
