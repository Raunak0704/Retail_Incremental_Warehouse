from datetime import datetime, timedelta, timezone

from retail_etl.hwm import lookback_lower_bound, tuple_is_after


def test_lookback_window_is_timezone_aware_and_inclusive():
    mark = datetime(2026, 11, 18, 12, 0, tzinfo=timezone.utc)
    assert lookback_lower_bound(mark, 5) == mark - timedelta(minutes=5)


def test_composite_watermark_orders_same_timestamp_by_primary_key():
    ts = datetime(2026, 11, 18, tzinfo=timezone.utc)
    assert tuple_is_after((ts, 11), (ts, 10))
    assert not tuple_is_after((ts, 10), (ts, 11))
    assert tuple_is_after((ts + timedelta(microseconds=1), 1), (ts, 999))
