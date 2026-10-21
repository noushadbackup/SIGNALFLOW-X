"""Event-time policy (ADR-0004): classify lateness against the stream clock; the watermark decides KPI finality."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

DHAKA = timezone(timedelta(hours=6), "Asia/Dhaka")
ALLOWED_LATENESS = timedelta(hours=6)
ON_TIME_TOLERANCE = timedelta(minutes=5)

ON_TIME = "ON_TIME"
LATE_WITHIN = "LATE_WITHIN_WATERMARK"
LATE_BEYOND = "LATE_BEYOND_WATERMARK"


def classify(event_time: datetime, stream_max_at_batch_start: datetime | None,
             allowed_lateness: timedelta = ALLOWED_LATENESS) -> tuple[str, int]:
    """Return (arrival_class, lateness_seconds). The stream clock is the max accepted event time *before* this
    micro-batch, so the result does not depend on row order within the batch."""
    if stream_max_at_batch_start is None:
        return ON_TIME, 0
    lateness = stream_max_at_batch_start - event_time
    seconds = max(0, int(lateness.total_seconds()))
    if lateness <= ON_TIME_TOLERANCE:
        return ON_TIME, seconds
    if lateness <= allowed_lateness:
        return LATE_WITHIN, seconds
    return LATE_BEYOND, seconds


def business_date(ts: datetime) -> date:
    return ts.astimezone(DHAKA).date()


def business_day_end(day: date) -> datetime:
    """First instant after the Dhaka business day, in UTC."""
    return datetime(day.year, day.month, day.day, tzinfo=DHAKA) + timedelta(days=1)


def is_final(day: date, watermark: datetime | None) -> bool:
    return watermark is not None and watermark >= business_day_end(day)
