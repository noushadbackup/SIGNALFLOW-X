from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

import pytest

from signalflow.processing.lateness import (
    LATE_BEYOND,
    LATE_WITHIN,
    ON_TIME,
    business_date,
    business_day_end,
    classify,
    is_final,
)

T = datetime(2026, 10, 2, 12, 0, tzinfo=UTC)


def test_first_batch_has_no_clock() -> None:
    assert classify(T - timedelta(days=400), None) == (ON_TIME, 0)


@pytest.mark.parametrize(
    ("behind", "expected"),
    [
        (timedelta(seconds=-30), ON_TIME),  # newer than the clock
        (timedelta(0), ON_TIME),
        (timedelta(minutes=5), ON_TIME),  # tolerance boundary is inclusive
        (timedelta(minutes=5, seconds=1), LATE_WITHIN),
        (timedelta(hours=6), LATE_WITHIN),  # allowed lateness boundary is inclusive
        (timedelta(hours=6, seconds=1), LATE_BEYOND),
        (timedelta(days=3), LATE_BEYOND),
    ],
)
def test_classification_boundaries(behind: timedelta, expected: str) -> None:
    cls, seconds = classify(T - behind, T)
    assert cls == expected
    assert seconds == max(0, int(behind.total_seconds()))


def test_business_date_uses_dhaka() -> None:
    assert business_date(datetime(2026, 10, 1, 17, 59, tzinfo=UTC)) == date(2026, 10, 1)
    assert business_date(datetime(2026, 10, 1, 18, 0, tzinfo=UTC)) == date(2026, 10, 2)


def test_day_end_and_finality() -> None:
    end = business_day_end(date(2026, 10, 1))
    assert end == datetime(2026, 10, 1, 18, 0, tzinfo=UTC)
    assert not is_final(date(2026, 10, 1), None)
    assert not is_final(date(2026, 10, 1), end - timedelta(milliseconds=1))
    assert is_final(date(2026, 10, 1), end)
