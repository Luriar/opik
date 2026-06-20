from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import calendar


@dataclass(frozen=True)
class DateWindow:
    start: date
    end: date

    def as_dart_params(self) -> dict[str, str]:
        return {
            "bgn_de": self.start.strftime("%Y%m%d"),
            "end_de": self.end.strftime("%Y%m%d"),
        }


def add_months(value: date, months: int) -> date:
    month_index = value.month - 1 + months
    year = value.year + month_index // 12
    month = month_index % 12 + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def subtract_months(value: date, months: int) -> date:
    return add_months(value, -months)


def backfill_windows(today: date, months: int) -> list[DateWindow]:
    start = subtract_months(today, months)
    return date_range_windows(start, today)


def date_range_windows(start: date, end: date, months_per_window: int = 1) -> list[DateWindow]:
    if start > end:
        raise ValueError("start date must be <= end date")
    if months_per_window <= 0:
        raise ValueError("months_per_window must be >= 1")
    windows: list[DateWindow] = []
    cursor = start
    while cursor <= end:
        next_start = add_months(cursor, months_per_window)
        window_end = min(next_start - timedelta(days=1), end)
        windows.append(DateWindow(start=cursor, end=window_end))
        cursor = next_start
    return windows


def incremental_window(today: date, days: int) -> DateWindow:
    return DateWindow(start=today - timedelta(days=days), end=today)
