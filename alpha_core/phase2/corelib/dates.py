from __future__ import annotations

"""
alpha_core.phase2.corelib.dates

日期與時間規則的共用工具：
- Half-open 區間 [start, end)
- W-FRI 週錨
- Walk-forward window 計算
- 交易日工具（trading_days.csv）
"""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from calendar import monthrange
from typing import Iterable, List, Optional, Tuple, Set


@dataclass(frozen=True)
class DateRange:
    """
    Half-open interval [start, end).

    Invariants:
    - start < end
    - both are naive datetime.date (no timezone)
    """

    start: date
    end: date

    def __post_init__(self) -> None:
        if self.start >= self.end:
            raise ValueError(f"Invalid DateRange: start={self.start} must be < end={self.end}")

    def contains(self, d: date) -> bool:
        """Return True if d ∈ [start, end)."""
        return self.start <= d < self.end

    def to_tuple(self) -> Tuple[date, date]:
        """Return (start, end) tuple."""
        return self.start, self.end


# ---------- basic parse/format ----------


def parse_ymd(s: str) -> date:
    """
    Parse 'YYYY-MM-DD' into a date object.

    Raises ValueError if the string is not a valid date representation.
    """
    s = s.strip()
    return datetime.strptime(s, "%Y-%m-%d").date()


def format_ymd(d: date) -> str:
    """
    Format a date into 'YYYY-MM-DD'.
    """
    return d.strftime("%Y-%m-%d")


# ---------- W-FRI helpers ----------

_FRIDAY = 4  # Monday=0, Sunday=6


def get_weekly_friday(d: date) -> date:
    """
    Get the W-FRI anchor for the week containing `d`.

    Convention:
    - Monday–Friday: return that week's Friday (same calendar week).
    - Saturday/Sunday: return the *previous* Friday (the just-finished trading week).

    This matches a "week-ending Friday" convention commonly used in
    backtests and Gate as-of semantics.
    """
    wd = d.weekday()
    if wd <= _FRIDAY:
        # Go forward to Friday within the same week
        delta_days = _FRIDAY - wd
        return d + timedelta(days=delta_days)
    else:
        # Saturday(5) / Sunday(6): go back to previous Friday
        delta_days = wd - _FRIDAY
        return d - timedelta(days=delta_days)


def get_previous_weekly_friday(d: date) -> date:
    """
    Get the previous W-FRI strictly before the W-FRI of `d`.

    Example:
    - If d is 2025-11-10 (Mon, W-FRI=2025-11-14),
      then previous W-FRI is 2025-11-07.
    """
    current = get_weekly_friday(d)
    return current - timedelta(days=7)


def get_next_weekly_friday(d: date) -> date:
    """
    Get the next W-FRI strictly after the W-FRI of `d`.
    """
    current = get_weekly_friday(d)
    return current + timedelta(days=7)


# ---------- half-open intervals & shifts ----------


def make_half_open(start: date, end: date) -> DateRange:
    """
    Build a standard half-open interval [start, end).

    Raises:
        ValueError if start >= end.
    """
    return DateRange(start=start, end=end)


def shift_days(d: date, days: int) -> date:
    """
    Shift a date by a number of days (positive or negative).
    """
    return d + timedelta(days=days)


def shift_months(d: date, months: int) -> date:
    """
    Shift a date by a number of calendar months, clamping the day
    to the last valid day of the target month when necessary.

    Example:
        2025-01-31 + 1 month -> 2025-02-28 (if Feb has 28 days)
    """
    # Convert to "months since year 0"
    total_months = d.year * 12 + (d.month - 1) + months
    if total_months < 0:
        raise ValueError(f"Resulting month is before year 1: d={d}, months={months}")

    new_year = total_months // 12
    new_month = total_months % 12 + 1
    last_day = monthrange(new_year, new_month)[1]
    new_day = min(d.day, last_day)
    return date(new_year, new_month, new_day)


def get_wf_window(as_of: date, months: int) -> DateRange:
    """
    Construct a WF window for the given as_of (usually W-FRI) and window length in months.

    Convention:
    - Start = as_of shifted by -months (calendar months), clamped to valid day.
    - End   = as_of + 1 day (since we use half-open [start, end)).

    So for a 6-month window with as_of=2025-11-14 (Fri),
    the window is roughly [~2025-05-14, 2025-11-15).
    """
    if months <= 0:
        raise ValueError(f"WF window length must be positive months, got {months}")

    start = shift_months(as_of, -months)
    end = as_of + timedelta(days=1)
    return DateRange(start=start, end=end)


# ---------- trading days helpers ----------


def _to_sorted_unique_days(days: Iterable[date]) -> List[date]:
    unique: Set[date] = set(days)
    return sorted(unique)


def load_trading_days(path: str) -> List[date]:
    """
    Load trading days from a text/CSV file.

    This function is intentionally tolerant:
    - It scans each non-empty line.
    - It tries to parse the first comma-separated token as 'YYYY-MM-DD'.
    - Lines that cannot be parsed are ignored (e.g., header row).

    Returns:
        Sorted list of unique trading dates.
    """
    results: List[date] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            token = line.split(",")[0].strip()
            try:
                d = parse_ymd(token)
            except ValueError:
                # probably header or malformed line; ignore
                continue
            results.append(d)
    return _to_sorted_unique_days(results)


def is_trading_day(d: date, trading_days: Iterable[date]) -> bool:
    """
    Check if d is in the given trading day set/list.
    """
    # Convert to set for O(1) lookup; if caller cares about performance,
    # they can pass in a pre-built set and we won't re-wrap it.
    if isinstance(trading_days, set):
        return d in trading_days  # type: ignore[arg-type]
    return d in set(trading_days)


def get_next_trading_day(
    d: date,
    trading_days: Iterable[date],
) -> Optional[date]:
    """
    Get the next trading day >= d.

    Returns:
        The next trading date if exists, otherwise None.
    """
    days_sorted = _to_sorted_unique_days(trading_days)
    for td in days_sorted:
        if td >= d:
            return td
    return None


def get_prev_trading_day(
    d: date,
    trading_days: Iterable[date],
) -> Optional[date]:
    """
    Get the previous trading day <= d.

    Returns:
        The previous trading date if exists, otherwise None.
    """
    days_sorted = _to_sorted_unique_days(trading_days)
    prev: Optional[date] = None
    for td in days_sorted:
        if td > d:
            break
        prev = td
    return prev
