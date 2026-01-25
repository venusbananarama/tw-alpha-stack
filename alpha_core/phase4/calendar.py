from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Set

import pandas as pd


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(str(value).strip(), fmt).date()
        except Exception:
            continue
    try:
        return pd.to_datetime(value, errors="raise").date()  # type: ignore[no-any-return]
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"invalid date value: {value}") from exc


def load_trading_days(calendar_csv: Path) -> Set[date]:
    if not calendar_csv.exists():
        raise FileNotFoundError(f"calendar not found: {calendar_csv}")
    df = pd.read_csv(calendar_csv)
    if "date" not in df.columns:
        raise ValueError(f"calendar missing 'date' column: {calendar_csv}")
    days: Set[date] = set()
    for v in df["date"].tolist():
        try:
            days.add(_parse_date(str(v)))
        except Exception:
            continue
    if not days:
        raise ValueError(f"calendar empty or invalid: {calendar_csv}")
    return days


def is_trading_day(as_of: str | date, trading_days: Iterable[date]) -> bool:
    target = _parse_date(as_of)
    return target in set(trading_days)


def nearest_weekly_anchor(as_of: str | date, anchor: str = "W-FRI") -> date:
    target = _parse_date(as_of)
    anchor = anchor.upper()
    if anchor != "W-FRI":
        raise ValueError(f"unsupported anchor: {anchor}")
    weekday = target.weekday()  # Monday=0
    delta = (weekday - 4) % 7
    return target - timedelta(days=delta)
