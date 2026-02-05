from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


DATE_TZ_NAME = "Asia/Taipei"


@dataclass(frozen=True)
class TradingCalendar:
    path: Path
    dates: List[date]


def resolve_calendar_path(repo_root: Path) -> Path:
    candidates = [
        repo_root / "datahub" / "ref" / "trading_days.csv",
        repo_root / "cal" / "trading_days.csv",
    ]
    for p in candidates:
        if p.is_file():
            return p
    raise FileNotFoundError(
        "trading_days.csv not found under datahub/ref or cal"
    )


def _load_csv_dates(path: Path) -> List[date]:
    df = pd.read_csv(path)
    if df.empty:
        raise ValueError(f"trading_days.csv is empty: {path}")
    col = "date" if "date" in df.columns else df.columns[0]
    s = pd.to_datetime(df[col], errors="coerce").dt.date
    dates = [d for d in s.tolist() if isinstance(d, date)]
    if not dates:
        raise ValueError(f"trading_days.csv has no valid dates: {path}")
    return sorted(set(dates))


def load_trading_calendar(repo_root: Path) -> TradingCalendar:
    path = resolve_calendar_path(repo_root)
    dates = _load_csv_dates(path)
    return TradingCalendar(path=path, dates=dates)


def today_local(tz_name: str = DATE_TZ_NAME) -> date:
    if ZoneInfo is None:
        return datetime.now().date()
    tz = ZoneInfo(tz_name)
    return datetime.now(tz).date()


def cap_dates(dates: Iterable[date], cap: date) -> List[date]:
    return [d for d in dates if d <= cap]


def trading_days_in_range(
    dates: Iterable[date], start: date, end: date
) -> List[date]:
    out = [d for d in dates if start <= d < end]
    return sorted(out)


def recent_trading_days(
    dates: Iterable[date], cap: date, lookback: int
) -> List[date]:
    capped = [d for d in dates if d <= cap]
    capped = sorted(capped)
    if lookback <= 0:
        return []
    return capped[-lookback:]


def next_day(d: date) -> date:
    return d + timedelta(days=1)
