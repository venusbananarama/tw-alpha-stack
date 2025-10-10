#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
from datetime import datetime
import pytz

def read_calendar(cal_path: Path) -> pd.Series:
    if not cal_path.exists():
        raise FileNotFoundError(f"Trading calendar not found: {cal_path}")
    try:
        df = pd.read_csv(cal_path)
        col = 'date' if 'date' in df.columns else df.columns[0]
    except Exception:
        df = pd.read_csv(cal_path, header=None, names=['date'])
        col = 'date'
    s = pd.to_datetime(df[col], errors='coerce')
    return s.dropna().dt.normalize()

def get_expect_date(repo_root: Path) -> pd.Timestamp:
    tz = pytz.timezone("Asia/Taipei")
    today = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    cal_path = (repo_root / "cal" / "trading_days.csv").resolve()
    s = read_calendar(cal_path).dt.tz_localize("Asia/Taipei")
    mask = s <= pd.Timestamp(today)
    if not mask.any():
        raise RuntimeError("No trading day found <= today in calendar.")
    return s[mask].max()

if __name__ == "__main__":
    root = Path(sys.argv[2]).resolve() if len(sys.argv)>2 and sys.argv[1] in ("--root","-r") else Path(".").resolve()
    dt = get_expect_date(root)
    print(dt.strftime("%Y-%m-%d"))
