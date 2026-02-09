#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Lag and align tool (P2-MVP).

Aligns input data to a weekly anchor and applies event lag / execution delay.
Outputs aligned data and a JSON alignment report.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--in", dest="in_path", required=True, help="Input data (csv/parquet)")
    p.add_argument("--out", dest="out_path", required=True, help="Output data path")
    p.add_argument("--as-of", default=None, help="Keep rows with date <= as-of")
    p.add_argument("--weekly-anchor", default="W-FRI", help="Weekly anchor, default W-FRI")
    p.add_argument("--exec-delay-days", type=int, default=1, help="Execution delay in days")
    p.add_argument("--lag-days", type=int, default=0, help="Event lag in days")
    p.add_argument("--date-col", default="date", help="Date column name")
    p.add_argument("--key-cols", default="", help="Comma-separated key columns (e.g., symbol)")
    return p.parse_args()


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _write_table(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)


def _parse_keys(raw: str) -> List[str]:
    keys = [k.strip() for k in raw.split(",") if k.strip()]
    return keys


def _missing_rate(df: pd.DataFrame, key_cols: List[str], date_col: str) -> float:
    if df.empty:
        return 0.0
    non_keys = [c for c in df.columns if c not in key_cols + [date_col]]
    if not non_keys:
        return 0.0
    mask = df[non_keys].isna().any(axis=1)
    return float(mask.mean())


def _check_sorted(df: pd.DataFrame, key_cols: List[str], date_col: str) -> bool:
    if df.empty:
        return True
    if not key_cols:
        return df[date_col].is_monotonic_increasing
    ok = True
    for _, g in df.groupby(key_cols):
        if not g[date_col].is_monotonic_increasing:
            ok = False
            break
    return ok


def main() -> None:
    args = _parse_args()
    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    key_cols = _parse_keys(args.key_cols)

    df = _read_table(in_path)
    if args.date_col not in df.columns:
        raise SystemExit(f"Missing date column: {args.date_col}")

    df = df.copy()
    df[args.date_col] = pd.to_datetime(df[args.date_col], errors="coerce")
    df = df[df[args.date_col].notna()]
    df = df.sort_values(key_cols + [args.date_col] if key_cols else [args.date_col])

    before_rows = int(len(df))
    before_min = str(df[args.date_col].min().date()) if not df.empty else None
    before_max = str(df[args.date_col].max().date()) if not df.empty else None

    total_delay = int(args.lag_days) + int(args.exec_delay_days)
    if total_delay != 0:
        df[args.date_col] = df[args.date_col] + pd.to_timedelta(total_delay, unit="D")

    if args.as_of:
        as_of = pd.to_datetime(args.as_of, errors="coerce")
        if pd.isna(as_of):
            raise SystemExit(f"Invalid as-of date: {args.as_of}")
        df = df[df[args.date_col] <= as_of]

    if args.weekly_anchor:
        df["__week"] = df[args.date_col].dt.to_period(args.weekly_anchor)
        df = df.sort_values(key_cols + [args.date_col] if key_cols else [args.date_col])
        group_cols = (key_cols + ["__week"]) if key_cols else ["__week"]
        df = df.groupby(group_cols, as_index=False).last()
        df = df.drop(columns=["__week"])

    after_rows = int(len(df))
    after_min = str(df[args.date_col].min().date()) if not df.empty else None
    after_max = str(df[args.date_col].max().date()) if not df.empty else None

    report = {
        "input_path": str(in_path),
        "output_path": str(out_path),
        "weekly_anchor": args.weekly_anchor,
        "exec_delay_days": int(args.exec_delay_days),
        "lag_days": int(args.lag_days),
        "as_of": args.as_of,
        "rows_before": before_rows,
        "rows_after": after_rows,
        "date_min_before": before_min,
        "date_max_before": before_max,
        "date_min_after": after_min,
        "date_max_after": after_max,
        "missing_rate_after": _missing_rate(df, key_cols, args.date_col),
        "is_sorted_after": _check_sorted(df, key_cols, args.date_col),
    }

    _write_table(df, out_path)
    report_path = Path(str(out_path) + ".align_report.json")
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] {exc}", file=sys.stderr)
        sys.exit(2)

