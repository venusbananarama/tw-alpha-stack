#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""nav_cleaner.py
Batch-clean NAV CSV files to remove inf/-inf/blank values so report generators don't crash.

Usage examples:
  python nav_cleaner.py --in-dir G:\AI\datahub\alpha\backtests\grid_test
  python nav_cleaner.py --in-dir G:\AI\datahub\alpha\backtests --pattern nav.csv --method drop --renorm 1.0
  python nav_cleaner.py --in-dir . --glob "**/nav.csv" --date-col date --value-col nav --suffix _clean.csv

Defaults assume a CSV with headers: date,nav
- date column parsed to datetime
- nav column coerced to numeric
"""

import argparse
from pathlib import Path
import pandas as pd
import numpy as np
import json

def clean_nav_df(df: pd.DataFrame, date_col: str, value_col: str, method: str, renorm: float|None):
    # Coerce types
    if date_col not in df.columns or value_col not in df.columns:
        raise ValueError(f"Missing required columns: date_col='{date_col}' or value_col='{value_col}'. Columns found: {list(df.columns)}")
    out = df.copy()
    # Parse dates (tolerant)
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    # Numeric coercion
    out[value_col] = pd.to_numeric(out[value_col], errors="coerce")
    # Replace +/-inf with NaN
    out[value_col] = out[value_col].replace([np.inf, -np.inf], np.nan)
    # Drop rows with invalid date
    out = out.dropna(subset=[date_col])
    out = out.sort_values(date_col).reset_index(drop=True)
    # Stats before clean
    stats = {
        "rows_before": int(len(df)),
        "nan_before": int(df[value_col].isna().sum()) if value_col in df.columns else None,
        "inf_before": int(np.isinf(pd.to_numeric(df[value_col], errors='coerce')).sum()) if value_col in df.columns else None,
    }
    # Handle NaNs in NAV
    if method == "drop":
        out = out.dropna(subset=[value_col])
    elif method == "ffill":
        out[value_col] = out[value_col].ffill()
        out = out.dropna(subset=[value_col])  # still drop leading NaNs
    else:
        raise ValueError("method must be 'drop' or 'ffill'")
    # Remove duplicates on date, keep last
    out = out.drop_duplicates(subset=[date_col], keep="last")
    # Final guard: finite only
    mask_finite = np.isfinite(out[value_col].to_numpy())
    out = out.loc[mask_finite].copy()
    # Optional renormalize so first value == renorm
    if renorm is not None and len(out) > 0:
        first = out[value_col].iloc[0]
        if first != 0 and np.isfinite(first):
            scale = renorm / first
            out[value_col] = out[value_col] * scale
    # Post stats
    stats.update({
        "rows_after": int(len(out)),
        "nan_after": int(out[value_col].isna().sum()),
        "min": float(out[value_col].min()) if len(out) else None,
        "max": float(out[value_col].max()) if len(out) else None,
        "start_date": out[date_col].iloc[0].strftime("%Y-%m-%d") if len(out) else None,
        "end_date": out[date_col].iloc[-1].strftime("%Y-%m-%d") if len(out) else None,
    })
    return out, stats

def main():
    p = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--in-dir", required=True, help="Root directory to search for NAV CSVs")
    p.add_argument("--glob", default="**/nav.csv", help="Glob pattern (relative to in-dir) to match files")
    p.add_argument("--date-col", default="date", help="Date column name in CSV") 
    p.add_argument("--value-col", default="nav", help="NAV/value column name in CSV")
    p.add_argument("--method", choices=["drop","ffill"], default="drop", help="How to handle NaNs after coercion") 
    p.add_argument("--suffix", default="_clean.csv", help="Suffix to append to filename for cleaned output") 
    p.add_argument("--renorm", type=float, default=None, help="If set, renormalize NAV so the first value equals this number (e.g., 1.0 or 100)" )
    p.add_argument("--report-json", default="clean_report.json", help="Path to write a summary JSON (at in-dir root)")
    args = p.parse_args()

    in_dir = Path(args.in_dir)
    files = sorted(in_dir.glob(args.glob))
    if not files:
        print(f"No files matched {args.glob} under {in_dir}")
        return 1
    report = {}
    total_written = 0
    for f in files:
        try:
            df = pd.read_csv(f)
        except Exception as e:
            print(f"[SKIP] {f}: failed to read CSV: {e}")
            continue
        try:
            cleaned, stats = clean_nav_df(df, args.date_col, args.value_col, args.method, args.renorm)
        except Exception as e:
            print(f"[SKIP] {f}: cleaning error: {e}")
            continue
        out_path = f.with_name(f.stem + args.suffix)
        try:
            cleaned.to_csv(out_path, index=False)
            total_written += 1
            stats["output"] = str(out_path)
            report[str(f)] = stats
            print(f"[OK] {f} -> {out_path} | rows: {stats['rows_before']} -> {stats['rows_after']}")
        except Exception as e:
            print(f"[SKIP] {f}: failed to write cleaned CSV: {e}")
            continue
    # Write report
    try:
        rep_path = Path(args.report_json)
        if not rep_path.is_absolute():
            rep_path = in_dir / rep_path
        with open(rep_path, "w", encoding="utf-8") as w:
            json.dump(report, w, ensure_ascii=False, indent=2)
        print(f"Summary written to {rep_path} | cleaned files: {total_written}")
    except Exception as e:
        print(f"[WARN] Failed to write report JSON: {e}")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
