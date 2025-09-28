#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Repair parquet files by converting the 'date' (or similar) column to pandas datetime64[ns]
and writing back as Arrow timestamp[ns]. Creates a backup alongside each file before replacing.
"""
import os
import glob
import argparse
import shutil
from typing import List, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

CAND_DATE_COLS = ["date", "trade_date", "Date"]

def find_date_col(cols: List[str]) -> str:
    for c in CAND_DATE_COLS:
        if c in cols:
            return c
    return ""

def to_tsns_series(s: pd.Series, tz: str) -> pd.Series:
    # Convert to datetime; handle common formats (YYYY-MM-DD, timestamps, YYYYMMDD)
    s2 = pd.to_datetime(s, errors="coerce", format=None)
    if s2.isna().mean() > 0.8:
        # try YYYYMMDD
        s2 = pd.to_datetime(s, errors="coerce", format="%Y%m%d")
    # Normalize timezone choice
    if tz.lower() == "utc":
        if s2.dt.tz is None:
            s2 = s2.dt.tz_localize("UTC")
        else:
            s2 = s2.dt.tz_convert("UTC")
    elif tz.lower() in ("naive", "none"):
        if s2.dt.tz is not None:
            s2 = s2.dt.tz_convert("UTC").dt.tz_localize(None)
    return s2

def cast_column_to_tsns(table: pa.Table, colname: str, tz: str) -> pa.Table:
    idx = table.column_names.index(colname)
    col = table[colname]
    if pa.types.is_timestamp(col.type):
        # ensure ns resolution
        if col.type.unit != "ns":
            col = pc.cast(col, pa.timestamp("ns", tz=col.type.tz))
        if tz.lower() in ("naive", "none") and col.type.tz is not None:
            arr = pc.assume_timezone(col, "UTC")
            col = pc.cast(arr, pa.timestamp("ns"))
        elif tz.lower() == "utc" and col.type.tz != "UTC":
            col = pc.assume_timezone(col, "UTC")
    else:
        try:
            col = pc.cast(col, pa.timestamp("ns"))
        except Exception:
            df = table.to_pandas()
            df[colname] = to_tsns_series(df[colname], tz=tz)
            t2 = pa.Table.from_pandas(df, preserve_index=False)
            col = t2[colname]
    return table.set_column(idx, colname, col)

def repair_file(path: str, tz: str, backup_suffix: str) -> Tuple[str, str]:
    try:
        table = pq.read_table(path)
        cols = table.column_names
        dcol = find_date_col(cols)
        if not dcol:
            return ("SKIP_NO_DATE_COL", path)

        df = table.to_pandas()
        before_dtype = str(df[dcol].dtype)
        df[dcol] = to_tsns_series(df[dcol], tz=tz)
        if df[dcol].isna().all():
            return ("FAIL_ALL_NAT", path)

        new_table = pa.Table.from_pandas(df, preserve_index=False)

        if dcol in new_table.column_names:
            if not pa.types.is_timestamp(new_table[dcol].type) or new_table[dcol].type.unit != "ns":
                new_table = cast_column_to_tsns(new_table, dcol, tz=tz)

        bak = path + backup_suffix
        if not os.path.exists(bak):
            shutil.copy2(path, bak)
        tmp = path + ".tmp"
        pq.write_table(new_table, tmp, compression="snappy")
        os.replace(tmp, path)
        return ("FIXED_TSNS", f"{path} | {before_dtype} -> {new_table[dcol].type if dcol in new_table.column_names else 'timestamp[ns]'}")
    except Exception as e:
        return ("ERROR", f"{path} -> {e}")

def main():
    ap = argparse.ArgumentParser(description="Force 'date' column to pandas datetime64[ns] / Arrow timestamp[ns].")
    ap.add_argument("--datahub-root", required=True, help="Path to datahub root (the parent of 'silver/')")
    ap.add_argument("--datasets", nargs="+", default=["prices", "chip"], help="Datasets under silver/alpha to process")
    ap.add_argument("--limit", type=int, default=0, help="Process at most N files (0 = all)")
    ap.add_argument("--tz", choices=["naive", "UTC", "none"], default="naive",
                    help="Normalize timezone: 'naive' (default) writes timestamp[ns] without tz; 'UTC' keeps tz-aware")
    ap.add_argument("--backup-suffix", default=".tsns.bak", help="Suffix for backup files")
    args = ap.parse_args()

    patterns = [os.path.join(args.datahub_root, "silver", "alpha", ds, "yyyymm=*", "*.parquet")
                for ds in args.datasets]

    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(pat))

    if not files:
        print("[REPAIR] No parquet files found. Check --datahub-root and dataset paths.")
        return

    from collections import Counter
    cnt = Counter()
    files = sorted(files)
    if args.limit and args.limit > 0:
        files = files[:args.limit]

    for p in files:
        tag, msg = repair_file(p, tz=args.tz, backup_suffix=args.backup_suffix)
        cnt[tag] += 1
        print(f"[{tag}] {msg}")

    print("[SUMMARY]", dict(cnt))

if __name__ == "__main__":
    main()
