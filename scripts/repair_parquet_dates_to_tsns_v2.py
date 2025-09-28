#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Repair parquet files by:
1) Converting the 'date' column to pandas datetime64[ns] and Arrow timestamp[ns].
2) Normalizing dictionary-encoded columns (esp. 'yyyymm') to their value type
   (decoding dictionary to plain array) and enforcing yyyymm=int32.
Each file is backed up with the suffix given by --backup-suffix before overwrite.
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
    s2 = pd.to_datetime(s, errors="coerce", format=None)
    if s2.isna().mean() > 0.8:
        s2 = pd.to_datetime(s, errors="coerce", format="%Y%m%d")
    if tz.lower() == "utc":
        if s2.dt.tz is None:
            s2 = s2.dt.tz_localize("UTC")
        else:
            s2 = s2.dt.tz_convert("UTC")
    elif tz.lower() in ("naive", "none"):
        if s2.dt.tz is not None:
            s2 = s2.dt.tz_convert("UTC").dt.tz_localize(None)
    return s2

def normalize_dictionary_columns(table: pa.Table) -> pa.Table:
    """Decode all dictionary-encoded columns to their value types."""
    cols = []
    for name in table.column_names:
        col = table[name]
        if pa.types.is_dictionary(col.type):
            col = pc.dictionary_decode(col)
        cols.append((name, col))
    return pa.table({k: v for k, v in cols})

def enforce_column_types(table: pa.Table) -> pa.Table:
    """Project certain columns to fixed primitive types: yyyymm -> int32."""
    if "yyyymm" in table.column_names:
        idx = table.column_names.index("yyyymm")
        col = table["yyyymm"]
        if pa.types.is_dictionary(col.type):
            col = pc.dictionary_decode(col)
        if not pa.types.is_int32(col.type):
            col = pc.cast(col, pa.int32())
        table = table.set_column(idx, "yyyymm", col)
    return table

def cast_column_to_tsns(table: pa.Table, colname: str, tz: str) -> pa.Table:
    idx = table.column_names.index(colname)
    col = table[colname]
    if pa.types.is_timestamp(col.type):
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
        orig = pq.read_table(path)
        dcol = find_date_col(orig.column_names)

        # If we have a date column, go via pandas to ensure datetime64[ns]
        if dcol:
            df = orig.to_pandas()
            before_dtype = str(df[dcol].dtype)
            df[dcol] = to_tsns_series(df[dcol], tz=tz)
            if df[dcol].isna().all():
                return ("FAIL_ALL_NAT", path)
            table = pa.Table.from_pandas(df, preserve_index=False)
        else:
            table = orig

        # Normalize dictionaries (e.g., yyyymm) and enforce types
        table = normalize_dictionary_columns(table)
        table = enforce_column_types(table)

        # Ensure date is timestamp[ns]
        if dcol and dcol in table.column_names:
            if (not pa.types.is_timestamp(table[dcol].type)) or table[dcol].type.unit != "ns":
                table = cast_column_to_tsns(table, dcol, tz=tz)

        # Backup and overwrite
        bak = path + backup_suffix
        if not os.path.exists(bak):
            shutil.copy2(path, bak)
        tmp = path + ".tmp"
        pq.write_table(table, tmp, compression="snappy")
        os.replace(tmp, path)

        after = table[dcol].type if dcol and dcol in table.column_names else "(no date col)"
        return ("FIXED_TSNS", f"{path} -> {after}")
    except Exception as e:
        return ("ERROR", f"{path} -> {e}")

def main():
    ap = argparse.ArgumentParser(description="Normalize parquet: fix 'date' to timestamp[ns], decode dictionary cols (yyyymm=int32)")
    ap.add_argument("--datahub-root", required=True)
    ap.add_argument("--datasets", nargs="+", default=["prices", "chip"])
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--tz", choices=["naive", "UTC", "none"], default="naive")
    ap.add_argument("--backup-suffix", default=".tsns.bak")
    args = ap.parse_args()

    patterns = [os.path.join(args.datahub_root, "silver", "alpha", ds, "yyyymm=*", "*.parquet") for ds in args.datasets]
    files: List[str] = []
    for pat in patterns:
        files.extend(glob.glob(pat))

    if not files:
        print("[REPAIR] No parquet files found. Check --datahub-root and dataset paths.")
        return

    files = sorted(files)
    if args.limit and args.limit > 0:
        files = files[:args.limit]

    from collections import Counter
    cnt = Counter()
    for p in files:
        tag, msg = repair_file(p, tz=args.tz, backup_suffix=args.backup_suffix)
        cnt[tag] += 1
        print(f"[{tag}] {msg}")

    print("[SUMMARY]", dict(cnt))

if __name__ == "__main__":
    main()
