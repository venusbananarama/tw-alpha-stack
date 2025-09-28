#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robust parquet repair (v3-fix):
- Force 'date' to pandas datetime64[ns] and Arrow timestamp[ns] (naive or UTC per flag).
- Remove dictionary/categorical dtypes for all columns.
- Enforce yyyymm=int32 (numeric), avoiding Arrow merge conflicts.
- Backup each file as <file>.tsns.bak before overwrite.
"""
import os
import glob
import argparse
import shutil
from typing import List, Tuple

import pandas as pd
import pandas.api.types as ptypes
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

CAND_DATE_COLS = ["date", "trade_date", "Date"]

def find_date_col(cols):
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
        # ensure tz-naive
        if getattr(s2.dt, "tz", None) is not None:
            s2 = s2.dt.tz_convert("UTC").dt.tz_localize(None)
    return s2

def normalize_pandas_df(df: pd.DataFrame) -> pd.DataFrame:
    # Drop categorical/dictionary to base types
    for col in df.columns:
        if ptypes.is_categorical_dtype(df[col]):
            cats = df[col].cat.categories
            if ptypes.is_integer_dtype(cats) or ptypes.is_float_dtype(cats):
                df[col] = pd.to_numeric(df[col].astype(str), errors="coerce")
            else:
                df[col] = df[col].astype("string")
    # yyyymm -> numeric int32 (nullable ok)
    if "yyyymm" in df.columns:
        df["yyyymm"] = pd.to_numeric(df["yyyymm"], errors="coerce").astype("Int32")
    return df

def repair_file(path: str, tz: str, backup_suffix: str) -> Tuple[str, str]:
    try:
        table = pq.read_table(path)
        df = table.to_pandas()  # go through pandas to unify dtypes
        df = normalize_pandas_df(df)

        dcol = find_date_col(df.columns)
        before_dtype = None
        after_dtype = None
        if dcol:
            before_dtype = str(df[dcol].dtype)
            df[dcol] = to_tsns_series(df[dcol], tz=tz)
            if df[dcol].isna().all():
                return ("FAIL_ALL_NAT", path)
            after_dtype = str(df[dcol].dtype)

        # Build Arrow table back
        table2 = pa.Table.from_pandas(df, preserve_index=False)

        # Enforce Arrow side: yyyymm -> int32
        if "yyyymm" in table2.column_names:
            i = table2.column_names.index("yyyymm")
            col = table2["yyyymm"]
            if pa.types.is_dictionary(col.type):
                col = pc.dictionary_decode(col)
            if not pa.types.is_int32(col.type):
                col = pc.cast(col, pa.int32())
            table2 = table2.set_column(i, "yyyymm", col)

        # Ensure date is timestamp[ns]
        if dcol and dcol in table2.column_names:
            i = table2.column_names.index(dcol)
            col = table2[dcol]
            if not pa.types.is_timestamp(col.type) or col.type.unit != "ns":
                # cast to timestamp[ns] (naive)
                col = pc.cast(col, pa.timestamp("ns"))
            table2 = table2.set_column(i, dcol, col)

        # Backup and overwrite
        bak = path + backup_suffix
        if not os.path.exists(bak):
            shutil.copy2(path, bak)
        tmp = path + ".tmp"
        pq.write_table(table2, tmp, compression="snappy")
        os.replace(tmp, path)

        msg = f"{path} | date: {before_dtype if dcol else 'N/A'} -> {after_dtype if dcol else 'N/A'}; yyyymm unified"
        return ("FIXED_TSNS", msg)
    except Exception as e:
        return ("ERROR", f"{path} -> {e}")

def main():
    ap = argparse.ArgumentParser(description="Repair parquet: date->timestamp[ns], drop categorical, yyyymm=int32 (v3-fix)")
    ap.add_argument("--datahub-root", required=True)
    ap.add_argument("--datasets", nargs="+", default=["prices", "chip"])
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--tz", choices=["naive", "UTC", "none"], default="naive")
    ap.add_argument("--backup-suffix", default=".tsns.bak")
    args = ap.parse_args()

    patterns = [os.path.join(args.datahub_root, "silver", "alpha", ds, "yyyymm=*", "*.parquet") for ds in args.datasets]
    files = []
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
