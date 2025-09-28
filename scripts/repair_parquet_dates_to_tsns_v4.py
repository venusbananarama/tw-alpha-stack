#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Robust parquet repair (v4):
- Read -> pandas -> normalize (remove categorical) -> from_pandas
- combine_chunks() to avoid mixed chunk types
- Decode ANY dictionary-encoded columns
- Enforce yyyymm=int32
- Ensure date (or trade_date/Date) is timestamp[ns] (naive or UTC per flag)
- Backup to <file>.tsns.bak before overwrite
"""
import os, glob, argparse, shutil
from typing import List, Tuple

import pandas as pd
import pandas.api.types as ptypes
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
        if getattr(s2.dt, "tz", None) is not None:
            s2 = s2.dt.tz_convert("UTC").dt.tz_localize(None)
    return s2

def normalize_pandas_df(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if ptypes.is_categorical_dtype(df[col]):
            cats = df[col].cat.categories
            if ptypes.is_integer_dtype(cats) or ptypes.is_float_dtype(cats):
                df[col] = pd.to_numeric(df[col].astype(str), errors="coerce")
            else:
                df[col] = df[col].astype("string")
    if "yyyymm" in df.columns:
        df["yyyymm"] = pd.to_numeric(df["yyyymm"], errors="coerce").astype("Int32")
    return df

def decode_all_dictionary_columns(tbl: pa.Table) -> pa.Table:
    cols = []
    for name in tbl.column_names:
        col = tbl[name]
        if pa.types.is_dictionary(col.type):
            col = pc.dictionary_decode(col)
        cols.append((name, col))
    return pa.table({k: v for k, v in cols})

def enforce_types(tbl: pa.Table, date_col: str, tz: str) -> pa.Table:
    # yyyymm -> int32
    if "yyyymm" in tbl.column_names:
        i = tbl.column_names.index("yyyymm")
        col = tbl["yyyymm"]
        if pa.types.is_dictionary(col.type):
            col = pc.dictionary_decode(col)
        if not pa.types.is_int32(col.type):
            col = pc.cast(col, pa.int32())
        tbl = tbl.set_column(i, "yyyymm", col)

    # date -> timestamp[ns]
    if date_col and date_col in tbl.column_names:
        i = tbl.column_names.index(date_col)
        col = tbl[date_col]
        if not pa.types.is_timestamp(col.type) or col.type.unit != "ns":
            col = pc.cast(col, pa.timestamp("ns"))
        tbl = tbl.set_column(i, date_col, col)

    return tbl

def repair_file(path: str, tz: str, backup_suffix: str) -> Tuple[str, str]:
    try:
        t = pq.read_table(path)
        df = t.to_pandas()
        df = normalize_pandas_df(df)

        dcol = find_date_col(df.columns)
        before_dtype = after_dtype = None
        if dcol:
            before_dtype = str(df[dcol].dtype)
            df[dcol] = to_tsns_series(df[dcol], tz=tz)
            if df[dcol].isna().all():
                return ("FAIL_ALL_NAT", path)
            after_dtype = str(df[dcol].dtype)

        tbl = pa.Table.from_pandas(df, preserve_index=False)
        tbl = tbl.combine_chunks()  # unify chunks first
        tbl = decode_all_dictionary_columns(tbl)  # drop dictionary encodings
        tbl = enforce_types(tbl, dcol, tz)
        tbl = tbl.combine_chunks()

        # Backup and atomic overwrite
        bak = path + backup_suffix
        if not os.path.exists(bak):
            shutil.copy2(path, bak)
        tmp = path + ".tmp"
        pq.write_table(tbl, tmp, compression="snappy")
        os.replace(tmp, path)

        return ("FIXED_TSNS", f"{path} | date dtype: {before_dtype}->{after_dtype}; yyyymm=int32; no dictionaries")
    except Exception as e:
        return ("ERROR", f"{path} -> {e}")

def main():
    ap = argparse.ArgumentParser(description="Repair parquet: date->timestamp[ns], remove dictionaries, yyyymm=int32 (v4)")
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
        print("[REPAIR] No parquet files found. Check --datahub-root and datasets.")
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
