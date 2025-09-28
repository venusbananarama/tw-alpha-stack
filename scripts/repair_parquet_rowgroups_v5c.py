#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Repair parquet v5c (row-group safe + Windows lock-safe):
- Row-group reading (avoid merge conflicts)
- Decode ALL dictionary columns
- Enforce yyyymm=int32
- Ensure date/trade_date/Date is timestamp[ns] (naive/UTC)
- Backup <file>.tsns.bak
- If target locked: write <file>.repaired.parquet instead of failing
"""
import os, glob, argparse, shutil, time, stat
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
        s2 = s2.dt.tz_localize("UTC") if s2.dt.tz is None else s2.dt.tz_convert("UTC")
    elif tz.lower() in ("naive", "none"):
        if getattr(s2.dt, "tz", None) is not None:
            s2 = s2.dt.tz_convert("UTC").dt.tz_localize(None)
    return s2

def decode_dicts(tbl: pa.Table) -> pa.Table:
    cols = {}
    for n in tbl.column_names:
        col = tbl[n]
        if pa.types.is_dictionary(col.type):
            col = pc.dictionary_decode(col)
        cols[n] = col
    return pa.table(cols)

def force_yyyymm_int32(tbl: pa.Table) -> pa.Table:
    if "yyyymm" in tbl.column_names:
        i = tbl.column_names.index("yyyymm")
        col = tbl["yyyymm"]
        if pa.types.is_dictionary(col.type):
            col = pc.dictionary_decode(col)
        if not pa.types.is_int32(col.type):
            try:
                col = pc.cast(col, pa.int32())
            except Exception:
                df = tbl.to_pandas()
                df["yyyymm"] = pd.to_numeric(df["yyyymm"], errors="coerce").astype("Int32").astype("int32", errors="ignore")
                tbl = pa.Table.from_pandas(df, preserve_index=False)
                col = tbl["yyyymm"]
        tbl = tbl.set_column(i, "yyyymm", col)
    return tbl

def ensure_date_tsns(tbl: pa.Table, dcol: str) -> pa.Table:
    if dcol and dcol in tbl.column_names:
        j = tbl.column_names.index(dcol)
        col = tbl[dcol]
        if not pa.types.is_timestamp(col.type) or col.type.unit != "ns":
            try:
                col = pc.cast(col, pa.timestamp("ns"))
            except Exception:
                df = tbl.to_pandas()
                df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
                tbl = pa.Table.from_pandas(df, preserve_index=False)
                col = tbl[dcol]
                if not pa.types.is_timestamp(col.type) or col.type.unit != "ns":
                    col = pc.cast(col, pa.timestamp("ns"))
        tbl = tbl.set_column(j, dcol, col)
    return tbl

def safe_replace(tmp_path: str, target_path: str):
    """Try to os.replace with retries; if denied, write side-by-side .repaired.parquet."""
    # clear read-only
    try:
        os.chmod(target_path, stat.S_IWRITE | stat.S_IREAD)
    except Exception:
        pass
    for _ in range(6):
        try:
            os.replace(tmp_path, target_path)
            return "REPLACED", target_path
        except Exception:
            time.sleep(1.0)
    repaired = target_path + ".repaired.parquet"
    if os.path.exists(repaired):
        os.remove(repaired)
    os.replace(tmp_path, repaired)
    return "WRITTEN_NEW", repaired

def repair_file(path: str, tz: str, backup_suffix: str) -> Tuple[str, str]:
    try:
        pf = pq.ParquetFile(path)
        parts = []
        for rg in range(pf.num_row_groups):
            rg_tbl = pf.read_row_group(rg)
            rg_tbl = decode_dicts(rg_tbl)
            rg_tbl = force_yyyymm_int32(rg_tbl)
            parts.append(rg_tbl)

        tbl = pa.concat_tables(parts, promote_options='default')
        # handle date via pandas to guarantee datetime64[ns]
        dcol = next((c for c in CAND_DATE_COLS if c in tbl.column_names), "")
        if dcol:
            df = tbl.to_pandas()
            before = str(df[dcol].dtype)
            df[dcol] = to_tsns_series(df[dcol], tz=tz)
            if df[dcol].isna().all():
                return ("FAIL_ALL_NAT", path)
            tbl = pa.Table.from_pandas(df, preserve_index=False)
            tbl = ensure_date_tsns(tbl, dcol)
            after = str(df[dcol].dtype)
        else:
            before = after = "N/A"

        tbl = decode_dicts(tbl)
        tbl = force_yyyymm_int32(tbl)
        tbl = tbl.combine_chunks()

        bak = path + backup_suffix
        if not os.path.exists(bak):
            shutil.copy2(path, bak)
        tmp = path + ".tmp"
        pq.write_table(tbl, tmp, compression="snappy")
        mode, final_path = safe_replace(tmp, path)
        if mode == "REPLACED":
            return ("FIXED_TSNS", f"{path} | replaced; date {before}->{after}; yyyymm=int32")
        else:
            return ("FIXED_WRITTEN_NEW", f"{path} | locked -> wrote {final_path}; date {before}->{after}; yyyymm=int32")
    except Exception as e:
        return ("ERROR", f"{path} -> {e}")

def main():
    ap = argparse.ArgumentParser(description="Repair parquet row-group-safe v5c (lock-safe)")
    ap.add_argument("--datahub-root", required=True)
    ap.add_argument("--datasets", nargs="+", default=["prices", "chip"])
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--tz", choices=["naive", "UTC", "none"], default="naive")
    ap.add_argument("--backup-suffix", default=".tsns.bak")
    args = ap.parse_args()

    pats = [os.path.join(args.datahub_root, "silver", "alpha", ds, "yyyymm=*", "*.parquet") for ds in args.datasets]
    files = []
    for p in pats:
        files.extend(glob.glob(p))
    if not files:
        print("[REPAIR] No parquet files found."); return
    files = sorted(files)
    if args.limit and args.limit > 0:
        files = files[:args.limit]

    from collections import Counter
    cnt = Counter()
    for f in files:
        tag, msg = repair_file(f, tz=args.tz, backup_suffix=args.backup_suffix)
        cnt[tag] += 1
        print(f"[{tag}] {msg}")
    print("[SUMMARY]", dict(cnt))

if __name__ == "__main__":
    main()
