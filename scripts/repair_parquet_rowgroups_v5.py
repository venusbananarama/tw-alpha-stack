#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Repair parquet v5 (row-group safe):
- 逐 Row Group 讀檔，避免 read_table 合併時型別衝突
- 對每個 Row Group：解碼所有 dictionary 欄位、強制 yyyymm=int32
- 合併時使用 concat_tables(promote=True)
- 將 date/trade_date/Date 轉成 timestamp[ns]（naive 或 UTC 依參數）
- 覆寫前產生 <file>.tsns.bak
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
        if c in cols: return c
    return ""

def to_tsns_series(s: pd.Series, tz: str) -> pd.Series:
    s2 = pd.to_datetime(s, errors="coerce", format=None)
    if s2.isna().mean() > 0.8:
        s2 = pd.to_datetime(s, errors="coerce", format="%Y%m%d")
    if tz.lower() == "utc":
        s2 = s2.dt.tz_localize("UTC") if s2.dt.tz is None else s2.dt.tz_convert("UTC")
    elif tz.lower() in ("naive","none"):
        if getattr(s2.dt,"tz",None) is not None:
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
                # 最兇殘 fallback：走 pandas → numeric → int32
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
                # 走 pandas 修
                df = tbl.to_pandas()
                df[dcol] = pd.to_datetime(df[dcol], errors="coerce")
                tbl = pa.Table.from_pandas(df, preserve_index=False)
                col = tbl[dcol]
                if not pa.types.is_timestamp(col.type) or col.type.unit != "ns":
                    col = pc.cast(col, pa.timestamp("ns"))
        tbl = tbl.set_column(j, dcol, col)
    return tbl

def repair_file(path: str, tz: str, backup_suffix: str) -> Tuple[str,str]:
    try:
        pf = pq.ParquetFile(path)
        parts = []
        for rg in range(pf.num_row_groups):
            chunk = pf.read_row_group(rg)            # 單一 Row Group，不會觸發合併錯誤
            chunk = decode_dicts(chunk)              # 解掉 dictionary
            chunk = force_yyyymm_int32(chunk)        # 統一 yyyymm=int32
            parts.append(chunk)

        tbl = pa.concat_tables(parts, promote=True)  # 我們自己合併
        # 找 date 欄位名稱（用合併後的 schema）
        dcol = find_date_col(tbl.column_names)
        if dcol:
            # 優先走 pandas，確保 datetime64[ns]
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

        # 再做一次安全收尾
        tbl = decode_dicts(tbl)
        tbl = force_yyyymm_int32(tbl)
        tbl = tbl.combine_chunks()

        bak = path + backup_suffix
        if not os.path.exists(bak):
            shutil.copy2(path, bak)
        tmp = path + ".tmp"
        pq.write_table(tbl, tmp, compression="snappy")
        os.replace(tmp, path)

        return ("FIXED_TSNS", f"{path} | date: {before}->{after}; yyyymm=int32; dictionaries removed")
    except Exception as e:
        return ("ERROR", f"{path} -> {e}")

def main():
    ap = argparse.ArgumentParser(description="Repair parquet row-group-safe v5")
    ap.add_argument("--datahub-root", required=True)
    ap.add_argument("--datasets", nargs="+", default=["prices","chip"])
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--tz", choices=["naive","UTC","none"], default="naive")
    ap.add_argument("--backup-suffix", default=".tsns.bak")
    args = ap.parse_args()

    pats = [os.path.join(args.datahub_root, "silver", "alpha", ds, "yyyymm=*", "*.parquet") for ds in args.datasets]
    files = []
    for p in pats: files.extend(glob.glob(p))
    if not files:
        print("[REPAIR] No parquet files found."); return
    files = sorted(files)
    if args.limit and args.limit>0: files = files[:args.limit]

    from collections import Counter
    cnt = Counter()
    for f in files:
        tag, msg = repair_file(f, tz=args.tz, backup_suffix=args.backup_suffix)
        cnt[tag]+=1
        print(f"[{tag}] {msg}")
    print("[SUMMARY]", dict(cnt))

if __name__ == "__main__":
    main()
