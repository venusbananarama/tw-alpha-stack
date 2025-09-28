#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
QC 檢查台股日線資料 (OHLCV) - v4
支援 multi-index 欄位 & date 在 index/第一欄位 的情況
"""

import pandas as pd
from pathlib import Path
from loguru import logger
import argparse

def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """將 multi-index 欄位攤平成單層"""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if c[0] != "date" else "date" for c in df.columns]
    return df

def check_file(f: Path):
    try:
        df = pd.read_parquet(f, engine="pyarrow")
        df = flatten_columns(df)
        if df.empty:
            return {"symbol": f.stem, "status": "empty"}

        # 確保有 date 欄位
        if "date" not in df.columns:
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index().rename(columns={"index": "date"})
            else:
                # 嘗試把第一欄當成 date
                first_col = df.columns[0]
                df = df.rename(columns={first_col: "date"})

        start, end = df["date"].min(), df["date"].max()
        n = len(df)
        zeros = (df["volume"] == 0).sum() if "volume" in df.columns else -1

        return {
            "symbol": f.stem,
            "status": "ok",
            "rows": n,
            "start": str(start)[:10],
            "end": str(end)[:10],
            "zero_volume_days": int(zeros),
        }
    except Exception as e:
        return {"symbol": f.stem, "status": f"error: {e}"}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ohlcv-root", type=str, required=True, help="ohlcv_daily 目錄")
    parser.add_argument("--out", type=str, required=True, help="輸出報告檔 CSV")
    args = parser.parse_args()

    root = Path(args.ohlcv_root)
    files = list(root.glob("*.parquet"))

    results = []
    for f in files:
        res = check_file(f)
        results.append(res)

    df = pd.DataFrame(results)
    df.to_csv(args.out, index=False, encoding="utf-8-sig")
    logger.info(f"已輸出 QC 報告 {args.out}, 共 {len(df)} 檔")

if __name__ == "__main__":
    main()
