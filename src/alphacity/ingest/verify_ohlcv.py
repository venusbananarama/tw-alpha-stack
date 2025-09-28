#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
驗證全市場日線大表 (ohlcv_daily_all.parquet)
- 每檔股票的有效筆數 (非 NaN close)
- 起訖日期
- 輸出報告 CSV
"""

import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=True, help="ohlcv_daily_all.parquet 路徑")
    parser.add_argument("--out", type=str, required=True, help="輸出報告 CSV")
    args = parser.parse_args()

    df = pd.read_parquet(args.file, engine="pyarrow")

    # 僅保留有成交價的
    valid = df.dropna(subset=["close"])

    stats = valid.groupby("symbol").agg(
        valid_rows=("close","count"),
        start=("date","min"),
        end=("date","max")
    ).reset_index()

    # 找出疑似整檔 NaN 的股票
    all_symbols = df["symbol"].unique()
    missing = set(all_symbols) - set(stats["symbol"])
    missing_df = pd.DataFrame({"symbol": list(missing), "valid_rows": 0, "start": None, "end": None})

    final = pd.concat([stats, missing_df], ignore_index=True)
    final = final.sort_values("symbol")

    final.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"已輸出報告 {args.out}, 共 {len(final)} 檔")
    print("疑似沒抓到資料的股票數：", len(missing))

if __name__ == "__main__":
    main()
