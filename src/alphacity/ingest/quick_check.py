#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
升級版 quick_check.py
- 螢幕顯示總覽 & 權值股檢查
- 輸出全市場檢查報告 CSV
"""

import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=True, help="ohlcv_daily_all.parquet 路徑")
    parser.add_argument("--out", type=str, default="quick_check_report.csv", help="輸出報告 CSV")
    args = parser.parse_args()

    df = pd.read_parquet(args.file, engine="pyarrow")

    print("=== 全市場總覽 ===")
    print("股票數：", len(df["symbol"].unique()))
    print("總筆數：", len(df))
    print("日期範圍：", df["date"].min(), "→", df["date"].max())

    print("\n=== 欄位型別 ===")
    print(df.dtypes)

    print("\n=== 權值股檢查 ===")
    for sym in ["2330.TW", "2317.TW", "1101.TW"]:
        sub = df[df["symbol"] == sym].dropna(subset=["close"])
        if sub.empty:
            print(f"{sym}: ❌ 沒有有效資料")
        else:
            print(f"{sym}: ✅ {len(sub)} 筆, {sub['date'].min()} → {sub['date'].max()}")
            print(sub.head())
            print(sub.tail())

    print("\n=== 產生全市場報告 ===")
    valid = df.dropna(subset=["close"])
    stats = valid.groupby("symbol").agg(
        valid_rows=("close","count"),
        start=("date","min"),
        end=("date","max")
    ).reset_index()
    stats.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"已輸出報告 {args.out}, 共 {len(stats)} 檔")

if __name__ == "__main__":
    main()
