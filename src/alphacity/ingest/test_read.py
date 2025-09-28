#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
測試讀取單一 parquet 檔案，確認欄位與內容
"""

import pandas as pd
import argparse

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, required=True, help="parquet 檔案路徑")
    args = parser.parse_args()

    try:
        df = pd.read_parquet(args.file, engine="pyarrow")
        print("=== 前五筆資料 ===")
        print(df.head())
        print("\n=== 欄位型別 ===")
        print(df.dtypes)
        print(f"共 {len(df)} 筆資料")
    except Exception as e:
        print(f"讀取失敗: {e}")

if __name__ == "__main__":
    main()
