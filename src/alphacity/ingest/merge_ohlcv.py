#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
合併全市場日線 OHLCV 資料 (支援 MultiIndex 與字串化 tuple 欄位)
輸入: ohlcv_daily/*.parquet
輸出: ohlcv_daily_all.parquet
"""

import pandas as pd
from pathlib import Path
from loguru import logger
import argparse
import re

def normalize_df(df: pd.DataFrame, fallback_symbol: str) -> pd.DataFrame:
    """處理單檔股票 DataFrame，轉成標準格式"""
    symbol = fallback_symbol
    new_cols = []

    for c in df.columns:
        field, sym = None, None

        # case 1: 真正的 tuple
        if isinstance(c, tuple):
            field = str(c[0]).lower().replace(" ","_")
            sym = c[1] if isinstance(c[1], str) and c[1].endswith(".TW") else None

        # case 2: 字串化 tuple
        elif isinstance(c, str) and c.startswith("(") and "," in c:
            m = re.match(r"\('([^']+)',\s*'([^']*)'\)", c.replace(" ", ""))
            if m:
                field, sym = m.groups()
                field = field.lower().replace(" ","_")
                if sym and sym.endswith(".TW"):
                    symbol = sym

        # fallback: 普通字串欄位
        else:
            field = str(c).lower().replace(" ","_")

        if field == "date":
            new_cols.append("date")
        elif field in ["open","high","low","close","adj_close","adj close","volume"]:
            new_cols.append(field.replace(" ","_"))
            if sym:
                symbol = sym
        else:
            new_cols.append(field)

    df.columns = new_cols

    # 確保有日期欄位
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
    elif isinstance(df.index, pd.DatetimeIndex):
        df = df.reset_index().rename(columns={"index":"date"})
    else:
        logger.warning(f"{symbol}: 找不到日期欄位，跳過")
        return None

    # 補齊必要欄位
    required_cols = ["date","open","high","low","close","adj_close","volume"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = pd.NA

    out = df[required_cols].copy()
    out["symbol"] = symbol

    # 強制數值轉換
    for col in ["open","high","low","close","adj_close","volume"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    return out

def load_file(f: Path):
    try:
        df = pd.read_parquet(f, engine="pyarrow")
        return normalize_df(df, f.stem)
    except Exception as e:
        logger.error(f"{f.name} 讀取失敗: {e}")
        return None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ohlcv-root", type=str, required=True, help="ohlcv_daily 目錄")
    parser.add_argument("--out", type=str, required=True, help="輸出檔案 parquet")
    args = parser.parse_args()

    root = Path(args.ohlcv_root)
    files = list(root.glob("*.parquet"))

    dfs = []
    for i,f in enumerate(files,1):
        df = load_file(f)
        if df is not None:
            dfs.append(df)
        if i % 50 == 0:
            logger.info(f"已處理 {i}/{len(files)} 檔")

    if not dfs:
        logger.error("沒有可合併的檔案")
        return

    all_df = pd.concat(dfs, ignore_index=True)
    all_df = all_df.sort_values(["date","symbol"])

    all_df["date"] = pd.to_datetime(all_df["date"], errors="coerce")

    out_file = Path(args.out)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    all_df.to_parquet(out_file, engine="pyarrow", index=False)
    logger.info(f"已輸出合併檔 {out_file}, 共 {len(all_df)} 筆")

if __name__ == "__main__":
    main()
