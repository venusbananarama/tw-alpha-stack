#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
抓取台股日線 OHLCV 資料
輸出: datahub/ohlcv_daily/{symbol}.parquet
狀態: datahub/metadata/resume_state.json
"""

import pandas as pd
import yfinance as yf
import time
import random
import json
from pathlib import Path
from loguru import logger
import argparse

def fetch_symbol(symbol, out_dir: Path, since="2007-01-01"):
    """抓取單一股票日線"""
    try:
        df = yf.download(symbol, start=since, auto_adjust=False, progress=False)
        if df.empty:
            logger.warning(f"{symbol} 無資料")
            return False
        df.reset_index(inplace=True)
        df.rename(columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume"
        }, inplace=True)
        out_file = out_dir / f"{symbol}.parquet"
        df.to_parquet(out_file, engine="pyarrow", index=False)
        logger.info(f"{symbol} 已存檔 {len(df)} 筆 → {out_file}")
        return True
    except Exception as e:
        logger.error(f"{symbol} 抓取失敗: {e}")
        return False

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", type=str, required=True, help="股票清單 parquet 檔")
    parser.add_argument("--out-root", type=str, required=True, help="輸出根目錄")
    parser.add_argument("--since", type=str, default="2007-01-01", help="起始日期")
    parser.add_argument("--resume", type=str, default=None, help="斷點續傳狀態檔 json")
    parser.add_argument("--rps", type=float, default=1.0, help="每秒請求數限制")
    args = parser.parse_args()

    universe = pd.read_parquet(args.universe)
    symbols = universe["symbol"].unique().tolist()

    out_dir = Path(args.out_root)
    out_dir.mkdir(parents=True, exist_ok=True)

    state_file = Path(args.resume) if args.resume else None
    done = set()
    if state_file and state_file.exists():
        done = set(json.loads(state_file.read_text()))

    for i, symbol in enumerate(symbols, 1):
        if symbol in done:
            continue

        ok = fetch_symbol(symbol, out_dir, since=args.since)
        if ok:
            done.add(symbol)
            if state_file:
                state_file.write_text(json.dumps(list(done)))

        # 速率限制
        time.sleep(1.0 / args.rps + random.random() * 0.3)

    logger.info(f"全部完成，共 {len(done)}/{len(symbols)} 檔")

if __name__ == "__main__":
    main()
