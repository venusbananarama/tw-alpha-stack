#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
建立台股全市場清單 (Universe)
輸出: datahub/universes/tw_universe.parquet
日誌: datahub/metadata/universe_fetch.log
"""

import pandas as pd
import httpx
import time
import random
from datetime import datetime
from pathlib import Path
from loguru import logger
import argparse

SOURCES = {
    "TSE": "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
    "OTC": "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_equities",
}

def fetch_json(url, retries=5):
    """帶重試的 JSON 抓取 (SSL 驗證失敗時自動 verify=False)"""
    for i in range(retries):
        try:
            with httpx.Client(timeout=20, verify=False) as client:
                r = client.get(url)
                if r.status_code == 200:
                    return r.json()
        except Exception as e:
            logger.warning(f"嘗試 {i+1}/{retries} 失敗: {e}")
            time.sleep(1.5 * (i+1))
    return None

def normalize_field(row, keys):
    """從多個候選欄位取值"""
    for k in keys:
        if k in row and row[k]:
            return row[k]
    return None

def build_universe(out_file: Path):
    records = []

    # TSE
    tse = fetch_json(SOURCES["TSE"])
    if tse:
        logger.info(f"TSE 第一筆資料欄位: {list(tse[0].keys())}")
        for row in tse:
            code = normalize_field(row, ["證券代號", "公司代號", "Code", "Symbol"])
            name = normalize_field(row, ["證券名稱", "公司名稱", "Name"])
            industry = normalize_field(row, ["產業別", "Industry"])
            listed_date = normalize_field(row, ["上市日", "ListingDate"])
            isin = normalize_field(row, ["國際證券辨識號碼(ISIN Code)", "ISIN"])
            lot_size = normalize_field(row, ["每單位股數", "TradingUnit"])

            if not code:
                logger.warning(f"跳過一筆缺少代號: {row}")
                continue

            symbol = code.strip() + ".TW"
            records.append({
                "symbol": symbol,
                "name_zh": name.strip() if name else None,
                "name_en": None,
                "market": "TSE",
                "industry": industry,
                "listed_date": listed_date,
                "delisted_date": None,
                "status": "active",
                "isin": isin,
                "lot_size": lot_size,
                "ingested_at": datetime.utcnow()
            })

    # OTC
    otc = fetch_json(SOURCES["OTC"])
    if otc:
        logger.info(f"OTC 第一筆資料欄位: {list(otc[0].keys())}")
        for row in otc:
            code = normalize_field(row, ["證券代號", "公司代號", "Code", "Symbol"])
            name = normalize_field(row, ["證券名稱", "公司名稱", "Name"])
            industry = normalize_field(row, ["產業別", "Industry"])
            listed_date = normalize_field(row, ["上市日", "ListingDate"])
            isin = normalize_field(row, ["國際證券辨識號碼(ISIN Code)", "ISIN"])
            lot_size = normalize_field(row, ["每單位股數", "TradingUnit"])

            if not code:
                logger.warning(f"跳過一筆缺少代號: {row}")
                continue

            symbol = code.strip() + ".TWO"
            records.append({
                "symbol": symbol,
                "name_zh": name.strip() if name else None,
                "name_en": None,
                "market": "OTC",
                "industry": industry,
                "listed_date": listed_date,
                "delisted_date": None,
                "status": "active",
                "isin": isin,
                "lot_size": lot_size,
                "ingested_at": datetime.utcnow()
            })

    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset=["symbol"])

    out_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_file, engine="pyarrow", index=False)
    logger.info(f"共 {len(df)} 檔，已輸出 {out_file}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=str, required=True, help="輸出檔案路徑 (parquet)")
    parser.add_argument("--log", type=str, required=False, help="日誌檔案")
    args = parser.parse_args()

    if args.log:
        logger.add(args.log, rotation="1 MB")

    out_file = Path(args.out)
    build_universe(out_file)

if __name__ == "__main__":
    main()
