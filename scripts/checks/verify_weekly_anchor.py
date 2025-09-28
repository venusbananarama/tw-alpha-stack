#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Phase 1 check: 驗證週期錨點 (weekly anchor) 是否為週五。
優先尋找 weekly 價格表；若無，從日線推導週末日並檢查是否為週五。
"""
import argparse, os
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
import yaml

def load_cfg(p): 
    with open(p,'r',encoding='utf-8') as f:
        return yaml.safe_load(f)

def find_weekly_prices(root: Path):
    # 常見放置：datahub/silver/alpha/prices_weekly/*.parquet
    wk = root / 'prices_weekly'
    if wk.exists():
        return list(wk.rglob('*.parquet'))
    return []

def find_daily_prices(root: Path):
    return list((root / 'prices').rglob('*.parquet'))

def check_weekday(series: pd.Series):
    # Monday=0 ... Friday=4
    return (series.dt.weekday == 4).all()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='configs/data_sources.yaml')
    ap.add_argument('--out', default='metrics/weekly_anchor_report.csv')
    args = ap.parse_args()

    cfg = load_cfg(args.config)
    root = Path(cfg['datahub_root']) / 'silver' / 'alpha'

    candidates = find_weekly_prices(root)
    recs = []
    if candidates:
        for p in candidates[:10]:  # 抽樣 10 檔
            tbl = ds.dataset(str(p), format='parquet').to_table(columns=['date'])
            df = tbl.to_pandas()
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            ok = check_weekday(df['date'].dropna())
            recs.append({'path': str(p), 'mode':'weekly', 'friday_only': bool(ok), 'rows': int(len(df))})
    else:
        # 從日線推導週末日
        dailies = find_daily_prices(root)
        for p in dailies[:10]:
            tbl = ds.dataset(str(p), format='parquet').to_table(columns=['date'])
            df = tbl.to_pandas()
            if df.empty: 
                recs.append({'path': str(p), 'mode':'daily', 'friday_only': None, 'rows': 0})
                continue
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            wk = df['date'].dt.to_period('W-FRI').dt.end_time.dt.tz_localize(None)
            ok = check_weekday(wk.dropna())
            recs.append({'path': str(p), 'mode':'daily->W-FRI', 'friday_only': bool(ok), 'rows': int(len(df))})

    pd.DataFrame(recs).to_csv(args.out, index=False)
    if not recs:
        print("ANCHOR CHECK: SKIP (no price data found)")
    else:
        bad = [r for r in recs if r['friday_only'] is False]
        print("ANCHOR CHECK:", "OK" if not bad else f"ISSUES={len(bad)}")

if __name__ == "__main__":
    main()
