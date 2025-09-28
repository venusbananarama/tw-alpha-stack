#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Phase 1: 銀層資料一致性檢查（輕量但嚴謹）：
- 檢 prices / chip 下最新月份分割是否可讀、必備欄位是否在、行數 > 0
- 匯出 CSV 報告到 metrics/silver_check_latest.csv
"""
import argparse, os
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
import yaml
from datetime import datetime

REQUIRED = {
    'prices': ['symbol','date','close','volume'],
    'chip':   ['symbol','date']
}

def load_cfg(p):
    with open(p,'r',encoding='utf-8') as f:
        return yaml.safe_load(f)

def latest_partition(path: Path):
    parts = [p for p in path.iterdir() if p.is_dir() and p.name.startswith('yyyymm=')]
    if not parts: 
        return None
    return sorted(parts, key=lambda x: x.name)[-1]

def check_dataset(root: Path, name: str):
    base = root / name
    part = latest_partition(base)
    if not part:
        return [{'dataset': name, 'partition': None, 'file': None, 'rows': 0, 'ok': False, 'missing_cols': 'NO_PARTITION'}]
    recs = []
    files = list(part.rglob('*.parquet'))[:50]  # 抽樣 50 檔
    for f in files:
        try:
            dset = ds.dataset(str(f), format='parquet')
            cols = dset.schema.names
            miss = [c for c in REQUIRED.get(name, []) if c not in cols]
            n = dset.count_rows()
            recs.append({'dataset': name, 'partition': part.name, 'file': str(f), 'rows': int(n), 'ok': (len(miss)==0 and n>0), 'missing_cols': ",".join(miss)})
        except Exception as e:
            recs.append({'dataset': name, 'partition': part.name, 'file': str(f), 'rows': 0, 'ok': False, 'missing_cols': f'ERROR:{type(e).__name__}'})
    return recs

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='configs/data_sources.yaml')
    ap.add_argument('--report-csv', default='metrics/silver_check_latest.csv')
    args = ap.parse_args()

    cfg = load_cfg(args.config)
    root = Path(cfg['datahub_root']) / 'silver' / 'alpha'
    rows = []
    for ds_name in ('prices','chip'):
        rows += check_dataset(root, ds_name)
    df = pd.DataFrame(rows)
    Path(args.report_csv).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.report_csv, index=False)
    print(f"SILVER CHECK: wrote -> {args.report_csv} ; OK={int(df['ok'].sum())}/{len(df)}")
    # Exit 0 一律通過（Phase 1 以報告為主）
if __name__ == "__main__":
    main()
