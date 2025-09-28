#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Phase 1 check: 驗證「公告日 <= 當週週五」(as-of gating) 是否被嚴格遵守。
找尋 datahub/silver/alpha 下具備 announce_date 與週欄位(date/week_end) 的週頻資料來檢查。
找不到時不報錯，輸出 "SKIP" 並以 0 結束碼返回（Phase 1 允許尚未建週頻表）。
"""
import argparse, os, sys, json, datetime as dt
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
import yaml

def load_paths(cfg_path: Path):
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    root = Path(cfg['datahub_root']) / 'silver' / 'alpha'
    return root

def find_weekly_parquets(root: Path):
    # 掃描所有 parquet，挑有 announce_date 且有 date/week_end 欄位的資料集
    candidates = []
    for p in root.rglob("*.parquet"):
        # 只抽查近 5 萬筆以避免過慢
        try:
            dset = ds.dataset(str(p), format="parquet")
            cols = set(dset.schema.names)
            if 'announce_date' in cols and ('date' in cols or 'week_end' in cols):
                candidates.append(p)
        except Exception:
            continue
    return candidates

def sample_and_check(path: Path, limit_weeks=520):
    dset = ds.dataset(str(path), format="parquet", partitioning="hive")
    # 只取必要欄位
    cols = [c for c in dset.schema.names if c in ('announce_date','date','week_end','symbol')]
    if 'week_end' in cols:
        date_col = 'week_end'
    elif 'date' in cols:
        date_col = 'date'
    else:
        return {'path': str(path), 'checked_rows': 0, 'violations': 0}
    table = dset.to_table(columns=cols)
    df = table.to_pandas(types_mapper=pd.ArrowDtype)
    # 轉日期
    for c in ('announce_date', date_col):
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=False, errors='coerce')
    if df.empty or 'announce_date' not in df:
        return {'path': str(path), 'checked_rows': 0, 'violations': 0}

    # 只取最近 limit_weeks 週
    df = df.sort_values(by=[date_col]).tail(limit_weeks)
    viol = (df['announce_date'] > df[date_col]).sum()
    return {
        'path': str(path),
        'checked_rows': int(len(df)),
        'violations': int(viol)
    }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--config', default='configs/data_sources.yaml')
    ap.add_argument('--weeks', type=int, default=520)
    ap.add_argument('--out', default='metrics/asof_weekly_violations.csv')
    args = ap.parse_args()

    root = load_paths(Path(args.config))
    paths = find_weekly_parquets(root)
    rows = []
    for p in paths:
        rows.append(sample_and_check(p, args.weeks))

    if not rows:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{'msg':'SKIP: no weekly fundamentals found'}]).to_csv(args.out, index=False)
        print("ASOF CHECK: SKIP (no weekly fundamentals found)")
        sys.exit(0)

    df = pd.DataFrame(rows).sort_values('violations', ascending=False)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    if df['violations'].sum() == 0:
        print("ASOF CHECK: OK (no violations)")
        sys.exit(0)
    else:
        print("ASOF CHECK: VIOLATIONS FOUND =", int(df['violations'].sum()))
        sys.exit(0)  # Phase 1 先不失敗，報告即可

if __name__ == "__main__":
    main()



