#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Phase 1: 依資料層規則（非策略）重建可投資池。
需要：prices（近 60 交易日），欄位至少含 symbol, date, close, volume 或 turnover。
"""
import argparse, sys
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
import yaml
from datetime import datetime, timedelta

def read_yaml(p): 
    import yaml, io
    with open(p,'r',encoding='utf-8') as f: 
        return yaml.safe_load(f)

def load_prices(root: Path, days=60, limit_files=50):
    paths = list((root / 'prices').rglob('*.parquet'))[:limit_files]
    frames = []
    for p in paths:
        try:
            tbl = ds.dataset(str(p), format='parquet').to_table(columns=['symbol','date','close','volume'])
            df = tbl.to_pandas()
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            frames.append(df)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame(columns=['symbol','date','close','volume'])
    df = pd.concat(frames, ignore_index=True)
    # 近 days
    cutoff = df['date'].max() - pd.Timedelta(days=days)
    return df[df['date'] >= cutoff].copy()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rules', default='configs/universe.rules.local.yaml')
    ap.add_argument('--config', default='configs/data_sources.yaml')
    ap.add_argument('--out', default='configs/investable_universe.txt')
    args = ap.parse_args()

    cfg = read_yaml(args.config)
    rules = read_yaml(args.rules)
    root = Path(cfg['datahub_root']) / 'silver' / 'alpha'

    df = load_prices(root, days=60)
    if df.empty:
        print("UNIVERSE: SKIP (no price data)")
        Path(args.out).write_text("", encoding='utf-8')
        sys.exit(0)

    # 基本指標
    df['turnover'] = (df['close'] * df['volume']).astype('float64')
    px = df.groupby('symbol').agg(
        last_date=('date','max'),
        min_price=('close','min'),
        adv_turnover=('turnover','mean'),
        days=('date','nunique')
    ).reset_index()

    # 規則
    min_listing_days = rules.get('min_listing_days', 180)
    min_price = float(rules.get('min_price', 0))
    min_avg_turnover_mil = float(rules.get('min_avg_turnover_mil', 0))

    # 濾掉上市天數不足（以近 60 天粗略代理）
    uni = px.copy()
    uni = uni[uni['days'] >= min(60, min_listing_days * 0.3)]  # Phase1 粗略近似

    if min_price > 0:
        uni = uni[uni['min_price'] >= min_price]

    if min_avg_turnover_mil > 0:
        uni = uni[(uni['adv_turnover'] / 1_000_000.0) >= min_avg_turnover_mil]

    symbols = sorted(uni['symbol'].unique().tolist())
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text("\n".join(symbols), encoding='utf-8')
    print(f"UNIVERSE: {len(symbols)} symbols written -> {args.out}")

if __name__ == "__main__":
    main()
