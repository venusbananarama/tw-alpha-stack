#!/usr/bin/env python3
import argparse
from pathlib import Path
import pandas as pd

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--factors', required=True)
    ap.add_argument('--start', default=None)
    ap.add_argument('--end', default=None)
    ap.add_argument('--outdir', required=True)
    args = ap.parse_args()

    outdir = Path(args.outdir); outdir.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(args.factors)
    df['date'] = pd.to_datetime(df['date'])
    if args.start: df = df[df['date'] >= pd.to_datetime(args.start)]
    if args.end:   df = df[df['date'] <= pd.to_datetime(args.end)]
    df = df.sort_values(['date','symbol'])

    rb = df.groupby(df['date'].dt.to_period('W'))['date'].max().sort_values()
    try:
        rb = rb.astype('datetime64[ns]')
    except Exception:
        rb = pd.to_datetime(rb)

    date_counts = df.groupby('date')['symbol'].count()
    probe = (rb.to_frame(name='rb_date')
               .assign(rows=lambda x: x['rb_date'].map(date_counts).fillna(0).astype(int)))
    sample = outdir / 'weekly_rb_dates_sample.csv'
    probe.to_csv(sample, index=False)
    zeros = int((probe['rows']==0).sum())
    print(f'[DIAG] total weekly rb_dates = {len(probe)}, zero-row = {zeros}')
    print(f'[OUT] {sample}')

if __name__ == '__main__':
    main()
