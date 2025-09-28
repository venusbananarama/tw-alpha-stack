#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse, pandas as pd
def parse_args():
    p = argparse.ArgumentParser(); p.add_argument("--factors-path","-p",type=str,required=True); p.add_argument("--limit",type=int,default=8); return p.parse_args()
def main():
    a = parse_args(); df = pd.read_parquet(a.factors_path)
    date_col = next((c for c in df.columns if c.lower() in ("date","trade_date","dt")), None)
    sym_col  = next((c for c in df.columns if c.lower() in ("symbol","sid","code","ticker")), None)
    if date_col is None or sym_col is None: raise SystemExit("Need date and symbol columns in the parquet.")
    df[date_col] = pd.to_datetime(df[date_col]); df = df.sort_values([sym_col, date_col])
    df["_week"] = df[date_col].dt.to_period("W-FRI").dt.to_timestamp("W-FRI")
    idx = df.groupby([sym_col,"_week"])[date_col].idxmax()
    snap = df.loc[idx,[sym_col,"_week",date_col]].rename(columns={sym_col:"symbol","_week":"week",date_col:"last_trade_date"})
    sample = snap.sort_values(["week","symbol"]).groupby("week").head(10).head(a.limit); print(sample.to_string(index=False))
if __name__ == "__main__":
    main()
