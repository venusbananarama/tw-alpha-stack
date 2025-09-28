#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse, os, sys, json, pandas as pd
try:
    import yaml
except Exception:
    yaml = None
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--factors","-f",type=str,default="composite_score")
    p.add_argument("--outdir","-o",type=str,required=True)
    p.add_argument("--start","-s",type=str,default=""); p.add_argument("--end","-e",type=str,default="")
    p.add_argument("--factors-path","-p",type=str,default=""); p.add_argument("--config","-c",type=str,default="")
    return p.parse_args()
def normalize_factor_list(s: str): return list(dict.fromkeys(x.strip() for x in s.replace(","," ").split() if x.strip()))
def load_config(path: str):
    if not path or not os.path.exists(path): return {}
    if yaml is None: return {}
    try:
        with open(path,"r",encoding="utf-8") as f: return (yaml.safe_load(f) or {})
    except Exception: return {}
def find_default_factors_path():
    for c in [os.path.join("G:\\","AI","datahub","alpha","alpha_factors_fixed.parquet"),
              os.path.join("G:\\","AI","datahub","alpha","alpha_factors.parquet"),
              os.path.join("data","alpha_factors.parquet")]:
        if os.path.exists(c): return c
    raise FileNotFoundError("Cannot locate a factors parquet. Pass --factors-path.")
def main():
    a = parse_args(); os.makedirs(a.outdir, exist_ok=True)
    factors = normalize_factor_list(a.factors); _cfg = load_config(a.config)
    fac_path = a.factors_path or find_default_factors_path()
    print(f"[info] Loading factors: {fac_path}")
    df = pd.read_parquet(fac_path)
    cols = {c.lower(): c for c in df.columns}
    date_col = next((cols[k] for k in ("date","trade_date","dt") if k in cols), None)
    sym_col  = next((cols[k] for k in ("symbol","sid","code","ticker") if k in cols), None)
    if date_col is None or sym_col is None: raise KeyError("Need date and symbol columns (date/trade_date/dt, symbol/sid/code/ticker)")
    df[date_col] = pd.to_datetime(df[date_col]); df = df.sort_values([sym_col, date_col])
    if a.start: df = df[df[date_col] >= pd.to_datetime(a.start)]
    if a.end:   df = df[df[date_col] <= pd.to_datetime(a.end)]
    week = df[date_col].dt.to_period("W-FRI").dt.to_timestamp("W-FRI"); df = df.assign(_week=week)
    idx = df.groupby([sym_col,"_week"])[date_col].idxmax()
    snap = df.loc[idx,[sym_col,"_week",date_col] + [c for c in factors if c in df.columns]].rename(columns={sym_col:"symbol","_week":"week",date_col:"last_trade_date"}).sort_values(["week","symbol"]).reset_index(drop=True)
    missing = [c for c in factors if c not in df.columns]
    snap.to_csv(os.path.join(a.outdir,"weekly_snapshot.csv"),index=False,encoding="utf-8-sig")
    snap.groupby("week",as_index=False).head(10).to_csv(os.path.join(a.outdir,"preview.csv"),index=False,encoding="utf-8-sig")
    weeks = snap["week"].sort_values().unique()
    stats = {"range":[str(weeks[0].date()) if len(weeks)>0 else None, str(weeks[-1].date()) if len(weeks)>0 else None],
             "num_weeks":int(len(weeks)),"rows":int(len(snap)),
             "symbols_first_week":int(snap[snap["week"]==weeks[0]].shape[0]) if len(weeks)>0 else 0,
             "symbols_last_week":int(snap[snap["week"]==weeks[-1]].shape[0]) if len(weeks)>0 else 0,
             "requested_factors":factors,"missing_factors":missing,"config_loaded":bool(a.config and os.path.exists(a.config))}
    with open(os.path.join(a.outdir,"summary.txt"),"w",encoding="utf-8") as f:
        f.write("W-FRI weekly snapshot summary\n"+"="*32+"\n"); f.write(json.dumps(stats,ensure_ascii=False,indent=2)); f.write("\n")
    with open(os.path.join(a.outdir,"log_args.txt"),"w",encoding="utf-8") as f:
        f.write("python scripts/project_check.py "+" ".join(sys.argv[1:])+"\n")
    print("[ok] Wrote snapshot →", os.path.join(a.outdir,"weekly_snapshot.csv"))
    if missing: print("[warn] Missing factor columns:", missing)
if __name__ == "__main__":
    main()
