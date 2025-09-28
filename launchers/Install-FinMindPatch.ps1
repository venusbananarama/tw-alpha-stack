
$ErrorActionPreference = "Stop"

# Ensure we're at project root (if not, prompt)
if (-not (Test-Path ".\scripts")) {
  Write-Host "Select your project root folder (contains 'scripts' and 'configs')..."
  $folder = Read-Host "Path to project root (e.g. G:\AI\tw-alpha-stack)"
  if (-not (Test-Path $folder)) { throw "Path not found: $folder" }
  Set-Location $folder
}

# Create required directories
$dirs = @("configs","scripts","scripts\ps","data\finmind\raw","data\finmind\reports")
foreach ($d in $dirs) { New-Item -ItemType Directory -Path $d -Force | Out-Null }

# --- Write files ---

# configs/datasets.yaml
@'
# AlphaCity x FinMind — datasets grouping (2025-09-18)
groups:
  prices:
    - dataset: TaiwanStockPrice
      mode: market_wide_single_day_or_per_stock
      slice: month
    - dataset: TaiwanStockPriceAdj
      mode: market_wide_single_day_or_per_stock
      slice: month
    - dataset: TaiwanStockMarketValue
      mode: market_wide_single_day_or_per_stock
      slice: month
    - dataset: TaiwanStockDayTrading
      mode: market_wide_single_day_or_per_stock
      slice: month
  chip:
    - dataset: TaiwanStockInstitutionalInvestorsBuySell
      mode: market_wide_single_day_or_per_stock
      slice: month
    - dataset: TaiwanStockTotalInstitutionalInvestors
      mode: market_wide_single_day_or_per_stock
      slice: month
    - dataset: TaiwanStockGovernmentBankBuySell
      mode: per_day
      slice: day
  derivatives:
    - dataset: TaiwanFuturesDaily
      mode: market_wide_or_fallback_per_stock
      slice: month
    - dataset: TaiwanOptionDaily
      mode: market_wide_or_fallback_per_stock
      slice: month
  macro_others:
    - dataset: TaiwanStockTotalReturnIndex
      mode: with_params
      slice: month
      params:
        data_id: ["TAIEX", "OTC"]
'@ | Out-File -Encoding utf8 -NoNewline configs/datasets.yaml

# scripts/ps/Invoke-FMBackfill.ps1
@'
param(
  [Parameter(Mandatory=$true)][string]$Start,
  [Parameter(Mandatory=$true)][string]$End,
  [Parameter(Mandatory=$true)][string[]]$Datasets,
  [string]$Universe = "configs\\universe.tw_all.txt",
  [string]$Extra = ""
)
$ErrorActionPreference = "Stop"
Write-Host "== FMBackfill =="
$dsList = @()
foreach ($d in $Datasets) { $dsList += ($d -split ",") | ForEach-Object { $_.Trim() } | Where-Object { $_ } }
if ($dsList.Count -eq 0) { throw "No dataset groups provided." }
Write-Host ("Start={0} End={1} Datasets={2}" -f $Start,$End,($dsList -join ","))
if (-not $env:FINMIND_TOKEN) { throw 'FINMIND_TOKEN is empty. 請先：$env:FINMIND_TOKEN = "your-token-here"' }
$pyArgs = @("scripts/finmind_backfill.py","--start",$Start,"--end",$End,"--datasets") + $dsList + @("--datasets-yaml","configs/datasets.yaml","--universe",$Universe)
python @pyArgs
'@ | Out-File -Encoding utf8 -NoNewline scripts/ps/Invoke-FMBackfill.ps1

# scripts/ps/Run-FMDaily.ps1
@'
param(
  [int]$LastNDays = 5,
  [string[]]$Groups = @("prices","chip","derivatives","macro_others"),
  [string]$DatasetsYaml = "configs/datasets.yaml"
)
$ErrorActionPreference = "Stop"
Write-Host "=== [AlphaCity] Daily EOD Update ==="
if (-not $env:FINMIND_TOKEN) { throw 'FINMIND_TOKEN is empty. 請先：$env:FINMIND_TOKEN = "your-token-here"' }
Write-Host "== EOD Flow =="
python scripts/finmind_daily_update.py --last-n-days $LastNDays --datasets-yaml $DatasetsYaml --groups ($Groups -join ",")
'@ | Out-File -Encoding utf8 -NoNewline scripts/ps/Run-FMDaily.ps1

# scripts/ps/Set-FMToken.ps1
@'
param(
  [Parameter(Mandatory=$true)][string]$Token,
  [switch]$OnlyCurrentSession = $false
)
$ErrorActionPreference = "Stop"
$env:FINMIND_TOKEN = $Token
Write-Host "[OK] Set FINMIND_TOKEN for current session."
if (-not $OnlyCurrentSession) {
  setx FINMIND_TOKEN $Token | Out-Null
  Write-Host "[OK] Persisted FINMIND_TOKEN to user environment. (Open a new PowerShell)"
}
'@ | Out-File -Encoding utf8 -NoNewline scripts/ps/Set-FMToken.ps1

# scripts/ps/Go-AlphaCity.ps1
@'
Set-Location "G:\AI\tw-alpha-stack"
Get-Location
'@ | Out-File -Encoding utf8 -NoNewline scripts/ps/Go-AlphaCity.ps1

# scripts/finmind_backfill.py
@'
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse, os, sys
from typing import List, Dict, Any, Tuple
import pandas as pd
try:
    import requests
except Exception:
    print("[ERROR] `requests` is required. pip install requests", file=sys.stderr); raise
try:
    import yaml
except Exception:
    print("[ERROR] `pyyaml` is required. pip install pyyaml", file=sys.stderr); raise
RAW_ROOT = os.path.join("data","finmind","raw")
def read_yaml(path):
    import yaml
    with open(path,"r",encoding="utf-8") as f:
        return yaml.safe_load(f)
def read_universe(path):
    if not os.path.exists(path):
        print(f"[WARN] Universe not found: {path}")
        return []
    with open(path,"r",encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
def month_slices(start_date,end_date):
    idx = pd.period_range(start=start_date, end=end_date, freq="M")
    if len(idx)==0: return [(start_date,end_date)]
    out=[]
    for p in idx:
        s=p.asfreq("D","start").strftime("%Y-%m-%d")
        e=p.asfreq("D","end").strftime("%Y-%m-%d")
        if s<start_date: s=start_date
        if e>end_date: e=end_date
        out.append((s,e))
    return out
def day_range(s,e): return [d.strftime("%Y-%m-%d") for d in pd.date_range(s,e,freq="D")]
def ensure_parent(path): os.makedirs(os.path.dirname(path), exist_ok=True)
def write_parquet(df, path):
    if df is None or df.empty: print(f"[INFO] Empty frame, skip write: {path}"); return False
    ensure_parent(path); df.to_parquet(path, index=False); print(f"[INFO] Wrote: {path} rows={len(df)}"); return True
def fm_get(params, token, timeout=60):
    url="https://api.finmindtrade.com/api/v4/data"; headers={"Authorization": f"Bearer {token}"} if token else {}
    r=requests.get(url, headers=headers, params=params, timeout=timeout)
    try: j=r.json()
    except Exception: j={"status": r.status_code, "msg": r.text}
    if r.status_code!=200 or j.get("status") not in (200,"200"): raise RuntimeError(f"HTTP {r.status_code}: {j}")
    return j
def expand_groups(requested, cfg):
    groups = cfg.get("groups", {}); out=[]
    for key in requested:
        if key in groups: out += groups[key]
        else: out.append({"dataset": key, "mode":"market_wide_or_fallback_per_stock","slice":"month"})
    return out
def fetch_market_wide_single_day(ds,s,e,params,token):
    frames=[]
    for d in day_range(s,e):
        p={"dataset":ds,"start_date":d}
        p.update({k:v for k,v in params.items() if k!="data_id"})
        try:
            j=fm_get(p, token); di=pd.DataFrame(j.get("data",[]))
            if not di.empty: frames.append(di)
        except Exception as ex:
            print(f"[WARN] market-wide(single-day) failed {ds} {d}: {ex}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
def fetch_per_stock(ds,s,e,universe,params,token):
    frames=[]
    for sid in universe:
        p={"dataset":ds,"data_id":sid,"start_date":s,"end_date":e}
        p.update({k:v for k,v in params.items() if k!="data_id"})
        try:
            j=fm_get(p, token); di=pd.DataFrame(j.get("data",[]))
            if not di.empty: frames.append(di)
        except Exception as ex:
            print(f"[WARN] per-stock failed {ds} {sid} {s}..{e}: {ex}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
def fetch_slice(item,s,e,universe,token):
    ds=item["dataset"]; mode=item.get("mode","market_wide_or_fallback_per_stock"); params=item.get("params",{})
    if ds=="TaiwanStockTotalReturnIndex":
        frames=[]
        for idx in params.get("data_id", []):
            j=fm_get({"dataset":ds,"data_id":idx,"start_date":s,"end_date":e}, token); frames.append(pd.DataFrame(j.get("data",[])))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if ds=="TaiwanStockGovernmentBankBuySell":
        frames=[]
        for d in day_range(s,e):
            j=fm_get({"dataset":ds,"start_date":d}, token); frames.append(pd.DataFrame(j.get("data",[])))
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if mode=="market_wide_single_day_or_per_stock":
        df=fetch_market_wide_single_day(ds,s,e,params,token)
        if not df.empty: return df
        print(f"[WARN] [{ds}] market-wide(single-day) empty → fallback per-stock {s}..{e}")
        return fetch_per_stock(ds,s,e,universe,params,token)
    if mode=="market_wide_or_fallback_per_stock":
        try:
            j=fm_get({"dataset":ds,"start_date":s,"end_date":e, **params}, token); df=pd.DataFrame(j.get("data",[]))
        except Exception as ex:
            print(f"[WARN] market-wide failed {ds} {s}..{e}: {ex}"); df=pd.DataFrame()
        if not df.empty: return df
        print(f"[WARN] [{ds}] market-wide empty → fallback per-stock {s}..{e}")
        return fetch_per_stock(ds,s,e,universe,params,token)
    j=fm_get({"dataset":ds,"start_date":s,"end_date":e}, token); return pd.DataFrame(j.get("data",[]))
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--start", required=True); ap.add_argument("--end", required=True)
    ap.add_argument("--datasets", nargs="+", required=True)
    ap.add_argument("--datasets-yaml", default="configs/datasets.yaml")
    ap.add_argument("--universe", default="configs/universe.tw_all.txt")
    args=ap.parse_args()
    token=os.environ.get("FINMIND_TOKEN","").strip()
    if not token: print("[ERROR] FINMIND_TOKEN is empty.", file=sys.stderr); sys.exit(2)
    cfg=read_yaml(args.datasets_yaml); universe=read_universe(args.universe)
    items=expand_groups(args.datasets, cfg)
    for item in items:
        ds=item["dataset"]; out_dir=os.path.join(RAW_ROOT, ds); os.makedirs(out_dir, exist_ok=True)
        # month slices
        idx = pd.period_range(start=args.start, end=args.end, freq="M")
        slices=[(args.start,args.end)] if len(idx)==0 else [(max(p.asfreq("D","start").strftime("%Y-%m-%d"), args.start),
                                                            min(p.asfreq("D","end").strftime("%Y-%m-%d"), args.end)) for p in idx]
        print(f"== Dataset {ds} ==")
        for s,e in slices:
            try:
                df=fetch_slice(item,s,e,universe,token)
                out_path=os.path.join(out_dir, f"{ds}__{s}_to_{e}.parquet")
                write_parquet(df,out_path)
            except Exception as ex:
                print(f"[ERROR] {ds} {s}..{e} → {ex}", file=sys.stderr); continue
if __name__=="__main__": main()
'@ | Out-File -Encoding utf8 -NoNewline scripts/finmind_backfill.py

# scripts/finmind_daily_update.py
@'
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse, os, sys
from datetime import datetime, timedelta
from typing import List, Dict, Any
import pandas as pd
try:
    import requests
except Exception:
    print("[ERROR] `requests` is required. pip install requests", file=sys.stderr); raise
try:
    import yaml
except Exception:
    print("[ERROR] `pyyaml` is required. pip install pyyaml", file=sys.stderr); raise
RAW_ROOT=os.path.join("data","finmind","raw"); REPORT_ROOT=os.path.join("data","finmind","reports")
def read_yaml(path): 
    import yaml; 
    with open(path,"r",encoding="utf-8") as f: return yaml.safe_load(f)
def ensure_parent(path): os.makedirs(os.path.dirname(path), exist_ok=True)
def write_parquet(df, path):
    if df is None or df.empty: print(f"[INFO] Empty frame, skip write: {path}"); return False
    ensure_parent(path); df.to_parquet(path, index=False); print(f"[INFO] Wrote: {path} rows={len(df)}"); return True
def fm_get(params, token, timeout=60):
    url="https://api.finmindtrade.com/api/v4/data"; headers={"Authorization": f"Bearer {token}"} if token else {}
    r=requests.get(url, headers=headers, params=params, timeout=timeout)
    try: j=r.json()
    except Exception: j={"status": r.status_code, "msg": r.text}
    if r.status_code != 200 or j.get("status") not in (200,"200"): raise RuntimeError(f"HTTP {r.status_code}: {j}")
    return j
def fetch_market_wide_single_day(ds, day, params, token):
    p={"dataset":ds,"start_date":day}; p.update({k:v for k,v in params.items() if k!="data_id"})
    j=fm_get(p, token); return pd.DataFrame(j.get("data",[]))
def fetch_per_stock_range(ds, start, end, universe, params, token):
    frames=[]; 
    for sid in universe:
        p={"dataset":ds,"data_id":sid,"start_date":start,"end_date":end}; p.update({k:v for k,v in params.items() if k!="data_id"})
        try:
            j=fm_get(p, token); di=pd.DataFrame(j.get("data",[]))
            if not di.empty: frames.append(di)
        except Exception as ex:
            print(f"[WARN] per-stock failed {ds} {sid} {start}..{end}: {ex}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--last-n-days", type=int, default=5)
    ap.add_argument("--datasets-yaml", default="configs/datasets.yaml")
    ap.add_argument("--groups", default="prices,chip,derivatives,macro_others")
    ap.add_argument("--universe", default="configs/universe.tw_all.txt")
    args=ap.parse_args()
    token=os.environ.get("FINMIND_TOKEN","").strip()
    if not token: print("[ERROR] FINMIND_TOKEN is empty.", file=sys.stderr); sys.exit(2)
    end=datetime.utcnow().strftime("%Y-%m-%d"); start_dt=(datetime.utcnow()-timedelta(days=max(1,args.last_n_days-1))); start=start_dt.strftime("%Y-%m-%d")
    cfg=read_yaml(args.datasets_yaml); groups=[g.strip() for g in args.groups.split(",") if g.strip()]
    universe=[]
    if os.path.exists(args.universe):
        with open(args.universe,"r",encoding="utf-8") as f: universe=[ln.strip() for ln in f if ln.strip() and not ln.startswith("#")]
    ds_cfgs=[]; all_groups=cfg.get("groups",{})
    for g in groups: ds_cfgs.extend(all_groups[g] if g in all_groups else [{"dataset": g, "mode":"market_wide_or_fallback_per_stock"}])
    for item in ds_cfgs:
        ds=item["dataset"]; mode=item.get("mode","market_wide_or_fallback_per_stock"); params=item.get("params", {})
        print(f"== Daily dataset {ds} ==")
        try:
            if ds=="TaiwanStockTotalReturnIndex":
                frames=[]
                for idx in params.get("data_id", []):
                    j=fm_get({"dataset":ds,"data_id":idx,"start_date":start,"end_date":end}, token); frames.append(pd.DataFrame(j.get("data",[])))
                df=pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            elif ds=="TaiwanStockGovernmentBankBuySell":
                frames=[]
                for d in pd.date_range(start, end, freq="D"):
                    day=d.strftime("%Y-%m-%d"); j=fm_get({"dataset":ds,"start_date":day}, token); frames.append(pd.DataFrame(j.get("data",[])))
                df=pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
            elif mode=="market_wide_single_day_or_per_stock":
                frames=[]
                for d in pd.date_range(start, end, freq="D"):
                    day=d.strftime("%Y-%m-%d")
                    try:
                        di=fetch_market_wide_single_day(ds, day, params, token)
                        if not di.empty: frames.append(di)
                    except Exception as ex:
                        print(f"[WARN] market-wide(single-day) failed {ds} {day}: {ex}")
                df=pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                if df.empty:
                    print(f"[WARN] [{ds}] market-wide(single-day) empty → fallback per-stock {start}..{end}")
                    df=fetch_per_stock_range(ds, start, end, universe, params, token)
            else:
                try:
                    j=fm_get({"dataset":ds,"start_date":start,"end_date":end, **params}, token); df=pd.DataFrame(j.get("data",[]))
                except Exception as ex:
                    print(f"[WARN] market-wide failed {ds} {start}..{end}: {ex}")
                    df=fetch_per_stock_range(ds, start, end, universe, params, token)
            out_dir=os.path.join(RAW_ROOT, ds); os.makedirs(out_dir, exist_ok=True)
            out_path=os.path.join(out_dir, f"{ds}__{start}_to_{end}.parquet")
            write_parquet(df, out_path)
        except Exception as ex:
            print(f"[ERROR] Daily fetch failed dataset={ds} {start}..{end} → {ex}", file=sys.stderr); continue
    verify_path=os.path.join(REPORT_ROOT, "daily_verify.csv")
    rows=[]
    for item in ds_cfgs:
        ds=item["dataset"]; dirp=os.path.join(RAW_ROOT, ds)
        if os.path.exists(dirp):
            for fn in sorted(os.listdir(dirp))[-5:]:
                fp=os.path.join(dirp, fn); rows.append({"dataset":ds,"file":fn,"size":os.path.getsize(fp)})
    pd.DataFrame(rows).to_csv(verify_path, index=False, encoding="utf-8")
    print(f"[INFO] Wrote verify: {verify_path}")
if __name__=="__main__": main()
'@ | Out-File -Encoding utf8 -NoNewline scripts/finmind_daily_update.py

Write-Host "[DONE] Files created."
