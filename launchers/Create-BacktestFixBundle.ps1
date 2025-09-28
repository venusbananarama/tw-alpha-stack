# Create-BacktestFixBundle.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root
New-Item -ItemType Directory -Force -Path (Join-Path $root "scripts") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $root "backtest") | Out-Null

# --- unified_run_backtest.ps1 ---
@'
param(
    [Parameter(Mandatory=$false)][string]$Factors = "composite_score",
    [Parameter(Mandatory=$false)][string]$OutDir = "",
    [Parameter(Mandatory=$false)][string]$Start = "",
    [Parameter(Mandatory=$false)][string]$End = "",
    [Parameter(Mandatory=$false)][string]$FactorsPath = "",
    [Parameter(Mandatory=$false)][string]$Config = "configs\backtest_topN_example.yaml",
    [Parameter(Mandatory=$false)][int]$TopN = 50,
    [Parameter(Mandatory=$false)][string]$Rebalance = "W",
    [Parameter(Mandatory=$false)][double]$Costs = 0.0005,
    [Parameter(Mandatory=$false)][int]$Seed = 42
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not $OutDir -or $OutDir -eq "") {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $OutDir = Join-Path $root ("out\backtest_" + $stamp)
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

$venvPy = Join-Path $root ".venv\Scripts\python.exe"
$python = (Test-Path $venvPy) ? $venvPy : "python"

$mergedCfg = Join-Path $OutDir "merged_config.yaml"
$cfgArgs = @("scripts\config_merge.py","--base",$Config,"--out",$mergedCfg,"--topn",$TopN,"--rebalance",$Rebalance,"--costs",$Costs,"--seed",$Seed)
if ($Start) { $cfgArgs += @("--start",$Start) }
if ($End) { $cfgArgs += @("--end",$End) }
if ($FactorsPath) { $cfgArgs += @("--factors-path",$FactorsPath) }
if ($Factors) { $cfgArgs += @("--factor-cols",$Factors) }

Write-Host "Merging config ->" $mergedCfg
& $python $cfgArgs
if ($LASTEXITCODE -ne 0) { throw "config_merge.py failed" }

$origScript = "backtest\longonly_topN.py"
$useFallback = $false

if (Test-Path $origScript) {
    Write-Host "Detected original script:" $origScript
    try {
        $args = @($origScript,"--out-dir",$OutDir,"--config",$mergedCfg)
        if ($FactorsPath) { $args += @("--factors",$FactorsPath) }  # 若原腳本把 --factors 當作檔案路徑，這行就派上用場
        Write-Host "Running original backtest with unified args mapping..."
        & $python $args
        if ($LASTEXITCODE -ne 0) { $useFallback = $true }
    } catch { $useFallback = $true }
} else { $useFallback = $true }

if ($useFallback) {
    Write-Warning "Falling back to simple TopN engine (simulate_topN.py)."
    $args = @("backtest\simulate_topN.py","--factors",$Factors,"--outdir",$OutDir,"--topn",$TopN,"--rebalance",$Rebalance)
    if ($Start) { $args += @("--start",$Start) }
    if ($End) { $args += @("--end",$End) }
    if ($FactorsPath) { $args += @("--factors-path",$FactorsPath) }
    Write-Host "Running:" $python $args
    & $python $args
    if ($LASTEXITCODE -ne 0) { throw "simulate_topN.py failed with exit code $LASTEXITCODE" }
}

Write-Host "`n✓ Backtest finished. Outputs in:" $OutDir
'@ | Set-Content -Encoding UTF8 (Join-Path $root "unified_run_backtest.ps1")

# --- scripts/config_merge.py ---
@'
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse, os, sys, json
from datetime import datetime
try:
    import yaml
except Exception:
    yaml = None

def load_yaml(path):
    if not path or not os.path.exists(path):
        return {}
    if yaml is None:
        print("[warn] pyyaml not installed; returning empty base config", file=sys.stderr)
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

def dump_yaml(obj, path):
    if yaml is None:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    else:
        with open(path, "w", encoding="utf-8") as f:
            yaml.safe_dump(obj, f, allow_unicode=True, sort_keys=False)

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base", type=str, default="")
    p.add_argument("--out", type=str, required=True)
    p.add_argument("--topn", type=int, default=None)
    p.add_argument("--rebalance", type=str, default=None)
    p.add_argument("--costs", type=float, default=None)
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--start", type=str, default=None)
    p.add_argument("--end", type=str, default=None)
    p.add_argument("--factors-path", type=str, default=None)
    p.add_argument("--factor-cols", type=str, default=None)
    args = p.parse_args()

    cfg = load_yaml(args.base)
    cfg.setdefault("meta", {})["merged_at"] = datetime.now().isoformat(timespec="seconds")
    if args.topn is not None: cfg["topN"] = int(args.topn)
    if args.rebalance: cfg["rebalance"] = args.rebalance
    if args.costs is not None: cfg["costs"] = float(args.costs)
    if args.seed is not None: cfg["seed"] = int(args.seed)
    if args.start: cfg["start"] = args.start
    if args.end: cfg["end"] = args.end
    if args.factors_path: cfg["factors_path"] = args.factors_path
    if args.factor_cols:
        raw = [x.strip() for x in args.factor_cols.replace(",", " ").split() if x.strip()]
        cfg["factor_columns"] = raw

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    dump_yaml(cfg, args.out)
    print("[ok] wrote merged config ->", args.out)

if __name__ == "__main__":
    main()
'@ | Set-Content -Encoding UTF8 (Join-Path $root "scripts\config_merge.py")

# --- backtest/simulate_topN.py ---
@'
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Simple Top-N fallback backtest:
- Weekly/Monthly rebalance
- Rank by factor columns (desc), mean z-score if multiple
- Equal-weight, basic NAV/returns
"""
import argparse, os, sys, math, json
from datetime import datetime
import numpy as np
import pandas as pd

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--factors", type=str, default="composite_score")
    p.add_argument("--factors-path", type=str, required=True)
    p.add_argument("--outdir", type=str, required=True)
    p.add_argument("--start", type=str, default="")
    p.add_argument("--end", type=str, default="")
    p.add_argument("--topn", type=int, default=50)
    p.add_argument("--rebalance", type=str, default="W", choices=["W","M"])
    p.add_argument("--costs", type=float, default=0.0005)
    return p.parse_args()

def find_cols(df):
    date_col = next((c for c in df.columns if c.lower() in ("date","trade_date","dt")), None)
    sym_col  = next((c for c in df.columns if c.lower() in ("symbol","sid","code","ticker")), None)
    px_col   = next((k for k in ("adj_close","close","close_price","px_close") if k in df.columns), None)
    ret_col  = next((k for k in ("ret_1d","return_1d","ret") if k in df.columns), None)
    if date_col is None or sym_col is None:
        raise KeyError("Need date and symbol columns in parquet.")
    return date_col, sym_col, px_col, ret_col

def to_period_stamp(series, mode):
    return series.dt.to_period("W-FRI").dt.to_timestamp("W-FRI") if mode=="W" else series.dt.to_period("M").dt.to_timestamp("M")

def compute_daily_ret(df, date_col, sym_col, px_col, ret_col):
    if ret_col is not None:
        return df[[date_col, sym_col, ret_col]].rename(columns={ret_col: "ret_1d"})
    if px_col is None:
        raise KeyError("No returns nor price columns found. Need adj_close/close or ret_1d.")
    out = df[[date_col, sym_col, px_col]].copy().sort_values([sym_col, date_col])
    out["ret_1d"] = out.groupby(sym_col)[px_col].pct_change()
    return out[[date_col, sym_col, "ret_1d"]]

def main():
    args = parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    fac_cols = [x.strip() for x in args.factors.replace(",", " ").split() if x.strip()]

    df = pd.read_parquet(args.factors_path)
    date_col, sym_col, px_col, ret_col = find_cols(df)
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.sort_values([sym_col, date_col])

    if args.start: df = df[df[date_col] >= pd.to_datetime(args.start)]
    if args.end:   df = df[df[date_col] <= pd.to_datetime(args.end)]

    period_mode = "W" if args.rebalance=="W" else "M"
    df["_period"] = to_period_stamp(df[date_col], period_mode)
    idx  = df.groupby([sym_col, "_period"])[date_col].idxmax()
    snap = df.loc[idx, [sym_col, "_period", date_col] + [c for c in fac_cols if c in df.columns]].rename(
        columns={sym_col:"symbol", "_period":"period", date_col:"last_dt"}
    ).sort_values(["period","symbol"])

    ret_daily = compute_daily_ret(df, date_col, sym_col, px_col, ret_col).rename(columns={date_col:"date", sym_col:"symbol"})
    ret_daily = ret_daily.sort_values(["symbol","date"])
    ret_daily["period"] = to_period_stamp(ret_daily["date"], period_mode)
    sym_ret = ret_daily.groupby(["symbol","period"])["ret_1d"].apply(lambda s: (1+s).prod()-1).reset_index()

    base = snap.copy()
    if len(fac_cols) == 1:
        base["rank_key"] = base[fac_cols[0]]
    else:
        zs = []
        for c in fac_cols:
            if c in base.columns:
                z = base.groupby("period")[c].transform(lambda s: (s - s.mean()) / (s.std(ddof=0)+1e-12))
                zs.append(z)
        if not zs: raise KeyError("None of the requested factor columns exist in data.")
        base["rank_key"] = np.vstack(zs).mean(axis=0)

    base["rank"] = base.groupby("period")["rank_key"].rank(ascending=False, method="first")
    picks = base[base["rank"] <= args.topn][["period","symbol"]].copy()
    picks["weight"] = 1.0 / args.topn

    sym_ret = sym_ret.rename(columns={"period":"next_period"})
    pnl = picks.merge(sym_ret, on=["symbol","next_period"], how="left").dropna(subset=["ret_1d"])
    pnl["contrib"] = pnl["weight"] * pnl["ret_1d"]
    port = pnl.groupby("next_period")["contrib"].sum().reset_index().rename(columns={"contrib":"ret"})
    port = port.sort_values("next_period")
    port["nav"] = (1 + port["ret"]).cumprod()
    port["date"] = port["next_period"].dt.date

    if len(port) > 1:
        periods_per_year = 52 if args.rebalance=="W" else 12
        cagr = port["nav"].iloc[-1] ** (periods_per_year / len(port)) - 1
        vol  = port["ret"].std(ddof=0) * (periods_per_year ** 0.5)
        sharpe = (cagr / vol) if vol > 0 else float("nan")
    else:
        cagr = vol = sharpe = float("nan")

    stats = {
        "period": "Weekly" if args.rebalance=="W" else "Monthly",
        "topN": int(args.topn),
        "range": [str(port["date"].iloc[0]) if len(port)>0 else None, str(port["date"].iloc[-1]) if len(port)>0 else None],
        "bars": int(len(port)),
        "CAGR": None if isinstance(cagr, float) and (np.isnan(cagr)) else float(cagr),
        "Vol":   None if isinstance(vol, float)  and (np.isnan(vol))   else float(vol),
        "Sharpe≈": None if isinstance(sharpe, float) and (np.isnan(sharpe)) else float(sharpe),
        "factors": fac_cols
    }

    nav_path = os.path.join(args.outdir, "nav.csv")
    sum_path = os.path.join(args.outdir, "summary_backtest.txt")
    port[["date","ret","nav"]].to_csv(nav_path, index=False, encoding="utf-8-sig")
    with open(sum_path, "w", encoding="utf-8") as f:
        f.write("TopN fallback backtest summary\n")
        f.write("="*30 + "\n")
        f.write(json.dumps(stats, ensure_ascii=False, indent=2))
        f.write("\n")

    print("[ok] wrote NAV ->", nav_path)
    print("[ok] wrote summary ->", sum_path)

if __name__ == "__main__":
    main()
'@ | Set-Content -Encoding UTF8 (Join-Path $root "backtest\simulate_topN.py")

# --- README ---
@'
# backtest_fix_bundle_v1
統一參數介面 + 後備回測引擎，與 weekly_check 相容。

**入口：** `unified_run_backtest.ps1`
支援：`-Factors -OutDir -Start -End -FactorsPath -Config -TopN -Rebalance -Costs -Seed`

**流程：**
1) 合併設定 → `merged_config.yaml`
2) 優先呼叫原本 `backtest\longonly_topN.py`
3) 若失敗 → 啟用 `backtest\simulate_topN.py`（等權 TopN，週/月重平衡）

**示例：**
.\unified_run_backtest.ps1 `
  -Factors "composite_score mom_252_21 vol_20" `
  -OutDir "G:\AI\datahub\alpha\backtests\topN_50_W" `
  -Start "2015-01-01" -End "2020-12-31" `
  -FactorsPath "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" `
  -Config "configs\backtest_topN_example.yaml" `
  -TopN 50 -Rebalance "W" -Costs 0.0005
'@ | Set-Content -Encoding UTF8 (Join-Path $root "README_backtest_fix.txt")

Write-Host "`n✓ Installed backtest_fix_bundle_v1 files into:" $root
