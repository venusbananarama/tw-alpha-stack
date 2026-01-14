#requires -Version 7
[CmdletBinding(PositionalBinding = $false)]
param(
    [ValidateSet('verify_only','run_and_verify')]
    [string]$Mode = 'verify_only',

    [string]$RunDir,
    [string]$AsOf,

    [string]$PricesDailyPath = 'datahub/silver/alpha/prices_daily.parquet',
    [string]$FactorsPath = 'datahub/silver/alpha/factor/sample_factors.parquet',
    [string]$ConfigPath = 'configs/backtest_topN_fixed.yaml',

    [string]$BacktestScript = 'backtest/longonly_topN.py'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:Failed = $false

function Write-Info {
    param([Parameter(Mandatory)][string]$Message)
    Write-Host "[INFO] $Message"
}

function Write-Fail {
    param(
        [Parameter(Mandatory)][string]$Code,
        [Parameter(Mandatory)][string]$Message
    )
    Write-Host "[FAIL][$Code] $Message" -ForegroundColor Red
    $script:Failed = $true
}

function Resolve-RepoPath {
    param([string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path)) {
        return $null
    }
    if ([System.IO.Path]::IsPathRooted($Path)) {
        return $Path
    }
    return (Join-Path $script:RepoRoot $Path)
}

$ScriptDir = if ($PSScriptRoot) {
    $PSScriptRoot
} else {
    (Resolve-Path '.').Path
}
$ToolsDir = Split-Path -Parent $ScriptDir
$RepoRoot = Split-Path -Parent $ToolsDir
$script:RepoRoot = $RepoRoot
Set-Location $RepoRoot

if ($Mode -eq 'verify_only') {
    if (-not $RunDir) {
        Write-Fail -Code 'BT_PHASE2_ARGS' -Message 'RunDir is required for verify_only.'
        exit 1
    }
}
elseif ($Mode -eq 'run_and_verify') {
    if (-not $RunDir) {
        Write-Fail -Code 'BT_PHASE2_ARGS' -Message 'RunDir is required for run_and_verify.'
        exit 1
    }
    if (-not $AsOf) {
        Write-Fail -Code 'BT_PHASE2_ARGS' -Message 'AsOf is required for run_and_verify.'
        exit 1
    }
}

$RunDirFull = Resolve-RepoPath $RunDir
$PricesDailyPathFull = Resolve-RepoPath $PricesDailyPath
$FactorsPathFull = Resolve-RepoPath $FactorsPath
$ConfigPathFull = Resolve-RepoPath $ConfigPath
$BacktestScriptPath = Resolve-RepoPath $BacktestScript

$PythonExe = Join-Path $RepoRoot '.venv\Scripts\python.exe'
if (-not (Test-Path $PythonExe)) {
    $PythonExe = 'python'
}

if ($Mode -eq 'run_and_verify') {
    if (-not (Test-Path $BacktestScriptPath)) {
        Write-Fail -Code 'BT_PHASE2_RUN_MISSING' -Message "Backtest script not found: $BacktestScriptPath"
        exit 1
    }
    if (-not (Test-Path $FactorsPathFull)) {
        Write-Fail -Code 'BT_PHASE2_RUN_MISSING' -Message "Factors file not found: $FactorsPathFull"
        exit 1
    }
    if (-not (Test-Path $ConfigPathFull)) {
        Write-Fail -Code 'BT_PHASE2_RUN_MISSING' -Message "Config file not found: $ConfigPathFull"
        exit 1
    }
    New-Item -ItemType Directory -Force -Path $RunDirFull | Out-Null
    Write-Info "Running backtest: $BacktestScriptPath"
    & $PythonExe $BacktestScriptPath `
        --factors $FactorsPathFull `
        --out-dir $RunDirFull `
        --config $ConfigPathFull `
        --as-of $AsOf
    if ($LASTEXITCODE -ne 0) {
        Write-Fail -Code 'BT_PHASE2_RUN_FAIL' -Message "Backtest failed with exit code $LASTEXITCODE."
        exit 1
    }
}

if (-not (Test-Path $RunDirFull)) {
    Write-Fail -Code 'BT_PHASE2_RUN_DIR' -Message "RunDir not found: $RunDirFull"
    exit 1
}

$requiredFiles = @(
    'eligibility_drops.csv',
    'eligibility_summary.json',
    'positions.csv',
    'nav_clean.csv',
    'metrics.json'
)
$missing = @()
foreach ($f in $requiredFiles) {
    $p = Join-Path $RunDirFull $f
    if (-not (Test-Path $p)) {
        $missing += $f
    }
}
if ($missing.Count -gt 0) {
    Write-Fail -Code 'BT_PHASE2_FILE_MISSING' -Message ("Missing output files: " + ($missing -join ', '))
    exit 1
}

function Invoke-PythonCheck {
    param(
        [Parameter(Mandatory)][string]$Code,
        [Parameter(Mandatory)][string[]]$Args,
        [Parameter(Mandatory)][string]$FailCode,
        [Parameter(Mandatory)][string]$FailMessage
    )
    $Code | & $PythonExe @Args
    if ($LASTEXITCODE -ne 0) {
        Write-Fail -Code $FailCode -Message $FailMessage
        return $false
    }
    return $true
}

$checkMissingPrice = @'
import os
import sys
import pandas as pd

run_dir = sys.argv[1]
prices_path = sys.argv[2]
pos_path = os.path.join(run_dir, "positions.csv")
pos = pd.read_csv(pos_path, dtype={"symbol": str})
px = pd.read_parquet(prices_path, columns=["date", "symbol", "close"])
pos["date"] = pd.to_datetime(pos["date"], errors="coerce")
pos["symbol"] = pos["symbol"].astype(str)
px["date"] = pd.to_datetime(px["date"], errors="coerce")
px["symbol"] = px["symbol"].astype(str)
m = pos.merge(px, on=["date", "symbol"], how="left")
miss = m[m["close"].isna()]
print("missing_price_rows=", len(miss))
sys.exit(1 if len(miss) != 0 else 0)
'@

$checkSummary = @'
import json
import os
import sys
import pandas as pd

run_dir = sys.argv[1]
summary_path = os.path.join(run_dir, "eligibility_summary.json")
drops_path = os.path.join(run_dir, "eligibility_drops.csv")

summary = json.load(open(summary_path, "r", encoding="utf-8"))
items = summary.get("per_rebalance") or summary.get("stats") or summary.get("per_exec_date") or []
if not items:
    print("summary missing per-rebalance list")
    sys.exit(1)

def normalize_bucket(bucket):
    if not isinstance(bucket, dict):
        return {}
    out = {}
    for k, v in bucket.items():
        try:
            iv = int(v)
        except Exception:
            iv = 0
        if iv != 0:
            out[k] = iv
    return out

bad = 0
for it in items:
    dropped = it.get("dropped", None)
    bucket = it.get("dropped_by_reason", None)
    if not isinstance(bucket, dict) or dropped is None:
        bad += 1
        continue
    if sum(int(v) for v in bucket.values()) != int(dropped):
        bad += 1
if bad:
    print("bad_entries=", bad)
    sys.exit(1)

total = summary.get("dropped_by_reason_total", None)
if not isinstance(total, dict):
    print("missing dropped_by_reason_total")
    sys.exit(1)

agg = {}
for it in items:
    bucket = it.get("dropped_by_reason") or {}
    for k, v in bucket.items():
        agg[k] = agg.get(k, 0) + int(v)
total_norm = normalize_bucket(total)
agg_norm = normalize_bucket(agg)
if agg_norm != total_norm:
    print("dropped_by_reason_total mismatch", agg_norm, total_norm)
    sys.exit(1)

drops = pd.read_csv(drops_path)
if drops.empty:
    drops = drops.assign(exec_date=pd.Series([], dtype=str), reason=pd.Series([], dtype=str))
else:
    drops["exec_date"] = drops["exec_date"].astype(str)
    drops["reason"] = drops["reason"].astype(str)

for it in items:
    ed = str(it.get("exec_date"))
    expected = int(it.get("dropped", 0))
    actual = int((drops["exec_date"] == ed).sum())
    if actual != expected:
        print("drops count mismatch", ed, expected, actual)
        sys.exit(1)
    bucket = it.get("dropped_by_reason") or {}
    actual_bucket = {}
    if not drops.empty:
        g = drops[drops["exec_date"] == ed].groupby("reason").size()
        actual_bucket = {k: int(v) for k, v in g.items()}
    expected_bucket = normalize_bucket(bucket)
    actual_bucket = normalize_bucket(actual_bucket)
    if actual_bucket != expected_bucket:
        print("drops bucket mismatch", ed, expected_bucket, actual_bucket)
        sys.exit(1)

print("per_rebalance_entries=", len(items), "bad_entries=", bad)
sys.exit(0)
'@

$checkFinite = @'
import json
import os
import sys
import math
import pandas as pd

run_dir = sys.argv[1]
nav_path = os.path.join(run_dir, "nav_clean.csv")
metrics_path = os.path.join(run_dir, "metrics.json")

nav = pd.read_csv(nav_path)
cols = ["nav_gross", "nav_net", "ret_gross", "ret_net"]
for c in cols:
    if c not in nav.columns:
        print("nav_clean missing column", c)
        sys.exit(1)
    s = pd.to_numeric(nav[c], errors="coerce")
    if not s.map(lambda x: math.isfinite(x) if x is not None else False).all():
        print("nav_clean nonfinite", c)
        sys.exit(1)

metrics = json.load(open(metrics_path, "r", encoding="utf-8"))
for key in ["metrics", "metrics_gross"]:
    block = metrics.get(key, {})
    for k in ["CAGR", "total_return", "Sharpe", "MaxDD"]:
        v = block.get(k, None)
        if v is None or not math.isfinite(float(v)):
            print("metrics nonfinite", key, k, v)
            sys.exit(1)

print("nav_clean and metrics finite")
sys.exit(0)
'@

Invoke-PythonCheck -Code $checkMissingPrice -Args @('-', $RunDirFull, $PricesDailyPathFull) `
    -FailCode 'BT_PHASE2_MISSING_PRICE' -FailMessage 'missing_price_rows is not zero.'

Invoke-PythonCheck -Code $checkSummary -Args @('-', $RunDirFull) `
    -FailCode 'BT_PHASE2_SUMMARY' -FailMessage 'eligibility_summary consistency check failed.'

Invoke-PythonCheck -Code $checkFinite -Args @('-', $RunDirFull) `
    -FailCode 'BT_PHASE2_FINITE' -FailMessage 'nav_clean or metrics contains non-finite values.'

if ($script:Failed) {
    Write-Host "[FAIL] BT Phase2 verification failed." -ForegroundColor Red
    exit 1
}

Write-Host "[PASS] BT Phase2 verification passed." -ForegroundColor Green
exit 0
