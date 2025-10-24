param(
  [string]$Root = ".",
  [switch]$Strict
)
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
Set-Location $Root

# --- 路徑與環境 ---
$cfgPath = "configs\data_sources.yaml"
if (-not (Test-Path $cfgPath)) { throw "Missing $cfgPath" }
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
if (-not (Test-Path "metrics")) { New-Item -ItemType Directory -Force -Path "metrics" | Out-Null }
if (-not (Test-Path "reports\registry")) { New-Item -ItemType Directory -Force -Path "reports\registry" | Out-Null }

function Get-PythonExe {
  try { return (Get-Command python -ErrorAction Stop).Source } catch {
    $cand1 = ".\.venv\Scripts\python.exe"
    $cand2 = "C:\AI\.venv\ac311-alpha\Scripts\python.exe"
    if (Test-Path $cand1) { return $cand1 }
    if (Test-Path $cand2) { return $cand2 }
    throw "Cannot find python. Add to PATH or ensure venv exists."
  }
}
$pyExe = Get-PythonExe

# --- Run registry（環境與設定留痕） ---
$pyCmd = "import sys, json, importlib; " +
         "def ver(m):" +
         " " + " " + " " + " " + " " + " " + " " + " " +
         "  " + " " + " " + " " + " " + " " + " " + " " +
         "  " + "import importlib; " +
         "  " + " " + " " + " " + " " + " " + " " + " " +
         "  " + " " + " " + " " + " " + " " + " " + " " +
         "  " + " " + " " + " " + " " + " " + " " + " " +
         "  " + " " + " " + " " + " " + " " + " " + " " +
         "  " + " " + " " + " " + " " + " " + " " + " " +
         "  " + " " + " " + " " + " " + " " + " " + " " +
         "  " + "try: return importlib.import_module(m).__version__ " +
         "  except Exception: return None; " +
         "print(json.dumps({'python':sys.version,'pandas':ver('pandas'), 'pyarrow':ver('pyarrow'), 'duckdb':ver('duckdb')}, ensure_ascii=False))"

$pyver = & $pyExe -c $pyCmd
$dsYaml = Get-Content $cfgPath -Raw -Encoding UTF8
$runJson = Join-Path "reports\registry" "run_$stamp.json"
$runObj = @{
  stage = "phase1"
  created_at = (Get-Date).ToString("s")
  env = (ConvertFrom-Json $pyver)
  data_sources_yaml = $dsYaml
}
$runObj | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $runJson -Encoding UTF8
Write-Host "✔ Run registry: $runJson"

# --- 檢查任務（都以報告為主，不故意中斷） ---
& $pyExe scripts\validate_silver.py --config $cfgPath --report-csv metrics\silver_check_latest.csv
& $pyExe scripts\checks\verify_weekly_anchor.py --config $cfgPath --out metrics\weekly_anchor_report.csv
& $pyExe scripts\checks\check_asof_weekly.py --config $cfgPath --out metrics\asof_weekly_violations.csv
& $pyExe scripts\build_universe.py --rules configs\universe.rules.local.yaml --config $cfgPath --out configs\investable_universe.txt

# --- 匯總清單 ---
$manifest = @{
  stamp = $stamp
  outputs = @{
    silver_check = "metrics/silver_check_latest.csv"
    weekly_anchor = "metrics/weekly_anchor_report.csv"
    asof = "metrics/asof_weekly_violations.csv"
    universe = "configs/investable_universe.txt"
    run_registry = $runJson
  }
}
$manifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath "metrics\phase1_manifest_latest.json" -Encoding UTF8
Write-Host "✔ Manifest: metrics\phase1_manifest_latest.json"
Write-Host "Phase 1 validation completed."
