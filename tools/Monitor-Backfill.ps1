[CmdletBinding()]
param(
  [string]$RootPath = "C:\AI\tw-alpha-stack",
  [int]$IntervalSec = 30,
  [int]$TailLines   = 120,
  [switch]$RunPreflight,
  [string]$Rules = "",
  [switch]$Once
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$showVerbose = $PSBoundParameters.ContainsKey('Verbose')

function _Echo($m){ Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $m) }

if (-not (Test-Path -LiteralPath $RootPath)) { throw "RootPath not found: $RootPath" }
$Reports = Join-Path $RootPath 'reports'
New-Item -ItemType Directory -Force -Path $Reports | Out-Null

$PY = Join-Path $RootPath '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $PY)) {
  Write-Warning "Python not found: $PY；將無法執行 preflight_check.py"
  $RunPreflight = $false
}

if (-not $Rules -or -not (Test-Path -LiteralPath $Rules)) {
  $eff = Join-Path $Reports 'rules.effective.yaml'
  $Rules = if (Test-Path -LiteralPath $eff) { $eff } else { Join-Path $RootPath 'rules.yaml' }
}

function Invoke-Preflight {
  try {
    & $PY (Join-Path $RootPath 'scripts\preflight_check.py') --rules $Rules --export $Reports --root $RootPath *>$null
  } catch {
    Write-Warning ("preflight_check.py 失敗：{0}" -f $_.Exception.Message)
  }
}

function Read-Preflight {
  try { return Get-Content (Join-Path $Reports 'preflight_report.json') -Raw -Encoding UTF8 | ConvertFrom-Json }
  catch { return $null }
}

function Show-Snapshot {
  if ($RunPreflight) { Invoke-Preflight }
  $p = Read-Preflight
  $now = Get-Date -Format 's'
  if (-not $p) { Write-Host "[$now] 尚無 preflight_report.json（等待產生）" -ForegroundColor Yellow; return }
  $exp = if ($p.meta -and $p.meta.expect_date) { $p.meta.expect_date } elseif ($p.expect_date) { $p.expect_date } else { $null }
  Write-Host ("[{0}] expect_date={1}" -f $now,$exp)
  $rows = @()
  foreach($k in @('prices','chip','per','dividend')){
    $md = $null
    if ($p.freshness.$k -and $p.freshness.$k.max_date) { $md = [datetime]$p.freshness.$k.max_date }
    $lag = if ($exp -and $md) { ([datetime]$exp - $md).Days } else { $null }
    $rows += [pscustomobject]@{ dataset=$k; max_date=if($md){$md.ToString('yyyy-MM-dd')}else{'-'}; lag_days=$lag }
  }
  $rows | Format-Table -Auto
}

function Get-LatestLog {
  # Search priority: run_backfill_all -> backfill_worker -> nightly
  $candidates = @(
    Get-ChildItem -LiteralPath $Reports -Filter 'run_backfill_all_*.log' -ErrorAction SilentlyContinue
    Get-ChildItem -LiteralPath $Reports -Filter 'backfill_worker_*.log'   -ErrorAction SilentlyContinue
    Get-ChildItem -LiteralPath $Reports -Filter 'nightly_*.log'           -ErrorAction SilentlyContinue
  ) | Where-Object { $_ } | Sort-Object LastWriteTime -Descending
  if ($candidates){ return $candidates[0].FullName } else { return $null }
}

function Print-Tail([int]$Lines){
  $log = Get-LatestLog
  if (-not $log) { Write-Host "尚無可讀取的 log 檔於 $Reports" -ForegroundColor Yellow; return }
  Write-Host ("--- tail {0} (last {1} lines) ---" -f (Split-Path $log -Leaf), $Lines) -ForegroundColor Cyan
  try { Get-Content -LiteralPath $log -Tail $Lines | ForEach-Object { $_ } } catch { Write-Warning $_.Exception.Message }
}

Show-Snapshot
Print-Tail -Lines $TailLines
if ($Once) { return }

while ($true) {
  Start-Sleep -Seconds $IntervalSec
  Show-Snapshot
  Print-Tail -Lines $TailLines
}
