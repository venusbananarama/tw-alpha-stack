Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# tools/Run-Daily.ps1  — Phase1 Orchestrator (SSOT=.\configs\rules.yaml)
$ErrorActionPreference = 'Stop'

# 路徑與 SSOT
$Tools   = Split-Path -Parent $PSCommandPath
$Root    = Split-Path -Parent $Tools
$Reports = Join-Path $Root 'reports'
$Rules   = Join-Path $Root 'configs\rules.yaml'
New-Item -ItemType Directory -Force -Path $Reports | Out-Null
Set-Location $Root

# Python 解析（含 blocked 回復與系統回退）
$Py        = Join-Path $Root '.venv\Scripts\python.exe'
$PyBlocked = Join-Path $Root '.venv\Scripts\python.blocked.exe'
if (!(Test-Path $Py) -and (Test-Path $PyBlocked)) { Rename-Item $PyBlocked $Py -Force }
if (!(Test-Path $Py)) {
  if (Get-Command py -ErrorAction SilentlyContinue)      { $Py = 'py';      $global:PY_ARGS = @('-3.11') }
  elseif (Get-Command python -ErrorAction SilentlyContinue){ $Py = (Get-Command python).Path; $global:PY_ARGS = @() }
  else { throw 'No Python found' }
} else { $global:PY_ARGS = @() }

function RunPy([string[]]$Args) {
  $old = $env:ALPHACITY_ALLOW; $env:ALPHACITY_ALLOW = '1'   # 解鎖 killswitch
  & $Py @($global:PY_ARGS) @Args
  $code = $LASTEXITCODE
  $env:ALPHACITY_ALLOW = $old
  if ($code -notin 0,2) { throw ("Exit {0}: {1}" -f $code, ($Args -join ' ')) }
  return $code
}

Write-Host '[S0] Preflight'
RunPy @('scripts/preflight_check.py','--rules',$Rules,'--export','reports') | Out-Null

Write-Host '[S1] Backfill'
$today = Get-Date -Format 'yyyy-MM-dd'
RunPy @('scripts/finmind_backfill.py','--start','2018-01-01','--end',$today,'--datasets',
  'TaiwanStockPrice','TaiwanStockInstitutionalInvestorsBuySell','TaiwanStockDividend','TaiwanStockPER',
  '--workers','6','--qps','1.6','--sentinel-safe') | Out-Null

Write-Host '[S2] Universe'
RunPy @('scripts/build_universe.py','--drop-empty','--rules',$Rules) | Out-Null

if ($env:SKIP_SMOKE -eq '1') {
  Write-Host '[S3] Smoke skipped'
} else {
  Write-Host '[S3] Smoke'
  $Smoke = if (Test-Path (Join-Path $Tools 'Run-SmokeTests.ps1')) { Join-Path $Tools 'Run-SmokeTests.ps1' } else { Join-Path $Tools 'Run-SmokeTests.ps1.new' }
  # 以獨立進程執行並 120s 超時，避免裸 python 進 REPL
  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = (Get-Command pwsh).Source
  $psi.Arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$Smoke`""
  $psi.WorkingDirectory = $Root
  $psi.UseShellExecute = $false
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $psi.Environment['ALPHACITY_ALLOW'] = '1'
  $psi.Environment['PATH'] = (Join-Path $Root '.venv\Scripts') + ';' + $env:PATH
  $p = New-Object System.Diagnostics.Process; $p.StartInfo = $psi; [void]$p.Start()
  if (-not $p.WaitForExit(120000)) { try { $p.Kill($true) } catch {}; $code = 2 } else { $code = $p.ExitCode }
  if ($code -notin 0,2) { throw "Run-SmokeTests failed ($code)" }
}

Write-Host '[S4] WF Gate'
RunPy @('scripts/wf_runner.py','--summary','--export','reports') | Out-Null

Write-Host '[S5] Reports'
$Emit = Join-Path $Root 'scripts\emit_reports.py'
if (Test-Path $Emit) { RunPy @('scripts/emit_reports.py','--rules',$Rules,'--out','reports') | Out-Null }

Write-Host '[Run-Daily] Done.'
exit 0

