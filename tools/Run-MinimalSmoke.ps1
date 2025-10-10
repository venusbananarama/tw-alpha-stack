Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
param(
  [string]$DataRoot='datahub',
  [string]$Rules   = '.\rules.yaml'
)
$ErrorActionPreference='Stop'
$env:ALPHACITY_ALLOW='1'
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue

# Resolve Python
$Py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $Py)) {
  if (Get-Command py -ErrorAction SilentlyContinue) { $Py = 'py'; $PY_ARGS=@('-3.11') }
  elseif (Get-Command python -ErrorAction SilentlyContinue) { $Py = (Get-Command python).Path; $PY_ARGS=@() }
  else { throw "Python not found" }
} else { $PY_ARGS=@() }

# Guard DataRoot
$dr = (Resolve-Path $DataRoot).Path
if ($dr -match 'silver\\alpha$') { throw "DataRoot錯：請傳 'datahub'，非 'datahub\\silver\\alpha'" }

# 指紋
pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Context-Stamp.ps1 -DataRoot $DataRoot -Rules $Rules | Write-Host

# Universe（--out）
& $Py @PY_ARGS 'scripts/build_universe.py' --rules $Rules --out '.\configs\investable_universe.txt'
$cnt = (Get-Content .\configs\investable_universe.txt | Measure-Object -Line).Lines
Write-Host "[UNIVERSE] lines=" $cnt

# Preflight（不觸 API）
& $Py @PY_ARGS 'scripts/preflight_check.py' --rules $Rules --export 'reports'
Get-Content .\reports\preflight_report.json -TotalCount 120

