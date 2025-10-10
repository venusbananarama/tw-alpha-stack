Param(
  [string]$RulesPath = ".\rules.yaml"
)
$env:ALPHACITY_ALLOW = "1"
if (-not (Test-Path $RulesPath)) { Write-Error "rules.yaml not found: $RulesPath"; exit 1 }
$PY = ".\.venv\Scripts\python.exe"
& $PY .\scripts\preflight_check.py --rules $RulesPath --export reports
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
if (-not (Test-Path .\reports)) { New-Item -ItemType Directory -Force -Path .\reports | Out-Null }
Write-Host "[OK] Preflight done. See .\reports"
