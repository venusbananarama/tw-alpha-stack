#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
Set-Location $root

$env:ALPHACITY_ALLOW = "1"
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue
if (Test-Path "$PSScriptRoot\Set-AlphaCity-Env.ps1") { . "$PSScriptRoot\Set-AlphaCity-Env.ps1" }

$orchestrator = Join-Path $PSScriptRoot "Run-DailyBackfill.ps1"
if (Test-Path $orchestrator) {
  & pwsh -NoProfile -ExecutionPolicy Bypass -File $orchestrator -Phase chip
  exit $LASTEXITCODE
}

$py = Join-Path $root ".venv\Scripts\python.exe"
# If your project supports chip-specific script, prefer it; else generic backfill
$chipScript = Join-Path $root "scripts\finmind_backfill.py"
& $py -S $chipScript
exit $LASTEXITCODE
