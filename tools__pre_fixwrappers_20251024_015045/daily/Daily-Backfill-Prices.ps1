#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
Set-Location $root

# Setup env (if helper exists)
$env:ALPHACITY_ALLOW = "1"
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue
if (Test-Path "$PSScriptRoot\Set-AlphaCity-Env.ps1") { . "$PSScriptRoot\Set-AlphaCity-Env.ps1" }

# Prefer existing orchestrator if available (keeps your project semantics intact)
$orchestrator = Join-Path $PSScriptRoot "Run-DailyBackfill.ps1"
if (Test-Path $orchestrator) {
  & pwsh -NoProfile -ExecutionPolicy Bypass -File $orchestrator -Phase prices
  exit $LASTEXITCODE
}

# Minimal fallback: call your Python finmind backfill (no args -> project defaults)
$py = Join-Path $root ".venv\Scripts\python.exe"
& $py -S "scripts\finmind_backfill.py"
exit $LASTEXITCODE
