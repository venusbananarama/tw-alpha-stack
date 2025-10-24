#requires -Version 7.0
Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent
Set-Location $root

$env:ALPHACITY_ALLOW = "1"
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue
if (Test-Path "$PSScriptRoot\Set-AlphaCity-Env.ps1") { . "$PSScriptRoot\Set-AlphaCity-Env.ps1" }

$py = Join-Path $root ".venv\Scripts\python.exe"

# Prefer your existing preflight runner if present
$pref = Join-Path $PSScriptRoot "Run-Preflight-V2.ps1"
if (Test-Path $pref) {
  & pwsh -NoProfile -ExecutionPolicy Bypass -File $pref
} else {
  & $py -S "scripts\preflight_v2_wrapper.py" --rules "rules.yaml" --export "reports" --root "."
}

# Rebuild universe with failsafe
& $py -S "scripts\build_universe_failsafe.py" --rules "rules.yaml" --out "configs\investable_universe.txt" --root "."
exit 0
