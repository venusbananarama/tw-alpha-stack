Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# gptcodex: alpha → tools/Run-SmokeTests.ps1
param()

$ErrorActionPreference = "Stop"
$ROOT = Resolve-Path (Join-Path $PSScriptRoot "..") | Select-Object -Expand Path
Set-Location $ROOT

# 印出 SSOT rules.yaml 雜湊並執行 Preflight
$r = Join-Path $ROOT "configs\rules.yaml"
if (-not (Test-Path $r)) { throw "configs\rules.yaml not found" }
$hash = (Get-FileHash -Path $r -Algorithm SHA256).Hash.Substring(0,12)
Write-Host ("[SSOT] rules.yaml hash={0}" -f $hash)

# 允許 .venv 或系統 Python
$py = Join-Path $ROOT ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }
& $py -X utf8 ".\scripts\preflight_check.py"

Write-Host "[SMOKE] Preflight OK. 可接續你的 smoke 測試流程。"

