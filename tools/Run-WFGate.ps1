[CmdletBinding()]
param(
  [string]$File,
  [string]$Dir,
  [switch]$Summary,
  [string]$Export
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
# gptcodex: alpha → tools/Run-WFGate.ps1
$ErrorActionPreference = "Stop"
$ROOT = Resolve-Path (Join-Path $PSScriptRoot "..") | Select-Object -Expand Path
Set-Location $ROOT

# 1) 預檢（印 SSOT 雜湊）
if (Test-Path ".\tools\Run-SmokeTests.ps1") {
  pwsh -NoProfile -ExecutionPolicy Bypass -File ".\tools\Run-SmokeTests.ps1"
} else {
  Write-Host "[WARN] tools\Run-SmokeTests.ps1 不存在，略過預檢"
}

# 2) 選擇 Python
$py = ".\.venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }

# 3) 組合參數並執行
$argsList = @("-X","utf8",".\scripts\wf_runner.py")
if ($File) { $argsList += @("--file",$File) }
if ($Dir)  { $argsList += @("--dir",$Dir) }
if ($Summary.IsPresent) { $argsList += "--summary" }
if ($Export) { $argsList += @("--export",$Export) }

& $py @argsList
exit $LASTEXITCODE


