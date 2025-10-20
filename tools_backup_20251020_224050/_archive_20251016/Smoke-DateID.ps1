#requires -Version 7
[CmdletBinding()]
param(
  [string]$Date = (Get-Date).AddDays(-1).ToString('yyyy-MM-dd'),
  [string]$IDs  = '2330,2317',
  [ValidateSet('A','B','All')][string]$Group='A',
  [string]$Root = (Split-Path -Parent $PSScriptRoot)
)
$ErrorActionPreference='Stop'
$env:ALPHACITY_ALLOW='1'; Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue | Out-Null
$run = Join-Path $Root 'tools\Run-DateID-Extras.ps1'
Write-Host "Smoke-DateID :: $Date :: $IDs :: Group=$Group"
# --end 不含 → 測試時用同一天（script 內 +1d）
& pwsh -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $run -Start $Date -End $Date -IDs $IDs -Group $Group
if ($LASTEXITCODE -ne 0) { throw 'Smoke-DateID 失敗（exit=）' }
