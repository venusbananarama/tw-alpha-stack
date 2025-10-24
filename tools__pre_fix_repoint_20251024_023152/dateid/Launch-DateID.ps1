# tools/Launch-DateID.ps1
# 作用：在「當前工作階段」自動設 Process-scope Bypass + Unblock，再轉呼叫 Run-DateID.ps1
# 用法範例：
# pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Launch-DateID.ps1 -Date '2025-10-06' -IDs '2330,2317,2303' -Datasets all
# 或（已在當前視窗）：
# .\tools\Launch-DateID.ps1 -Date '2025-10-06' -IDs '2330,2317,2303' -Datasets all

[CmdletBinding(SupportsShouldProcess=$true)]
param(
  [Parameter(Mandatory=$true)][string]$Date,
  [Parameter(Mandatory=$true)][string]$IDs,
  [ValidateSet('prices','chip','dividend','per','all')][string]$Datasets='all',
  [string]$Root='.',
  [string]$DataHubRoot='datahub',
  [switch]$NoLog
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# 1) 當前程序層級允許腳本
try { Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force } catch { }

# 2) 解鎖檔案（若有 MOTW）
$rd = Join-Path $PSScriptRoot 'Run-DateID.ps1'
if (Test-Path $rd) { try { Unblock-File $rd } catch { } }

# 3) 轉呼叫 Run-DateID.ps1（參數直接傳遞）
& $rd -Date $Date -IDs $IDs -Datasets $Datasets -Root $Root -DataHubRoot $DataHubRoot -NoLog:$NoLog
