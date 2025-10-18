#requires -Version 7.0
param([switch]$Once)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

Write-Host "[Smoke] Start" -ForegroundColor Green
$ROOT = Split-Path -Parent $PSScriptRoot
Set-Location $ROOT

# 基本檔案存在性
$must = @(
  '.\tools\Backfill-FullMarket.ps1',
  '.\scripts\finmind_backfill.py'
)
foreach($p in $must){
  if (-not (Test-Path $p)) { throw "Missing required file: $p" }
}

# Python 可執行（盡量顯示版本，不視為致命）
$py = '.\.venv\Scripts\python.exe'
if (-not (Test-Path $py)) { $py = 'python' }
try { & $py -V } catch { Write-Warning "Python not available: $($_.Exception.Message)" }

Write-Host "[Smoke] OK" -ForegroundColor Green
