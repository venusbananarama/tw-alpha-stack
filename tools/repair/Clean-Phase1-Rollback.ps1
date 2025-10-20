<#
.SYNOPSIS
  AlphaCity Phase-1 一鍵回滾/清理腳本（安全預設：Dry-Run）
.DESCRIPTION
  依據 metrics\phase1_manifest_latest.json 或參數，清理 _phase1_validation 回測輸出、暫存報表與日誌。
#>
param(
  [string]$Root = 'G:/AI/tw-alpha-stack',
  [switch]$Deep,          # 連帶刪除 manifest 指到的報表/日誌
  [switch]$Apply,         # 實際執行；預設 Dry-Run
  [string]$Since = ''     # 另可指定日期 'YYYY-MM-DD'，會刪除該日起之後建立的 _phase1_validation 子資料夾
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Do-Remove {
  param([string]$Path)
  if (-not (Test-Path $Path)) { return }
  if ($Apply) {
    Remove-Item $Path -Recurse -Force -ErrorAction SilentlyContinue
    Write-Host "DEL $Path" -ForegroundColor Red
  } else {
    Write-Host "[Dry-Run] DEL $Path" -ForegroundColor Yellow
  }
}

$manPath = Join-Path $Root 'metrics/phase1_manifest_latest.json'
$targets = @()

# 1) 主要回測輸出：_phase1_validation/*
$btRoot = 'G:/AI/datahub/alpha/backtests/_phase1_validation'
if (Test-Path $btRoot) {
  if ($Since) {
    try { $sinceDt = [datetime]::ParseExact($Since,'yyyy-MM-dd',$null) } catch { throw "Since 需為 yyyy-MM-dd" }
    Get-ChildItem -Directory $btRoot | Where-Object { $_.CreationTime -ge $sinceDt } | ForEach-Object { $targets += $_.FullName }
  } else {
    $targets += (Get-ChildItem -Directory $btRoot | ForEach-Object { $_.FullName })
  }
}

# 2) 從 manifest 讀取
if (Test-Path $manPath) {
  $man = Get-Content $manPath -Raw | ConvertFrom-Json
  if ($man.outputs) {
    $targets += @($man.outputs | Where-Object { $_ -match '_phase1_validation' })
    if ($Deep) {
      $targets += @($man.outputs | Where-Object { $_ -notmatch '_phase1_validation' })  # 可能包含報表根目錄；不建議除非 Deep
    }
  }
  if ($Deep -and $man.logs) { $targets += @($man.logs) }
} else {
  Write-Host "找不到 manifest：$manPath（將只清 _phase1_validation）" -ForegroundColor Yellow
}

# 3) 其它安全可清路徑
$extra = @(
  (Join-Path $Root '_smoketest'),
  (Join-Path $Root 'logs/layout_check.log'),
  (Join-Path $Root 'metrics/fetch_single_dateid_resume.log'),
  (Join-Path $Root 'metrics/fetch_single_latest.log'),
  (Join-Path $Root 'metrics/fetch_backfill_latest.log'),
  'G:/AI/tw-alpha-stack/reports/tmp'
)
$targets += $extra

# 去重、存在檢查
$targets = $targets | Where-Object { $_ -and (Test-Path $_) } | Sort-Object -Unique

# 執行
if (-not $targets) { Write-Host "沒有可清理的目標。" -ForegroundColor Green; exit 0 }
Write-Host "=== 一鍵回滾/清理（$(if($Apply){'Apply'}else{'Dry-Run'})） ===" -ForegroundColor Cyan
$targets | ForEach-Object { Do-Remove $_ }
Write-Host "完成。提示：加上 -Apply 參數才會真的刪除。" -ForegroundColor Green
