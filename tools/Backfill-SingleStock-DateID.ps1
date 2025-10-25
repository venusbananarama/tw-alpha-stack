#requires -Version 7
<#
  Backfill-SingleStock-DateID.ps1
  目的：不變動主線（preflight/build_universe/wf/gate），
        以 DateID 或日期範圍回補「單股」的 prices / chip。
        注意：--end 為不含終點，內部用 (End + 1) 餵主線。
#>

[CmdletBinding(PositionalBinding=$false, DefaultParameterSetName='ByDateId')]
param(
  # 目標股票（逗號或陣列，會自動補 .TW）
  [Parameter(Mandatory)][Alias('Symbol','Symbols')][string[]]$Ticker,

  # 時間視窗（二選一）
  [Parameter(Mandatory, ParameterSetName='ByDateId')][int]$StartId,
  [Parameter(Mandatory, ParameterSetName='ByDateId')][int]$EndId,

  [Parameter(Mandatory, ParameterSetName='ByDate')][datetime]$StartDate,
  [Parameter(Mandatory, ParameterSetName='ByDate')][datetime]$EndDate,

  # 資料集開關
  [switch]$DoPrices = $true,
  [switch]$DoChip   = $true,

  # 速率（以環境變數傳給 Python）
  [ValidateRange(0.10,100.0)][double]$Qps = 1.5,
  [ValidateRange(100,20000)][int]$HourlyCap = 6000,  # 保留參數，不往下傳

  # 路徑
  [string]$DataRoot = 'datahub'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# 0) 根目錄與 Python
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
Set-Location $root
if (-not $env:ALPHACITY_ALLOW) { $env:ALPHACITY_ALLOW = '1' }
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue | Out-Null
$PY = '.\.venv\Scripts\python.exe'
if (-not (Test-Path $PY)) { throw "找不到 $PY，請先建立虛擬環境並安裝需求套件。" }

# 1) 交易日曆（SSOT：cal\trading_days.csv）
$calPath = Join-Path $root 'cal\trading_days.csv'
if (-not (Test-Path $calPath)) { throw "缺少交易日曆 $calPath" }
$tradingDays = Import-Csv -LiteralPath $calPath | ForEach-Object {
  [datetime]::ParseExact($_.date, 'yyyy-MM-dd', $null)
} | Sort-Object
if (-not $tradingDays -or $tradingDays.Count -lt 10) { throw "交易日曆內容異常（筆數過少）。" }

# 1.1) 活日曆 clamp：只保留 ≤ 今天(台北) 的交易日，避免 DateID 指到未來
try { $todayTpe = [TimeZoneInfo]::ConvertTimeBySystemTimeZoneId([DateTime]::UtcNow, 'Taipei Standard Time').Date }
catch { $todayTpe = (Get-Date).AddHours(8).Date }  # 後援：UTC+8
$tradingDays = @($tradingDays | Where-Object { $_ -le $todayTpe })
if (-not $tradingDays) { throw "交易日曆不含 <= 今天 的日期；請檢查 cal\trading_days.csv" }

function Get-DateFromId([int]$id){
  if ($id -lt 1 -or $id -gt $tradingDays.Count) {
    throw "DateID 超界：$id（有效範圍 1..$($tradingDays.Count)）"
  }
  return $tradingDays[$id - 1]
}
function Normalize-DateToTrading([datetime]$dt){
  # 若傳入非交易日，取「不大於該日的最近一個交易日」
  $dt  = $dt.Date
  $idx = [Array]::BinarySearch([datetime[]]$tradingDays, $dt)
  if ($idx -ge 0) { return $tradingDays[$idx] }
  $ins = -bnot $idx
  if ($ins -le 0) { throw "日期早於最早交易日：$dt" }
  return $tradingDays[$ins - 1]
}

# 2) 視參數型態決定視窗
switch ($PSCmdlet.ParameterSetName) {
  'ByDateId' { $sDate = Get-DateFromId $StartId; $eDate = Get-DateFromId $EndId }
  'ByDate'   { $sDate = Normalize-DateToTrading $StartDate; $eDate = Normalize-DateToTrading $EndDate }
}
if ($eDate -lt $sDate) { throw "時間區間錯誤：End < Start（$eDate < $sDate）" }

# SSOT：--end 不含 → 內部 +1 天，讓使用者的 EndDate/EndId 含當天
$startStr = $sDate.ToString('yyyy-MM-dd')
$endStrEx = ($eDate.AddDays(1)).ToString('yyyy-MM-dd')

# 3) 準備 symbols 與 datasets
$symbols = foreach($t in $Ticker){ $x=$t.Trim(); if($x -notmatch '\.'){"$x.TW"} else {$x} }
$symbolsArg = ($symbols -join ',')

$datasets = @()
if ($DoPrices) { $datasets += 'TaiwanStockPrice' }
if ($DoChip)   { $datasets += 'TaiwanStockInstitutionalInvestorsBuySell' }
if (-not $datasets) { throw "未選任何資料集（請開啟 -DoPrices 或 -DoChip）" }

# 4) 以環境變數傳遞 QPS 給 Python（主線不吃 --qps/--workers）
$prevQps = $env:FINMIND_QPS
$env:FINMIND_QPS = ('{0}' -f $Qps)

Write-Host "[Backfill] symbols=$symbolsArg, window=$startStr → $endStrEx (end exclusive)"
try{
  foreach($ds in $datasets){
    Write-Host "  - dataset=$ds, qps=$($env:FINMIND_QPS)"
    & $PY .\scripts\finmind_backfill.py `
       --datasets $ds `
       --symbols  $symbolsArg `
       --start    $startStr `
       --end      $endStrEx `
       --datahub-root $DataRoot
  }
} finally {
  if($null -ne $prevQps){ $env:FINMIND_QPS = $prevQps } else { Remove-Item Env:FINMIND_QPS -EA SilentlyContinue }
}
Write-Host "Done."
