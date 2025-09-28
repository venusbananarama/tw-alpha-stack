<#
.SYNOPSIS
  批量抓取「需要 date_id」的單股 API（全市場股票）。v2.1a：
  - 支援 -ApiToken（自動注入子 Job 環境，免設環境變數）
  - 支援 -UseThreadJob（Start-ThreadJob 同進程，避免環境隔離造成 0% 卡住）
  - 修正 PowerShell 語法（移除 C# 三元運算子，改用 if/else）

.DESCRIPTION
  - 從 investable_universe.csv 或 symbols.txt 載入清單（需含 symbol 欄或每行一個代碼）。
  - 針對每個 symbol 逐一呼叫 Invoke-FMSingle.ps1，抓取需要 date_id 的資料集。
  - 內建斷點續跑（ResumeLog）、重試/指數退避、進度/ETA、失敗率告警（> FailRateAbort 提前中止）。

.PARAMETER UniverseCsv
  股票池 CSV，需含 'symbol' 欄。與 SymbolsTxt 擇一必填。

.PARAMETER SymbolsTxt
  純文字代碼清單（每行一個 symbol）。與 UniverseCsv 擇一必填。

.PARAMETER Datasets
  要抓取的資料集陣列（字串）。若未指定，預設為常用的需 date_id 清單。

.PARAMETER Start
  起始日期（yyyy-MM-dd）。

.PARAMETER End
  結束日期（yyyy-MM-dd）。

.PARAMETER Workers
  併發工作數量（1..64）。

.PARAMETER Qps
  每秒請求上限（傳遞給 Invoke-FMSingle.ps1）。

.PARAMETER ApiToken
  FinMind API token。若未提供，使用環境變數 $env:FINMIND_TOKEN。

.PARAMETER UseThreadJob
  改用 Start-ThreadJob（同進程）避免 Start-Job 的環境隔離問題。

.PARAMETER ResumeLog
  斷點續跑記錄檔路徑：<symbol>|ok 或 <symbol>|fail|<message>

.EXAMPLE
  .\scripts\ps\Fetch-All-SingleDateId_Pro_v2_1a.ps1 `
    -UniverseCsv 'G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv' `
    -ApiToken '<TOKEN>' -UseThreadJob `
    -Start '2015-01-01' -End ((Get-Date).ToString('yyyy-MM-dd')) `
    -Workers 4 -Qps 1.2
#>

[CmdletBinding()]
param(
  [Parameter(ParameterSetName='csv', Mandatory=$true)]
  [string]$UniverseCsv,

  [Parameter(ParameterSetName='txt', Mandatory=$true)]
  [string]$SymbolsTxt,

  [string[]]$Datasets = @(
    'TaiwanStockInstitutionalInvestorsBuySell',
    'TaiwanStockShareholding',
    'TaiwanStockMarginPurchaseShortSale',
    'TaiwanStockTotalMarginPurchaseShortSale',
    'TaiwanStockGovernmentBankBuySell',
    'TaiwanStockPER',
    'TaiwanStockPBR'
  ),

  [string]$Start = '2015-01-01',
  [string]$End   = ((Get-Date).ToString('yyyy-MM-dd')),

  [ValidateRange(1,64)]
  [int]$Workers = 6,

  [ValidateRange(0.1,10.0)]
  [double]$Qps = 1.6,

  [string]$ApiToken,

  [switch]$UseThreadJob,

  [string]$ResumeLog = 'G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log',

  [ValidateRange(1,10)]
  [int]$MaxRetries = 3,

  [ValidateRange(1,60)]
  [int]$BackoffSeconds = 2,

  [ValidateRange(0.0,1.0)]
  [double]$FailRateAbort = 0.05,

  [ValidateRange(1,60)]
  [int]$ProgressInterval = 2
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# --- 前置檢查 ---
$InvokeSingle = Join-Path $PSScriptRoot 'Invoke-FMSingle.ps1'
if (-not (Test-Path $InvokeSingle)) {
  throw "Missing script: $InvokeSingle（請確認與本檔同在 scripts\ps\ 資料夾）"
}

# Token：優先使用參數，其次環境變數
$effectiveToken = if ($ApiToken) { $ApiToken } else { $env:FINMIND_TOKEN }
if (-not $effectiveToken) {
  throw '找不到 API token。請以 -ApiToken "<token>" 指定，或設定環境變數 FINMIND_TOKEN 後重試。'
}

# 載入 symbols
function Load-Symbols {
  param([string]$UniverseCsv, [string]$SymbolsTxt)
  if ($UniverseCsv) {
    if (-not (Test-Path $UniverseCsv)) { throw "UniverseCsv not found: $UniverseCsv" }
    try { $syms = Import-Csv -Path $UniverseCsv | ForEach-Object { $_.symbol } }
    catch { throw "讀取 UniverseCsv 失敗：$UniverseCsv，錯誤：$($_.Exception.Message)" }
  } else {
    if (-not (Test-Path $SymbolsTxt)) { throw "SymbolsTxt not found: $SymbolsTxt" }
    $syms = Get-Content -Path $SymbolsTxt
  }
  $syms = $syms | Where-Object { $_ -and $_.Trim() -ne '' } | Select-Object -Unique
  if (-not $syms -or $syms.Count -eq 0) { throw '沒有有效的 symbol 可用。' }
  return ,$syms
}

$symbols = Load-Symbols -UniverseCsv $UniverseCsv -SymbolsTxt $SymbolsTxt

# 確保日誌資料夾存在
New-Item -ItemType Directory -Force -Path (Split-Path $ResumeLog) | Out-Null

# 斷點續跑：讀取已完成清單
$done = @{}
if (Test-Path $ResumeLog) {
  Get-Content $ResumeLog | ForEach-Object {
    $parts = $_.Split('|')
    if ($parts.Length -ge 2 -and $parts[1] -eq 'ok') { $done[$parts[0]] = $true }
  }
}

[int]$submitted = 0
[int]$skipped   = 0
[int]$okCount   = 0
[int]$failCount = 0

$engine = $(if ($UseThreadJob) { 'thread' } else { 'process' })
Write-Host ("[INFO] symbols={0} datasets={1} start={2} end={3} workers={4} qps={5} engine={6}" -f `
  $symbols.Count, $Datasets.Count, $Start, $End, $Workers, $Qps, $engine)

# === 內部：工作腳本（含 retry） ===
$jobScript = {
  param($sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds,$ApiToken)

  if ($ApiToken) { $env:FINMIND_TOKEN = $ApiToken }

  function Invoke-One {
    param($sym,$Datasets,$Start,$End,$Qps,$InvokeSingle)
    & $InvokeSingle -Symbol $sym -Datasets $Datasets -Start $Start -End $End -Qps $Qps -VerboseCmd | Out-Null
  }

  $attempt = 0
  while ($true) {
    try {
      $attempt++
      Invoke-One -sym $sym -Datasets $Datasets -Start $Start -End $End -Qps $Qps -InvokeSingle $InvokeSingle
      Add-Content -Path $ResumeLog -Value ("{0}|ok" -f $sym)
      break
    } catch {
      if ($attempt -ge $MaxRetries) {
        Add-Content -Path $ResumeLog -Value ("{0}|fail|{1}" -f $sym, $_.Exception.Message)
        throw
      }
      Start-Sleep -Seconds ([int]([math]::Pow(2, $attempt-1) * $BackoffSeconds))
    }
  }
}

# === 併發提交與進度 ===
$jobs = @()
$startTime = Get-Date
$lastProgress = Get-Date

function Show-Progress {
  param($submitted,$skipped,$okCount,$failCount,$total)
  $done = $okCount + $failCount
  $elapsed = (Get-Date) - $startTime
  $rate = if ($done -gt 0) { $done / [Math]::Max($elapsed.TotalSeconds,1) } else { 0 }
  $remain = $total - $done - $skipped
  $etaSec = if ($rate -gt 0) { [int]($remain / $rate) } else { 0 }
  $pct = if ($total -gt 0) { [int](($done + $skipped) * 100 / $total) } else { 0 }
  Write-Host ("[PROGRESS] {0}% done | ok={1} fail={2} skip={3} submitted={4} | eta≈{5}s" -f `
    $pct, $okCount, $failCount, $skipped, $submitted, $etaSec)
}

foreach ($sym in $symbols) {
  if ($done.ContainsKey($sym)) { $skipped++; continue }

  while ($jobs.Count -ge $Workers) {
    $finished = $jobs | Where-Object { $_.State -in 'Completed','Failed','Stopped' }
    foreach ($j in $finished) {
      if ($j.State -eq 'Completed') { $okCount++ } else { $failCount++ }
      $jobs = $jobs | Where-Object { $_.Id -ne $j.Id }
    }

    if (((Get-Date) - $lastProgress).TotalSeconds -ge $ProgressInterval) {
      Show-Progress -submitted $submitted -skipped $skipped -okCount $okCount -failCount $failCount -total $symbols.Count
      $lastProgress = Get-Date
    }
    Start-Sleep -Milliseconds 200
  }

  if ($UseThreadJob) {
    $jobs += Start-ThreadJob -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobScript `
      -ArgumentList $sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds,$effectiveToken
  } else {
    $jobs += Start-Job -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobScript `
      -ArgumentList $sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds,$effectiveToken
  }

  $submitted++

  $doneNow = $okCount + $failCount
  if ($doneNow -gt 20) {
    $failRate = $failCount / $doneNow
    if ($failRate -gt $FailRateAbort) {
      Write-Warning ("Fail rate {0:P1} > threshold {1:P1}. Aborting to protect rate limit. 請降低 QPS 或 Workers 後重跑。" -f $failRate, $FailRateAbort)
      break
    }
  }
}

# 等待所有工作結束
while ($jobs.Count -gt 0) {
  $finished = $jobs | Where-Object { $_.State -in 'Completed','Failed','Stopped' }
  foreach ($j in $finished) {
    if ($j.State -eq 'Completed') { $okCount++ } else { $failCount++ }
    $jobs = $jobs | Where-Object { $_.Id -ne $j.Id }
  }

  if (((Get-Date) - $lastProgress).TotalSeconds -ge $ProgressInterval) {
    Show-Progress -submitted $submitted -skipped $skipped -okCount $okCount -failCount $failCount -total $symbols.Count
    $lastProgress = Get-Date
  }

  Start-Sleep -Milliseconds 300
}

$elapsedTotal = (Get-Date) - $startTime
Write-Host ("[DONE] ok={0} fail={1} skip={2} submitted={3} | elapsed={4:c} | log={5}" -f `
  $okCount, $failCount, $skipped, $submitted, $elapsedTotal, $ResumeLog)
