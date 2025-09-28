<#
.SYNOPSIS
  批量抓取「需要 date_id」的單股 API（全市場股票）。支援：自訂資料集、斷點續跑、重試/退避、併發、QPS 控制、進度/ETA、失敗率告警。

.DESCRIPTION
  - 從 investable_universe.csv 或 symbols.txt 載入股票清單（需包含 symbol）。
  - 以 Invoke-FMSingle.ps1 對每個股票抓取多個需要 date_id 的資料集。
  - 具備斷點續跑（ResumeLog），重跑會跳過已成功的股票。
  - 每項任務內含 retry/backoff，並在主行程輸出進度、完成統計與 ETA。
  - 若失敗率超過 FailRateAbort（預設 5%），會提前結束並提示降低 QPS 或 Workers。

.PARAMETER UniverseCsv
  股票池清單（CSV），需包含 'symbol' 欄。與 SymbolsTxt 擇一必填。

.PARAMETER SymbolsTxt
  純文字代碼清單（每行一個 symbol）。與 UniverseCsv 擇一必填。

.PARAMETER Datasets
  要抓取的資料集陣列（字串）。未指定時使用預設需 date_id 清單。

.PARAMETER Start
  起始日期（yyyy-MM-dd）。

.PARAMETER End
  結束日期（yyyy-MM-dd）。

.PARAMETER Workers
  併發工作數量（1..64）。

.PARAMETER Qps
  每秒請求上限（傳遞給 Invoke-FMSingle.ps1）。

.PARAMETER ResumeLog
  斷點續跑記錄檔路徑。格式：<symbol>|ok 或 <symbol>|fail|<message>

.PARAMETER MaxRetries
  每個 symbol 的最大重試次數（含首次執行，預設 3）。

.PARAMETER BackoffSeconds
  第一次重試等待秒數；之後每次 *2（指數退避）。

.PARAMETER FailRateAbort
  若 fail/(ok+fail) 超過此比例則終止（預設 0.05 = 5%）。

.PARAMETER ProgressInterval
  進度列更新間隔（秒）。

.EXAMPLE
  PS> .\scripts\ps\Fetch-All-SingleDateId_Pro.ps1 `
        -UniverseCsv 'G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv' `
        -Start '2015-01-01' -End ((Get-Date).ToString('yyyy-MM-dd')) `
        -Workers 6 -Qps 1.6 -MaxRetries 3 -BackoffSeconds 2
#>

[CmdletBinding()]
param(
  [Parameter(ParameterSetName='csv', Mandatory=$true)]
  [string]$UniverseCsv,

  [Parameter(ParameterSetName='txt', Mandatory=$true)]
  [string]$SymbolsTxt,

  [string[]]$Datasets = @(
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockShareholding",
    "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockTotalMarginPurchaseShortSale",
    "TaiwanStockGovernmentBankBuySell",
    "TaiwanStockPER",
    "TaiwanStockPBR"
  ),

  [string]$Start = "2015-01-01",
  [string]$End   = ((Get-Date).ToString("yyyy-MM-dd")),

  [ValidateRange(1,64)]
  [int]$Workers = 6,

  [ValidateRange(0.1,10.0)]
  [double]$Qps = 1.6,

  [string]$ResumeLog = "G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log",

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

# === 前置檢查 ===
$InvokeSingle = Join-Path $PSScriptRoot "Invoke-FMSingle.ps1"
if (-not (Test-Path $InvokeSingle)) {
  throw "Missing script: $InvokeSingle（請確認與本檔同在 scripts\ps\ 資料夾）"
}

# 載入 symbols
function Load-Symbols {
  param([string]$UniverseCsv, [string]$SymbolsTxt)
  if ($UniverseCsv) {
    if (-not (Test-Path $UniverseCsv)) { throw "UniverseCsv not found: $UniverseCsv" }
    try {
      $syms = Import-Csv -Path $UniverseCsv | ForEach-Object { $_.symbol }
    } catch { throw "讀取 UniverseCsv 失敗：$UniverseCsv，錯誤：$($_.Exception.Message)" }
  } else {
    if (-not (Test-Path $SymbolsTxt)) { throw "SymbolsTxt not found: $SymbolsTxt" }
    $syms = Get-Content -Path $SymbolsTxt
  }
  $syms = $syms | Where-Object { $_ -and $_.Trim() -ne '' } | Select-Object -Unique
  if (-not $syms -or $syms.Count -eq 0) { throw "沒有有效的 symbol 可用。" }
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

# 統計
[int]$submitted = 0
[int]$skipped   = 0
[int]$okCount   = 0
[int]$failCount = 0

Write-Host ("[INFO] symbols={0} datasets={1} start={2} end={3} workers={4} qps={5}" -f `
  $symbols.Count, $Datasets.Count, $Start, $End, $Workers, $Qps)

# === 內部：工作腳本（含 retry） ===
$jobScript = {
  param($sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds)

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

function Show-Progress {
  param($submitted,$skipped,$okCount,$failCount,$total)

  $done = $okCount + $failCount
  if ($done -gt 0) {
    $elapsed = (Get-Date) - $startTime
    $rate = $done / [Math]::Max($elapsed.TotalSeconds,1)
    $remain = $total - $done - $skipped
    $etaSec = if ($rate -gt 0) { [int]($remain / $rate) } else { 0 }
    $pct = [int](($done + $skipped) * 100 / $total)
    Write-Host ("[PROGRESS] {0}% done | ok={1} fail={2} skip={3} submitted={4} | eta≈{5}s" -f `
      $pct, $okCount, $failCount, $skipped, $submitted, $etaSec)
  } else {
    Write-Host "[PROGRESS] started..."
  }
}

$lastProgress = Get-Date

foreach ($sym in $symbols) {
  if ($done.ContainsKey($sym)) { $skipped++; continue }

  # throttle on workers
  while ($jobs.Count -ge $Workers) {
    $finished = $jobs | Where-Object { $_.State -in 'Completed','Failed','Stopped' }
    foreach ($j in $finished) {
      if ($j.State -eq 'Completed') { $okCount++ } else { $failCount++ }
      $jobs = $jobs | Where-Object { $_.Id -ne $j.Id }
    }

    # progress tick
    if (((Get-Date) - $lastProgress).TotalSeconds -ge $ProgressInterval) {
      Show-Progress -submitted $submitted -skipped $skipped -okCount $okCount -failCount $failCount -total $symbols.Count
      $lastProgress = Get-Date
    }

    Start-Sleep -Milliseconds 200
  }

  $jobs += Start-Job -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobScript `
    -ArgumentList $sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds
  $submitted++

  # 失敗率檢查（只在有完成量時進行）
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
