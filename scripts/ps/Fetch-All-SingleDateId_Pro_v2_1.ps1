<#
.SYNOPSIS
  批量抓取「需要 date_id」的單股 API。v2.1：支援 -ApiToken（自動注入子 Job 環境）、-UseThreadJob（避免 Start-Job 環境隔離）。

.PARAMETER ApiToken
  明確指派 FinMind API token。若未提供，會使用 $env:FINMIND_TOKEN。

.PARAMETER UseThreadJob
  使用 Start-ThreadJob 取代 Start-Job（同進程執行，較穩定但對 CPU 綁定較緊）。

.Notes
  仍包含：自訂 Datasets、QPS/Workers、斷點續跑、重試/退避、進度/ETA、失敗率告警。
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
  [int]$ProgressInterval = 2,

  [string]$ApiToken,

  [switch]$UseThreadJob
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$InvokeSingle = Join-Path $PSScriptRoot "Invoke-FMSingle.ps1"
if (-not (Test-Path $InvokeSingle)) { throw "Missing script: $InvokeSingle" }

# --- Token preflight ---
$effectiveToken = if ($ApiToken) { $ApiToken } else { $env:FINMIND_TOKEN }
if (-not $effectiveToken) {
  throw "找不到 API token。請設定環境變數 FINMIND_TOKEN 或以 -ApiToken '<token>' 指定。"
}

# 載入 symbols
function Load-Symbols {
  param([string]$UniverseCsv, [string]$SymbolsTxt)
  $syms = @()
  if ($UniverseCsv) {
    if (-not (Test-Path $UniverseCsv)) { throw "UniverseCsv not found: $UniverseCsv" }
    $syms = Import-Csv -Path $UniverseCsv | ForEach-Object { $_.symbol }
  } else {
    if (-not (Test-Path $SymbolsTxt)) { throw "SymbolsTxt not found: $SymbolsTxt" }
    $syms = Get-Content -Path $SymbolsTxt
  }
  $syms = $syms | Where-Object { $_ -and $_.Trim() -ne '' } | Select-Object -Unique
  if (-not $syms -or $syms.Count -eq 0) { throw "沒有有效的 symbol 可用。" }
  return ,$syms
}

$symbols = Load-Symbols -UniverseCsv $UniverseCsv -SymbolsTxt $SymbolsTxt

New-Item -ItemType Directory -Force -Path (Split-Path $ResumeLog) | Out-Null

# 斷點續跑
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

Write-Host ("[INFO] symbols={0} datasets={1} start={2} end={3} workers={4} qps={5} engine={6}" -f `
  $symbols.Count, $Datasets.Count, $Start, $End, $Workers, $Qps, ($UseThreadJob?'thread':'process'))

# 共用工作邏輯
$jobBlock = {
  param($sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds,$ApiToken)
  if ($ApiToken) { $env:FINMIND_TOKEN = $ApiToken }

  function Invoke-One { param($sym,$Datasets,$Start,$End,$Qps,$InvokeSingle)
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

$jobs = @()
$startTime = Get-Date

function Show-Progress { param($submitted,$skipped,$okCount,$failCount,$total)
  $done = $okCount + $failCount
  $elapsed = (Get-Date) - $startTime
  $rate = if ($done -gt 0) { $done / [Math]::Max($elapsed.TotalSeconds,1) } else { 0 }
  $remain = $total - $done - $skipped
  $etaSec = if ($rate -gt 0) { [int]($remain / $rate) } else { 0 }
  $pct = if ($total -gt 0) { [int](($done + $skipped) * 100 / $total) } else { 0 }
  Write-Host ("[PROGRESS] {0}% done | ok={1} fail={2} skip={3} submitted={4} | eta≈{5}s" -f `
    $pct, $okCount, $failCount, $skipped, $submitted, $etaSec)
}

$lastProgress = Get-Date

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
    $jobs += Start-ThreadJob -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobBlock `
      -ArgumentList $sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds,$effectiveToken
  } else {
    $jobs += Start-Job -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobBlock `
      -ArgumentList $sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds,$effectiveToken
  }
  $submitted++

  $doneNow = $okCount + $failCount
  if ($doneNow -gt 20) {
    $failRate = $failCount / $doneNow
    if ($failRate -gt $FailRateAbort) {
      Write-Warning ("Fail rate {0:P1} > threshold {1:P1}. Aborting. 請降低 QPS 或 Workers 後重跑。" -f $failRate, $FailRateAbort)
      break
    }
  }
}

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
