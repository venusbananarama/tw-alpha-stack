<#
.SYNOPSIS
  批量抓取「需要 date_id」的單股 API（全市場股票）。v2.1b：
  - 自動限制總速率（Workers × Qps ≤ MaxRps，預設 0.16 ≈ 600/hr）
  - 自動辨識「整體市場」資料集並拆跑（改用 Invoke-FMAll.ps1，只跑一次）
  - 自動規範 symbol：移除 .TW/.TWO 等後綴與非數字，僅保留數字代碼
  - 支援 -ApiToken（注入每個 Job 環境），-UseThreadJob（避免環境隔離）
  - 斷點續跑、重試/退避、進度/ETA、失敗率告警

.EXAMPLE
  .\scripts\ps\Fetch-All-SingleDateId_Pro_v2_1b.ps1 `
    -UniverseCsv 'G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv' `
    -ApiToken '<TOKEN>' -UseThreadJob `
    -Start '2015-01-01' -End ((Get-Date).ToString('yyyy-MM-dd')) `
    -Workers 4 -Qps 0.03   # 確保 Workers×Qps ≤ 0.16
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
    'TaiwanStockGovernmentBankBuySell',
    'TaiwanStockPER',
    'TaiwanStockPBR'
  ),

  [string]$Start = '2015-01-01',
  [string]$End   = ((Get-Date).ToString('yyyy-MM-dd')),

  [ValidateRange(1,64)]
  [int]$Workers = 6,

  [ValidateRange(0.001,10.0)]
  [double]$Qps = 0.03,

  [double]$MaxRps = 0.16,  # FinMind 600/hr

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

# 依據 FinMind 規則：總請求速率不得超過 600/hr ≈ 0.166 req/s
$agg = [Math]::Round($Workers * $Qps, 3)
if ($agg -gt $MaxRps) {
  throw ("總速率超過限制：Workers×Qps={0} > MaxRps={1}。請降低併發或 Qps。" -f $agg, $MaxRps)
}

# 路徑
$InvokeSingle = Join-Path $PSScriptRoot 'Invoke-FMSingle.ps1'
$InvokeAll    = Join-Path $PSScriptRoot 'Invoke-FMAll.ps1'
if (-not (Test-Path $InvokeSingle)) { throw "Missing script: $InvokeSingle" }
if (-not (Test-Path $InvokeAll))    { Write-Warning "找不到 $InvokeAll，若有整體市場資料集將無法自動拆跑。" }

# Token：參數優先，其次環境變數
$effectiveToken = if ($ApiToken) { $ApiToken } else { $env:FINMIND_TOKEN }
if (-not $effectiveToken) {
  throw '找不到 API token。請以 -ApiToken "<token>" 指定，或設定環境變數 FINMIND_TOKEN 後重試。'
}

# 分辨「整體市場」資料集（只需跑一次，不跟著每檔股票）
$MarketDatasets = @(
  'TaiwanStockTotalInstitutionalInvestors',
  'TaiwanStockTotalMarginPurchaseShortSale',
  'TaiwanStockMarketTradingInfo',
  'TaiwanStockTAIEX',
  'TaiwanStockOTCIndex'
)
$ds_market = @()
$ds_single = @()
foreach ($d in $Datasets) {
  if ($MarketDatasets -contains $d) { $ds_market += $d } else { $ds_single += $d }
}

# 載入 symbols，並做正規化
function Clean-Symbol([string]$s) {
  if (-not $s) { return $null }
  $t = $s -replace '\.TW(O)?$', ''           # 移除 .TW / .TWO
  $t = $t -replace '^[A-Za-z]+:', ''         # 移除 TSE: 之類前綴
  $t = ($t -replace '[^0-9]', '')            # 只留數字
  if ($t.Length -lt 3 -or $t.Length -gt 6) { return $null }
  return $t
}

function Load-Symbols {
  param([string]$UniverseCsv, [string]$SymbolsTxt)
  $raw = @()
  if ($UniverseCsv) {
    if (-not (Test-Path $UniverseCsv)) { throw "UniverseCsv not found: $UniverseCsv" }
    try {
      $csv = Import-Csv -Path $UniverseCsv
      $col = @('symbol','stock_id','id') | Where-Object { $_ -in $csv[0].psobject.Properties.Name }
      if (-not $col -or $col.Count -eq 0) { throw "CSV 缺少 symbol/stock_id 欄位" }
      $raw = $csv | ForEach-Object { $_.$($col[0]) }
    } catch {
      throw "讀取 UniverseCsv 失敗：$UniverseCsv，錯誤：$($_.Exception.Message)"
    }
  } else {
    if (-not (Test-Path $SymbolsTxt)) { throw "SymbolsTxt not found: $SymbolsTxt" }
    $raw = Get-Content -Path $SymbolsTxt
  }
  $syms = @()
  foreach ($r in $raw) {
    $c = Clean-Symbol $r
    if ($c) { $syms += $c }
  }
  $syms = $syms | Select-Object -Unique
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
Write-Host ("[INFO] symbols={0} datasets_single={1} datasets_market={2} start={3} end={4} workers={5} qps={6} total_rps={7} engine={8}" -f `
  $symbols.Count, $ds_single.Count, $ds_market.Count, $Start, $End, $Workers, $Qps, $agg, $engine)

# 先跑整體市場資料集（如有）
if ($ds_market.Count -gt 0) {
  if (-not (Test-Path $InvokeAll)) {
    throw "需要 Invoke-FMAll.ps1 才能處理整體市場資料集，請補齊後重試。"
  }
  Write-Host ("[STAGE] Market-wide datasets → {0}" -f ($ds_market -join ','))
  $env:FINMIND_TOKEN = $effectiveToken
  & $InvokeAll -Datasets $ds_market -Start $Start -End $End -Workers 1 -Qps ([Math]::Min($Qps, $MaxRps)) -VerboseCmd
  Write-Host "[STAGE] Market-wide done"
}

# 若沒有任何單股資料集，直接結束
if ($ds_single.Count -eq 0) {
  Write-Host "[DONE] 沒有需要逐檔跑的單股資料集。"
  return
}

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
      -ArgumentList $sym,$ds_single,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds,$effectiveToken
  } else {
    $jobs += Start-Job -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobScript `
      -ArgumentList $sym,$ds_single,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$MaxRetries,$BackoffSeconds,$effectiveToken
  }

  $submitted++

  $doneNow = $okCount + $failCount
  if ($doneNow -gt 20) {
    $failRate = $failCount / $doneNow
    if ($failRate -gt $FailRateAbort) {
      Write-Warning ("Fail rate {0:P1} > threshold {1:P1}. Aborting. 請降低 Qps 或 Workers 後重跑。" -f $failRate, $FailRateAbort)
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
