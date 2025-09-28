<#
.SYNOPSIS
  v2.1b3 hotfix：改用自帶的 Invoke-FMAll_Shim.ps1 處理「整體市場」資料集，不再依賴既有 Invoke-FMAll.ps1。
  - 保留速率保護（Workers×Qps ≤ 0.16）
  - 市場級資料集自動分流（chip/stock_info）
  - symbol 正規化與單股批次
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
  [int]$Workers = 4,

  [ValidateRange(0.001,10.0)]
  [double]$Qps = 0.03,

  [double]$MaxRps = 0.16,  # 600/hr

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

# 速率保護
$agg = [Math]::Round($Workers * $Qps, 3)
if ($agg -gt $MaxRps) {
  throw ("總速率超過限制：Workers×Qps={0} > MaxRps={1}" -f $agg, $MaxRps)
}

$InvokeSingle = Join-Path $PSScriptRoot 'Invoke-FMSingle.ps1'
$FMAllShim    = Join-Path $PSScriptRoot 'Invoke-FMAll_Shim.ps1'
if (-not (Test-Path $InvokeSingle)) { throw "Missing script: $InvokeSingle" }
if (-not (Test-Path $FMAllShim))    { throw "Missing script: $FMAllShim（熱修版需要此檔）" }

# Token
$effectiveToken = if ($ApiToken) { $ApiToken } else { $env:FINMIND_TOKEN }
if (-not $effectiveToken) { throw '找不到 API token，請以 -ApiToken 指定或設 FINMIND_TOKEN。' }

# 市場級映射 → 群組
$MarketMap = @{
  'TaiwanStockTotalInstitutionalInvestors'  = 'chip';
  'TaiwanStockTotalMarginPurchaseShortSale' = 'chip';
  'TaiwanStockMarketTradingInfo'            = 'stock_info';
  'TaiwanStockTAIEX'                        = 'stock_info';
  'TaiwanStockOTCIndex'                     = 'stock_info';
}

$ds_market = @()
$ds_single = @()
foreach ($d in $Datasets) {
  if ($MarketMap.ContainsKey($d)) { $ds_market += $d } else { $ds_single += $d }
}

# 符號正規化
function Clean-Symbol([string]$s) {
  if (-not $s) { return $null }
  $t = $s -replace '\.TW(O)?$', ''
  $t = $t -replace '^[A-Za-z]+:', ''
  $t = ($t -replace '[^0-9]', '')
  if ($t.Length -lt 3 -or $t.Length -gt 6) { return $null }
  return $t
}
function Load-Symbols {
  param([string]$UniverseCsv, [string]$SymbolsTxt)
  $raw = @()
  if ($UniverseCsv) {
    if (-not (Test-Path $UniverseCsv)) { throw "UniverseCsv not found: $UniverseCsv" }
    $csv = Import-Csv -Path $UniverseCsv
    $col = @('symbol','stock_id','id') | Where-Object { $_ -in $csv[0].psobject.Properties.Name }
    if (-not $col -or $col.Count -eq 0) { throw "CSV 缺少 symbol/stock_id 欄位" }
    $raw = $csv | ForEach-Object { $_.$($col[0]) }
  } else {
    if (-not (Test-Path $SymbolsTxt)) { throw "SymbolsTxt not found: $SymbolsTxt" }
    $raw = Get-Content -Path $SymbolsTxt
  }
  $syms = @()
  foreach ($r in $raw) { $c = Clean-Symbol $r; if ($c) { $syms += $c } }
  $syms = $syms | Select-Object -Unique
  if (-not $syms -or $syms.Count -eq 0) { throw '沒有有效的 symbol 可用。' }
  return ,$syms
}
$symbols = Load-Symbols -UniverseCsv $UniverseCsv -SymbolsTxt $SymbolsTxt

# === 市場級先跑（用 shim；每個群組只跑一次） ===
if ($ds_market.Count -gt 0) {
  $groups = $ds_market | ForEach-Object { $MarketMap[$_] } | Select-Object -Unique
  Write-Host "[STAGE] Market-wide groups → $($groups -join ',')"
  foreach ($g in $groups) {
    & $FMAllShim -Group $g -Start $Start -End $End -Qps ([Math]::Min($Qps, $MaxRps)) -ApiToken $effectiveToken
  }
  Write-Host "[STAGE] Market-wide done"
}

if ($ds_single.Count -eq 0) { Write-Host "[DONE] 無單股資料集"; return }

# 斷點續跑
New-Item -ItemType Directory -Force -Path (Split-Path $ResumeLog) | Out-Null
$done=@{}; if (Test-Path $ResumeLog) {
  Get-Content $ResumeLog | % { $p=$_.Split('|'); if ($p.Length -ge 2 -and $p[1]-eq'ok'){$done[$p[0]]=$true} }
}

[int]$submitted=0; [int]$skipped=0; [int]$okCount=0; [int]$failCount=0
$startTime=Get-Date; $lastProgress=Get-Date
$engine = $(if ($UseThreadJob) { 'thread' } else { 'process' })
Write-Host ("[INFO] symbols={0} datasets_single={1} start={2} end={3} workers={4} qps={5} total_rps={6} engine={7}" -f `
  $symbols.Count, $ds_single.Count, $Start, $End, $Workers, $Qps, $agg, $engine)

$jobScript = {
  param($sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$ApiToken,$MaxRetries,$BackoffSeconds)
  if ($ApiToken) { $env:FINMIND_TOKEN = $ApiToken }
  $attempt=0
  while ($true) {
    try {
      $attempt++
      & $InvokeSingle -Symbol $sym -Datasets $Datasets -Start $Start -End $End -Qps $Qps -VerboseCmd | Out-Null
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

$jobs=@()
foreach ($sym in $symbols) {
  if ($done.ContainsKey($sym)) { $skipped++; continue }
  while ($jobs.Count -ge $Workers) {
    $finished = $jobs | Where-Object { $_.State -in 'Completed','Failed','Stopped' }
    foreach ($j in $finished) {
      if ($j.State -eq 'Completed') { $okCount++ } else { $failCount++ }
      $jobs = $jobs | Where-Object { $_.Id -ne $j.Id }
    }
    if (((Get-Date)-$lastProgress).TotalSeconds -ge $ProgressInterval) {
      $doneN = $okCount + $failCount
      $elapsed = (Get-Date)-$startTime
      $rate = 0
      if ($doneN -gt 0) { $rate = $doneN / [Math]::Max($elapsed.TotalSeconds,1) }
      $remain = $symbols.Count - $doneN - $skipped
      $etaSec = 0
      if ($rate -gt 0) { $etaSec = [int]($remain / $rate) }
      $pct = [int](($doneN + $skipped) * 100 / $symbols.Count)
      Write-Host ("[PROGRESS] {0}% done | ok={1} fail={2} skip={3} submitted={4} | eta≈{5}s" -f `
        $pct, $okCount, $failCount, $skipped, $submitted, $etaSec)
      $lastProgress=Get-Date
    }
    Start-Sleep -Milliseconds 200
  }
  if ($UseThreadJob) {
    $jobs += Start-ThreadJob -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobScript -ArgumentList $sym,$ds_single,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$effectiveToken,$MaxRetries,$BackoffSeconds
  } else {
    $jobs += Start-Job -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobScript -ArgumentList $sym,$ds_single,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$effectiveToken,$MaxRetries,$BackoffSeconds
  }
  $submitted++
}

while ($jobs.Count -gt 0) {
  $finished = $jobs | Where-Object { $_.State -in 'Completed','Failed','Stopped' }
  foreach ($j in $finished) {
    if ($j.State -eq 'Completed') { $okCount++ } else { $failCount++ }
    $jobs = $jobs | Where-Object { $_.Id -ne $j.Id }
  }
  if (((Get-Date)-$lastProgress).TotalSeconds -ge $ProgressInterval) {
    $doneN = $okCount + $failCount
    $elapsed = (Get-Date)-$startTime
    $rate = 0
    if ($doneN -gt 0) { $rate = $doneN / [Math]::Max($elapsed.TotalSeconds,1) }
    $remain = $symbols.Count - $doneN - $skipped
    $etaSec = 0
    if ($rate -gt 0) { $etaSec = [int]($remain / $rate) }
    $pct = [int](($doneN + $skipped) * 100 / $symbols.Count)
    Write-Host ("[PROGRESS] {0}% done | ok={1} fail={2} skip={3} submitted={4} | eta≈{5}s" -f `
      $pct, $okCount, $failCount, $skipped, $submitted, $etaSec)
    $lastProgress=Get-Date
  }
  Start-Sleep -Milliseconds 300
}

Write-Host ("[DONE] ok={0} fail={1} skip={2} submitted={3} | log={4}" -f $okCount,$failCount,$skipped,$submitted,$ResumeLog)
