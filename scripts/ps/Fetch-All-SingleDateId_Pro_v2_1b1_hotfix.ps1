<#
.SYNOPSIS
  v2.1b1 hotfix：修正 Invoke-FMAll.ps1 的 -Datasets ValidateSet 限制（僅允許 prices/chip/stock_info）。
  會將「整體市場」資料集自動映射到群組後，逐一以 Invoke-FMAll.ps1 執行。
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

  [double]$MaxRps = 0.16,

  [string]$ApiToken,

  [switch]$UseThreadJob,

  [string]$ResumeLog = 'G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$agg = [Math]::Round($Workers * $Qps, 3)
if ($agg -gt $MaxRps) { throw ("總速率超過限制：Workers×Qps={0} > MaxRps={1}" -f $agg, $MaxRps) }

$InvokeSingle = Join-Path $PSScriptRoot 'Invoke-FMSingle.ps1'
$InvokeAll    = Join-Path $PSScriptRoot 'Invoke-FMAll.ps1'
if (-not (Test-Path $InvokeSingle)) { throw "Missing script: $InvokeSingle" }
if (-not (Test-Path $InvokeAll))    { throw "Missing script: $InvokeAll（需要此檔以跑整體市場資料）" }

$effectiveToken = if ($ApiToken) { $ApiToken } else { $env:FINMIND_TOKEN }
if (-not $effectiveToken) { throw '找不到 API token，請以 -ApiToken 指定或設 FINMIND_TOKEN。' }

# 分流：單股 vs. 整體市場（並映射群組）
$MarketMap = @{
  'TaiwanStockTotalInstitutionalInvestors' = 'chip';
  'TaiwanStockTotalMarginPurchaseShortSale' = 'chip';
  'TaiwanStockMarketTradingInfo' = 'stock_info';
  'TaiwanStockTAIEX' = 'stock_info';
  'TaiwanStockOTCIndex' = 'stock_info';
}
$ds_market = @()
$ds_single = @()
foreach ($d in $Datasets) {
  if ($MarketMap.ContainsKey($d)) { $ds_market += $d } else { $ds_single += $d }
}

# 載入/清理 symbols
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

# === 先處理整體市場 ===
if ($ds_market.Count -gt 0) {
  $groups = $ds_market | ForEach-Object { $MarketMap[$_] } | Select-Object -Unique
  Write-Host "[STAGE] Market-wide groups → $($groups -join ',')"
  $env:FINMIND_TOKEN = $effectiveToken
  foreach ($g in $groups) {
    & $InvokeAll -Datasets $g -Start $Start -End $End -Workers 1 -Qps ([Math]::Min($Qps, $MaxRps)) -VerboseCmd
  }
  Write-Host "[STAGE] Market-wide done"
}

# === 再跑單股 ===
if ($ds_single.Count -eq 0) { Write-Host "[DONE] 無單股資料集"; return }

[int]$submitted=0; [int]$skipped=0; [int]$okCount=0; [int]$failCount=0
New-Item -ItemType Directory -Force -Path (Split-Path $ResumeLog) | Out-Null
$done=@{}; if (Test-Path $ResumeLog) {
  Get-Content $ResumeLog | % { $p=$_.Split('|'); if ($p.Length -ge 2 -and $p[1]-eq'ok'){$done[$p[0]]=$true} }
}
$startTime=Get-Date; $lastProgress=Get-Date

$jobScript = {
  param($sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$ApiToken)
  if ($ApiToken) { $env:FINMIND_TOKEN = $ApiToken }
  & $InvokeSingle -Symbol $sym -Datasets $Datasets -Start $Start -End $End -Qps $Qps -VerboseCmd | Out-Null
  Add-Content -Path $ResumeLog -Value ("{0}|ok" -f $sym)
}

function Show-Progress { param($submitted,$skipped,$okCount,$failCount,$total,$startTime)
  $done = $okCount + $failCount
  $elapsed = (Get-Date) - $startTime
  $rate = if ($done -gt 0){ $done / [Math]::Max($elapsed.TotalSeconds,1)} else {0}
  $remain = $total - $done - $skipped
  $etaSec = if ($rate -gt 0){ [int]($remain / $rate)} else {0}
  $pct = if ($total -gt 0){ [int](($done + $skipped) * 100 / $total)} else {0}
  Write-Host ("[PROGRESS] {0}% done | ok={1} fail={2} skip={3} submitted={4} | eta≈{5}s" -f `
    $pct, $okCount, $failCount, $skipped, $submitted, $etaSec)
}

$jobs=@()
$engine = $(if ($UseThreadJob) { 'thread' } else { 'process' })
Write-Host ("[INFO] symbols={0} datasets_single={1} start={2} end={3} workers={4} qps={5} total_rps={6} engine={7}" -f `
  $symbols.Count, $ds_single.Count, $Start, $End, $Workers, $Qps, $agg, $engine)

foreach ($sym in $symbols) {
  if ($done.ContainsKey($sym)) { $skipped++; continue }
  while ($jobs.Count -ge $Workers) {
    $finished = $jobs | ? { $_.State -in 'Completed','Failed','Stopped' }
    foreach ($j in $finished) {
      if ($j.State -eq 'Completed') { $okCount++ } else { $failCount++ }
      $jobs = $jobs | ? { $_.Id -ne $j.Id }
    }
    if (((Get-Date)-$lastProgress).TotalSeconds -ge 2) { Show-Progress $submitted $skipped $okCount $failCount $symbols.Count $startTime; $lastProgress=Get-Date }
    Start-Sleep -Milliseconds 200
  }
  if ($UseThreadJob) {
    $jobs += Start-ThreadJob -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobScript -ArgumentList $sym,$ds_single,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$effectiveToken
  } else {
    $jobs += Start-Job -Name ("Fetch_{0}" -f $sym) -ScriptBlock $jobScript -ArgumentList $sym,$ds_single,$Start,$End,$Qps,$ResumeLog,$InvokeSingle,$effectiveToken
  }
  $submitted++
}

while ($jobs.Count -gt 0) {
  $finished = $jobs | ? { $_.State -in 'Completed','Failed','Stopped' }
  foreach ($j in $finished) {
    if ($j.State -eq 'Completed') { $okCount++ } else { $failCount++ }
    $jobs = $jobs | ? { $_.Id -ne $j.Id }
  }
  if (((Get-Date)-$lastProgress).TotalSeconds -ge 2) { Show-Progress $submitted $skipped $okCount $failCount $symbols.Count $startTime; $lastProgress=Get-Date }
  Start-Sleep -Milliseconds 300
}

Write-Host ("[DONE] ok={0} fail={1} skip={2} submitted={3} | log={4}" -f $okCount,$failCount,$skipped,$submitted,$ResumeLog)
