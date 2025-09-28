<#
.SYNOPSIS
  Fetch-All-SingleDateId v2.2a — Simplified Runner (hotfix)
  - 修正：Sequential 模式報「Using variable cannot be retrieved」錯誤。
  - 方式：不再在子 ScriptBlock 使用 $using:；改以參數傳入 InvokeSingle 的路徑。
  - 仍保留：ForEach-Object -Parallel、速率保護、市場級 Shim、dataset 級打點/重試、resume log、per-symbol log。
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

  [int]$ThrottleLimit = 4,
  [double]$QpsPerWorker = 0.04,
  [double]$MaxRps = 0.16,

  [string]$ApiToken,
  [switch]$ForceResumeReset,
  [int]$MaxSymbols = 0,
  [switch]$Sequential,

  [string]$ResumeLog = 'G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log',
  [string]$LogsDir   = 'G:\AI\tw-alpha-stack\metrics\single_logs'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# 速率保護
$agg = [Math]::Round($ThrottleLimit * $QpsPerWorker, 3)
if ($agg -gt $MaxRps) { throw ("總速率超過限制：ThrottleLimit×QpsPerWorker={0} > MaxRps={1}" -f $agg, $MaxRps) }

# 依存腳本
$InvokeSinglePath = Join-Path $PSScriptRoot 'Invoke-FMSingle.ps1'
$FMAllShim        = Join-Path $PSScriptRoot 'Invoke-FMAll_Shim.ps1'
if (-not (Test-Path $InvokeSinglePath)) { throw "Missing script: $InvokeSinglePath" }
if (-not (Test-Path $FMAllShim))        { throw "Missing script: $FMAllShim（請使用打包附帶的 Shim）" }

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

# 符號正規化 + 載入
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

# 準備
if ($ForceResumeReset) { Remove-Item $ResumeLog -ErrorAction SilentlyContinue }
New-Item -ItemType Directory -Force -Path (Split-Path $ResumeLog) | Out-Null
New-Item -ItemType Directory -Force -Path $LogsDir | Out-Null

$symbols = Load-Symbols -UniverseCsv $UniverseCsv -SymbolsTxt $SymbolsTxt
if ($MaxSymbols -gt 0) { $symbols = $symbols | Select-Object -First $MaxSymbols }

# 拆分資料集
$ds_market = @()
$ds_single = @()
foreach ($d in $Datasets) { if ($MarketMap.ContainsKey($d)) { $ds_market += $d } else { $ds_single += $d } }

# 市場級先跑
if ($ds_market.Count -gt 0) {
  $groups = $ds_market | ForEach-Object { $MarketMap[$_] } | Select-Object -Unique
  Write-Host "[STAGE] Market-wide groups → $($groups -join ',')"
  foreach ($g in $groups) { & $FMAllShim -Group $g -Start $Start -End $End -Qps $QpsPerWorker -ApiToken $effectiveToken }
  Write-Host "[STAGE] Market-wide done"
}

if ($ds_single.Count -eq 0) { Write-Host "[DONE] 無單股資料集"; return }

# 斷點續跑：讀歷史完成
$done=@{}; if (Test-Path $ResumeLog) {
  Get-Content $ResumeLog | ForEach-Object {
    $p=$_.Split('|'); if ($p.Length -ge 2 -and $p[1]-eq'ok'){ $done[$p[0]]=$true }
  }
}

# 定義每檔工作內容（逐 dataset）
$perSymbol = {
  param($sym,$dsSingle,$Start,$End,$QpsPerWorker,$ResumeLog,$effectiveToken,$LogsDir,$InvokeSinglePath)

  $log = Join-Path $LogsDir ("single_{0}.log" -f $sym)
  "=== {0} {1} → {2} | Datasets={3} ===" -f $sym,$Start,$End,($dsSingle -join ',') | Tee-Object -FilePath $log

  if ($effectiveToken) { $env:FINMIND_TOKEN = $effectiveToken }

  $hadFail = $false
  foreach ($ds in $dsSingle) {
    $attempt=0
    $dsStart = Get-Date
    "[DATASET] {0} | {1} | start {2:HH:mm:ss}" -f $sym,$ds,$dsStart | Tee-Object -FilePath $log -Append
    while ($true) {
      try {
        $attempt++
        & $InvokeSinglePath -Symbol $sym -Datasets @($ds) -Start $Start -End $End -Qps $QpsPerWorker -VerboseCmd `
          *>&1 | Tee-Object -FilePath $log -Append
        $dsEnd = Get-Date
        $dur = [int](($dsEnd - $dsStart).TotalSeconds)
        "[OK] {0} | {1} | {2}s" -f $sym,$ds,$dur | Tee-Object -FilePath $log -Append
        Add-Content -Path $ResumeLog -Value ("{0}|{1}|ok" -f $sym,$ds)
        break
      } catch {
        $msg = $_.Exception.Message
        Add-Content -Path $log -Value ("[WARN] {0} | {1} | attempt={2} | {3}" -f $sym,$ds,$attempt,$msg)
        if ($attempt -ge 3) {
          Add-Content -Path $ResumeLog -Value ("{0}|{1}|fail|{2}" -f $sym,$ds,$msg)
          "[FAIL] {0} | {1} | {2}" -f $sym,$ds,$msg | Tee-Object -FilePath $log -Append
          $hadFail = $true
          break
        }
        Start-Sleep -Seconds ([int]([math]::Pow(2, $attempt-1) * 2))
      }
    }
  }

  if (-not $hadFail) { Add-Content -Path $ResumeLog -Value ("{0}|ok" -f $sym) }
  else { Add-Content -Path $ResumeLog -Value ("{0}|partial_fail" -f $sym) }
}

$startTime = Get-Date
Write-Host ("[INFO] symbols={0} datasets_single={1} start={2} end={3} throttle={4} qps/worker={5} total_rps={6} mode={7}" -f `
  $symbols.Count, $ds_single.Count, $Start, $End, $ThrottleLimit, $QpsPerWorker, $agg, $(if ($Sequential) {'sequential'} else {'parallel'}))

if ($Sequential) {
  foreach ($sym in $symbols) {
    if ($done.ContainsKey($sym)) { continue }
    & $perSymbol $sym $ds_single $Start $End $QpsPerWorker $ResumeLog $effectiveToken $LogsDir $InvokeSinglePath
  }
} else {
  $symbolsToRun = $symbols | Where-Object { -not $done.ContainsKey($_) }
  $symbolsToRun | ForEach-Object -Parallel $perSymbol -ThrottleLimit $ThrottleLimit -ArgumentList @(
    $ds_single,$Start,$End,$QpsPerWorker,$ResumeLog,$effectiveToken,$LogsDir,$InvokeSinglePath
  )
}

$elapsed = (Get-Date) - $startTime
Write-Host ("[DONE] elapsed={0:c} | log={1}" -f $elapsed, $ResumeLog)
