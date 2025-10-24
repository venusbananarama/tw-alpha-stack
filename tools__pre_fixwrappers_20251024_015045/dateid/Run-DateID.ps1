[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$Date,     # yyyy-MM-dd（請加引號）
  [Parameter(Mandatory=$true)][string]$IDs,      # 逗號或空白分隔："2330,2317"
  [ValidateSet("prices","chip","dividend","per","all")]
  [string]$Datasets = "all",
  [string]$Root = ".",
  [string]$DataHubRoot = "datahub",
  [switch]$NoLog
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# 固定路徑
$ROOT = (Resolve-Path $Root).Path
$PY   = Join-Path $ROOT ".venv\Scripts\python.exe"
if (-not (Test-Path $PY)) { throw "未找到 $PY，請先建立 venv。" }

# 解鎖環境（代號六口徑）
$env:ALPHACITY_ALLOW = '1'
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue

# 1) 嚴格解析日期（--end 不含 → +1 天）
try {
  $start   = [datetime]::ParseExact($Date, 'yyyy-MM-dd', [Globalization.CultureInfo]::InvariantCulture)
} catch {
  throw "無效的 -Date：$Date（請用 'yyyy-MM-dd'，例：'2025-10-06'）"
}
$end     = $start.AddDays(1)
$startStr= $start.ToString('yyyy-MM-dd')
$endStr  = $end.ToString('yyyy-MM-dd')

# 2) 解析 IDs
$syms = @()
$IDs -split '[,\s;]+' | ForEach-Object { if ($_ -and $_.Trim().Length -gt 0) { $syms += $_.Trim() } }
if ($syms.Count -eq 0) { throw "請提供至少一個 ID（例：-IDs 2330,2317）" }
$symbolsArg = ($syms -join ',')

# 3) datasets 映射
$map = @{
  'prices'   = 'TaiwanStockPrice'
  'chip'     = 'TaiwanStockInstitutionalInvestorsBuySell'
  'dividend' = 'TaiwanStockDividend'
  'per'      = 'TaiwanStockPER'
}
$runList = if ($Datasets -eq 'all') { @('prices','chip','dividend','per') } else { @($Datasets) }

# 4) 日誌
$logFile = Join-Path $ROOT ("reports\dateid_{0}_{1}.log" -f $start.ToString('yyyyMMdd'), ($symbolsArg -replace ',', '-'))
$logDir  = Split-Path -Parent $logFile
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Force -Path $logDir | Out-Null }
if (-not $NoLog) { New-Item -ItemType File -Force -Path $logFile | Out-Null }

function Invoke-Backfill([string]$key) {
  $table = $map[$key]

  # prices 走「全市場不帶 symbols」＝最穩
  $baseArgs = @(
    (Join-Path $ROOT "scripts\finmind_backfill.py"),
    "--datasets", $table,
    "--start",    $startStr,
    "--end",      $endStr,            # --end 不含 → 已 +1 天
    "--datahub-root", $DataHubRoot
  )

  $args = $baseArgs.Clone()
  $label = ""

  if ($key -eq 'prices') {
    $label = "PRICES :: ALL-MARKET :: $startStr→$endStr (end不含)"
    # 不加 --symbols，交由底層全市場處理
  } else {
    $label = "$($key.ToUpper()) :: $($syms -join ',') :: $startStr→$endStr (end不含)"
    $args += @("--symbols", $symbolsArg)
  }

  Write-Host ">>> 回填 $label"
  if ($NoLog) { & $PY $args } else { & $PY $args 1>> $logFile 2>&1 }
  if ($LASTEXITCODE -ne 0) { throw "Backfill failed: $key（exit $LASTEXITCODE）" }
}

foreach ($k in $runList) { Invoke-Backfill $k }

Write-Host "Run-DateID 完成：$($runList -join ', ')；IDs=$symbolsArg；Date=$startStr（--end=$endStr 不含）"
if (-not $NoLog) { Write-Host "日誌：$logFile" }
