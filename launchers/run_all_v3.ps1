param(
  [string]$OhlcvDir = "G:\AI\datahub\ohlcv_daily",
  [string]$MergedPath = "G:\AI\datahub\ohlcv_daily_all.parquet",
  [string]$BoardCsv = "G:\AI\datahub\metadata\symbol_board.csv",
  [string]$ReportXlsx = "G:\AI\datahub\reports\market_all_in_one.xlsx",
  [string]$DetailSample = "2330.TW,2317.TW,1101.TW",
  [int]$TopN = 100,
  [switch]$WithCharts = $false,
  [switch]$CleanReports = $false
)

Write-Host "[INFO] run_all_v3 starting..."

$python = "$env:VIRTUAL_ENV\Scripts\python.exe"
if (-not (Test-Path $python)) { $python = "python" }

if ($CleanReports) {
  Write-Host "[INFO] Cleaning reports folder: " (Split-Path $ReportXlsx)
  try {
    Remove-Item -Force -Recurse (Join-Path (Split-Path $ReportXlsx) "*") -ErrorAction SilentlyContinue
  } catch {}
}

if (-not (Test-Path $BoardCsv)) {
  Write-Host "[INFO] Building board mapping → $BoardCsv"
  & $python ".\ingest\build_symbol_board.py" --from-parquet "$MergedPath" --out "$BoardCsv"
}

$chartsFlag = ""
if ($WithCharts) { $chartsFlag = "--with-charts" }

Write-Host "[INFO] Generating market report..."
& $python ".\ingest\market_report_all_in_one.py" --file "$MergedPath" --board-csv "$BoardCsv" --detail-sample "$DetailSample" --topn $TopN $chartsFlag --out "$ReportXlsx"

$reportDir = Split-Path $ReportXlsx
Write-Host "[INFO] Computing alpha factors..."
& $python ".\ingest\alpha_factors.py" --file "$MergedPath" --out-root "$reportDir" --topn $TopN

Write-Host "[INFO] run_all_v3 done."