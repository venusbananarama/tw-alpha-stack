param(
    [switch]$WithCharts,
    [switch]$CleanReports,
    [string]$OhlcvDir = "G:\AI\datahub\ohlcv_daily",
    [string]$MergedPath = "G:\AI\datahub\ohlcv_daily_all.parquet",
    [string]$BoardCsv = "G:\AI\datahub\metadata\symbol_board.csv",
    [string]$ReportXlsx = "G:\AI\datahub\reports\market_all_in_one.xlsx",
    [string]$DetailSample = "2330.TW,2317.TW,1101.TW",
    [int]$TopN = 100
)

Write-Host "[INFO] Running all steps..."

# 呼叫核心 Python 腳本
python "G:\AI\tw-alpha-stack\run_all_v2_core.py" `
  --ohlcv-dir $OhlcvDir `
  --merged-path $MergedPath `
  --board-csv $BoardCsv `
  --report-xlsx $ReportXlsx `
  --detail-sample $DetailSample `
  --topn $TopN `
  $(if ($WithCharts) { "--with-charts" }) `
  $(if ($CleanReports) { "--clean-reports" })
