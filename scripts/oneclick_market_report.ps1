# Requires: Windows PowerShell, Python venv with pandas/xlsxwriter installed.
param(
  [string]$AllParquet = "G:\AI\datahub\ohlcv_daily_all.parquet",
  [string]$MetaCsv    = "G:\AI\datahub\metadata\symbol_board.csv",
  [string]$ReportXlsx = "G:\AI\datahub\reports\market_all_in_one.xlsx"
)

function Run-Py([string]$cmdline) {
  Write-Host ">> python $cmdline"
  & python $cmdline
  if ($LASTEXITCODE -ne 0) { throw "Python step failed: $cmdline" }
}

# 1) Ensure merged parquet exists
if (-not (Test-Path $AllParquet)) {
  throw "Merged parquet not found: $AllParquet"
}

# 2) Build or update board mapping
Run-Py "ingest\build_symbol_board.py --from-parquet `"$AllParquet`" --out `"$MetaCsv`""

# 3) Generate all-in-one report (will include BoardStats if MetaCsv present)
Run-Py "ingest\market_report_all_in_one.py --file `"$AllParquet`" --board-csv `"$MetaCsv`" --detail-sample `"2330.TW,2317.TW,1101.TW`" --out `"$ReportXlsx`""

# 4) Optional: classic summaries
Run-Py "ingest\market_summary_with_stats.py --file `"$AllParquet`" --out-root `"G:\AI\datahub\reports\market_summary`""
Run-Py "ingest\market_summary_by_board.py --file `"$AllParquet`" --out `"G:\AI\datahub\reports\market_summary_by_board.csv`""

Write-Host "=== DONE ==="
Write-Host "Report: $ReportXlsx"
Write-Host "Board CSV: $MetaCsv"
