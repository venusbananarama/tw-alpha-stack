
# File: scripts\ps\Check-FMSingleStatus.ps1
Write-Host "== [AlphaCity] Single-Stock Dataset Status =="

$root = "data\finmind\raw"

$datasets = @(
    "TaiwanStockGovernmentBankBuySell",
    "TaiwanStockPER",
    "TaiwanStockDividend",
    "TaiwanStockFinancialStatements",
    "TaiwanStockBalanceSheet",
    "TaiwanStockCashFlowsStatement",
    "TaiwanStockShareholding"
)

foreach ($ds in $datasets) {
    $files = Get-ChildItem "$root\$ds" -Filter *.parquet -ErrorAction SilentlyContinue
    if ($files.Count -eq 0) {
        Write-Host "❌ $ds → no files"
        continue
    }

    $first = ($files | Sort-Object Name | Select-Object -First 1).Name
    $last  = ($files | Sort-Object Name | Select-Object -Last 1).Name
    Write-Host "✅ $ds → files=$($files.Count) range=[$first → $last]"
}
