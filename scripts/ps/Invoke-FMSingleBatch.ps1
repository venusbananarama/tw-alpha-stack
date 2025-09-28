
# File: scripts\ps\Invoke-FMSingleBatch.ps1
param(
    [string]$Start = "2015-01-01",
    [string]$End   = (Get-Date).ToString("yyyy-MM-dd"),
    [string]$UniverseFile = "configs\universe.tw_all_sorted.txt",
    [string[]]$Datasets = @(
        "TaiwanStockGovernmentBankBuySell",
        "TaiwanStockPER",
        "TaiwanStockDividend",
        "TaiwanStockFinancialStatements",
        "TaiwanStockBalanceSheet",
        "TaiwanStockCashFlowsStatement",
        "TaiwanStockShareholding"
    ),
    [int]$BatchSize = 100
)

Write-Host "== [AlphaCity] Single-Stock Backfill (Batch Mode) =="

$symbols = Get-Content $UniverseFile | Where-Object { $_ -match '^\d{4}$' }

$chunks = [System.Collections.ArrayList]@()
for ($i = 0; $i -lt $symbols.Count; $i += $BatchSize) {
    $chunks.Add($symbols[$i..([Math]::Min($i+$BatchSize-1, $symbols.Count-1))])
}

$batchNum = 1
foreach ($chunk in $chunks) {
    Write-Host "== Batch $batchNum / $($chunks.Count) =="

    foreach ($ds in $Datasets) {
        Write-Host "== Dataset $ds =="
        foreach ($sym in $chunk) {
            Write-Host "→ $ds $sym"
            python scripts/finmind_backfill.py `
              --start $Start `
              --end $End `
              --datasets $ds `
              --universe $sym
            Start-Sleep -Milliseconds 500
        }
    }

    $batchNum++
    Write-Host "== Batch done, sleeping 10s =="
    Start-Sleep -Seconds 10
}
