param(
  [Parameter(Mandatory=$true)][string]$factors,
  [Parameter(Mandatory=$true)][string]$config,
  [Parameter(Mandatory=$true)][string]$outdir
)
$py = Join-Path (Get-Location) ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }
& $py "run_all_backtests.py" "--factors" $factors "--config" $config "--outdir" $outdir `
  "--factorset" "composite_score" "mom_252_21" "vol_20" "--topn" 20 50 100 "--rebalance" "M" "W"
