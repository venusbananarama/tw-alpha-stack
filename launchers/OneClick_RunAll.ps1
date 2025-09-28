param(
    [ValidateSet("Backtest","Weekly")][string]$Mode = "Backtest",
    [switch]$NoPause
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

Write-Host "=== Step 1: 環境檢查 ==="
powershell -NoProfile -ExecutionPolicy Bypass -File tools\check_env.ps1 -FixPolicy

if ($Mode -eq "Backtest") {
    Write-Host "=== Step 2: 執行回測 ==="
    .\unified_run_backtest.ps1 `
      -Factors "composite_score mom_252_21 vol_20" `
      -OutDir "out\OneClick_Backtest" `
      -Start "2015-01-01" -End "2020-12-31" `
      -FactorsPath "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" `
      -Config "configs\backtest_topN_example.yaml" `
      -TopN 50 -Rebalance "W" -Costs 0.0005 -NoPause
    Write-Host "=== Step 3: 開啟最新結果 ==="
    .\open_last_results.ps1 -Dir "out\OneClick_Backtest" -NoPause
}
elseif ($Mode -eq "Weekly") {
    Write-Host "=== Step 2: 執行週度快照 ==="
    .\check_weekly_after_patch.ps1 `
      -Factors "composite_score mom_252_21 vol_20" `
      -OutDir "out\OneClick_Weekly" `
      -Start "2015-01-01" -End "2020-12-31" `
      -FactorsPath "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" `
      -Config "configs\backtest_topN_fixed.yaml" -NoPause
    Write-Host "=== Step 3: 開啟最新結果 ==="
    .\open_last_results.ps1 -Dir "out\OneClick_Weekly" -WeeklyPreview -NoPause
}

if (-not $NoPause) { Pause }
