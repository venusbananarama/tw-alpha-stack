# run_weekly_backtest.ps1 — 本機排程 / 手動可執行（UTF-8）
param(
  [string]$Root = "G:\AI\tw-alpha-stack"
)

chcp 65001 > $null
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force

Push-Location $Root
try {
  if (Test-Path ".\.venv\Scripts\Activate.ps1") { . .\.venv\Scripts\Activate.ps1 }

  python run_batch_backtests.py `
    --grid-yaml "configs/batch_grid_minimal.yaml" `
    --backtest-cmd "python backtest/longonly_topN.py --factors {factors} --out-dir {out_dir} --config {config} {extra}" `
    --out-root "G:\AI\datahub\alpha\backtests\grid_test_minimal" `
    --reports yes `
    --report-script "make_report_safe.py"
}
finally {
  Pop-Location
}
