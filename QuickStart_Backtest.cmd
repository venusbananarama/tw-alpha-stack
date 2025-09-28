@echo off
REM QuickStart for backtest
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0unified_run_backtest.ps1" ^
  -Factors "composite_score mom_252_21 vol_20" ^
  -OutDir "G:\AI\datahub\alpha\backtests\topN_50_W" ^
  -Start "2015-01-01" -End "2020-12-31" ^
  -FactorsPath "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" ^
  -Config "configs\backtest_topN_example.yaml" ^
  -TopN 50 -Rebalance "W" -Costs 0.0005
