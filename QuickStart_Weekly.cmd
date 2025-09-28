@echo off
REM QuickStart for weekly snapshot
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0check_weekly_after_patch.ps1" ^
  -Factors "composite_score mom_252_21 vol_20" ^
  -OutDir "G:\AI\datahub\alpha\backtests\grid_test\_weekly_check" ^
  -Start "2015-01-01" -End "2020-12-31" ^
  -FactorsPath "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" ^
  -Config "configs\backtest_topN_fixed.yaml"
