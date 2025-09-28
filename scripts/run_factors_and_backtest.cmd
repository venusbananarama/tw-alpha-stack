
@echo off
setlocal ENABLEDELAYEDEXPANSION
REM === Edit these paths if needed ===
set OHLCV_ALL=G:\AI\datahub\ohlcv_daily_all.parquet
set FACTORS_OUT=G:\AI\datahub\alpha\alpha_factors.parquet
set BT_OUT=G:\AI\datahub\alpha\backtests\topN_50_M

REM 1) Compute factors
python "%~dp0..\ingest\alpha_factors.py" --file "%OHLCV_ALL%" --out "%FACTORS_OUT%" --config "%~dp0..\configs\factors_example.yaml"

REM 2) Backtest TopN
python "%~dp0..\backtest\longonly_topN.py" --factors "%FACTORS_OUT%" --out-dir "%BT_OUT%" --config "%~dp0..\configs\backtest_topN_example.yaml"

echo Done.
