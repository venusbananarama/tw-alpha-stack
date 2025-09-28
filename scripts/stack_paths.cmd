@echo off
REM -------- stack_paths.cmd --------
REM Set default paths and Python location. ASCII only (no Unicode) to avoid mojibake.

set "SCRIPT_DIR=%~dp0"
for %%I in ("%SCRIPT_DIR%\..") do set "REPO_ROOT=%%~fI"

REM Defaults (override by setting env vars before calling)
if not defined OHLCV_DIR set "OHLCV_DIR=G:\AI\datahub\ohlcv_daily"
if not defined MERGED_PATH set "MERGED_PATH=G:\AI\datahub\ohlcv_daily_all.parquet"
if not defined BOARD_CSV set "BOARD_CSV=G:\AI\datahub\metadata\symbol_board.csv"
if not defined REPORT_XLSX set "REPORT_XLSX=G:\AI\datahub\reports\market_all_in_one.xlsx"
if not defined DETAIL_SAMPLE set "DETAIL_SAMPLE=2330.TW,2317.TW,1101.TW"
if not defined TOPN set "TOPN=100"

REM Prefer venv Python if available
if exist "%REPO_ROOT%\.venv\Scripts\python.exe" (
  set "PY_EXE=%REPO_ROOT%\.venv\Scripts\python.exe"
) else (
  set "PY_EXE=python"
)

echo [OK] Paths loaded.
exit /b 0
