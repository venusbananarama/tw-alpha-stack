@echo off
REM === run_all_v2.cmd (hotfix) ===
REM Robust, pure-ASCII, no echo. blanks; avoids ". was unexpected" in cmd.exe

setlocal EnableExtensions

REM Resolve repo root (scripts folder is %~dp0)
set "SCRIPTS_DIR=%~dp0"
for %%I in ("%SCRIPTS_DIR%\..") do set "ROOT=%%~fI"

REM If stack_paths.cmd exists, call it to load ENV (safe if missing)
if exist "%SCRIPTS_DIR%\stack_paths.cmd" call "%SCRIPTS_DIR%\stack_paths.cmd"

REM ===== Defaults (can be overridden by environment variables) =====
if not defined OHLCV_DIR     set "OHLCV_DIR=G:\AI\datahub\ohlcv_daily"
if not defined MERGED_PATH   set "MERGED_PATH=G:\AI\datahub\ohlcv_daily_all.parquet"
if not defined BOARD_CSV     set "BOARD_CSV=G:\AI\datahub\metadata\symbol_board.csv"
if not defined REPORT_XLSX   set "REPORT_XLSX=G:\AI\datahub\reports\market_all_in_one.xlsx"
if not defined DETAIL_SAMPLE set "DETAIL_SAMPLE=2330.TW,2317.TW,1101.TW"
if not defined TOPN          set "TOPN=100"

REM ===== Parse flags =====
set "WITH_CHARTS_FLAG="
:parse_args
if "%~1"=="" goto start
if /I "%~1"=="--with-charts" set "WITH_CHARTS_FLAG=--with-charts"
shift
goto parse_args

:start
echo [OK] Paths loaded.
echo [1/4] Check Python
python -V >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Python not found
  exit /b 1
)
for /f "delims=" %%v in ('python -V') do echo %%v

echo [2/4] Merge OHLCV
python "%ROOT%\ingest\merge_ohlcv.py" --ohlcv-root "%OHLCV_DIR%" --out "%MERGED_PATH%"
if errorlevel 1 goto fail

echo [3/4] Build Board Mapping
python "%ROOT%\ingest\build_symbol_board.py" --from-parquet "%MERGED_PATH%" --out "%BOARD_CSV%"
if errorlevel 1 goto fail

echo [4/4] Generate Market Report
python "%ROOT%\ingest\market_report_all_in_one.py" --file "%MERGED_PATH%" --board-csv "%BOARD_CSV%" --detail-sample "%DETAIL_SAMPLE%" --topn %TOPN% %WITH_CHARTS_FLAG% --out "%REPORT_XLSX%"
if errorlevel 1 goto fail

echo [DONE] All steps completed.
exit /b 0

:fail
echo [ERROR] Pipeline failed. Please check messages above.
exit /b 1
