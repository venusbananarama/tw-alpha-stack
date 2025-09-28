@echo off
REM -------- run_report_once.cmd --------
REM Generate the Excel report only (assumes merged parquet and board csv exist)
setlocal
set "SCRIPT_DIR=%~dp0"
call "%SCRIPT_DIR%\stack_paths.cmd"

REM flags:
set "WITH_CHARTS="
:parse
if "%~1"=="" goto run
if /I "%~1"=="--with-charts" set "WITH_CHARTS=--with-charts"
shift
goto parse

:run
echo [RUN] Report only -> "%REPORT_XLSX%"
"%PY_EXE%" "%REPO_ROOT%\ingest\market_report_all_in_one.py" --file "%MERGED_PATH%" --board-csv "%BOARD_CSV%" --detail-sample "%DETAIL_SAMPLE%" --topn %TOPN% --out "%REPORT_XLSX%" %WITH_CHARTS%
set ERR=%ERRORLEVEL%
if not "%ERR%"=="0" echo [ERROR] Report failed with code %ERR% & (endlocal & exit /b %ERR%)
echo [OK] Report done.
endlocal & exit /b 0
