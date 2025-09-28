@echo off
REM -------- merge_only.cmd --------
REM Merge ohlcv parquet files into a single parquet
setlocal
set "SCRIPT_DIR=%~dp0"
call "%SCRIPT_DIR%\stack_paths.cmd"

echo [RUN] Merge -> "%MERGED_PATH%"
"%PY_EXE%" "%REPO_ROOT%\ingest\merge_ohlcv.py" --ohlcv-root "%OHLCV_DIR%" --out "%MERGED_PATH%"
set ERR=%ERRORLEVEL%
if not "%ERR%"=="0" echo [ERROR] Merge failed with code %ERR% & (endlocal & exit /b %ERR%)
echo [OK] Merge done.
endlocal & exit /b 0
