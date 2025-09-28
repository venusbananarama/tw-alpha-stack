@echo off
REM -------- board_only.cmd --------
REM Build or update symbol->board mapping from merged parquet
setlocal
set "SCRIPT_DIR=%~dp0"
call "%SCRIPT_DIR%\stack_paths.cmd"

echo [RUN] Build board mapping -> "%BOARD_CSV%"
"%PY_EXE%" "%REPO_ROOT%\ingest\build_symbol_board.py" --from-parquet "%MERGED_PATH%" --out "%BOARD_CSV%"
set ERR=%ERRORLEVEL%
if not "%ERR%"=="0" echo [ERROR] Board mapping failed with code %ERR% & (endlocal & exit /b %ERR%)
echo [OK] Board mapping done.
endlocal & exit /b 0
