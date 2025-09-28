@echo off
call "%~dp0stack_paths.cmd"

echo [1/4] Check Python
"%PYTHON%" --version || (echo [ERROR] Python not found && exit /b 1)

echo [2/4] Merge OHLCV
"%PYTHON%" "%PROJ_ROOT%\ingest\merge_ohlcv.py" --in-root "%OHLCV_DIR%" --out "%MERGED_PATH%"

echo [3/4] Build Board Mapping
"%PYTHON%" "%PROJ_ROOT%\ingest\build_symbol_board.py" --from-parquet "%MERGED_PATH%" --out "%BOARD_CSV%"

echo [4/4] Generate Market Report
"%PYTHON%" "%PROJ_ROOT%\ingest\market_report_all_in_one.py" --file "%MERGED_PATH%" --board-csv "%BOARD_CSV%" --detail-sample "2330.TW,2317.TW,1101.TW" --topn 100 --with-charts --out "%REPORT_XLSX%"

echo [DONE] All steps completed.
pause
