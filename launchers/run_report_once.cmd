@echo off
call "%~dp0stack_paths.cmd"

echo [Run] Market Report Only
"%PYTHON%" "%PROJ_ROOT%\ingest\market_report_all_in_one.py" --file "%MERGED_PATH%" --board-csv "%BOARD_CSV%" --detail-sample "2330.TW,2317.TW,1101.TW" --topn 100 --with-charts --out "%REPORT_XLSX%"

pause
