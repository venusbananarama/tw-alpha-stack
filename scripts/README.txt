scripts bundle (English, ASCII only)
===========================================

Files
-----
- stack_paths.cmd          : set defaults; prefer venv python if present
- run_all_v2.cmd           : merge -> board mapping -> report
- run_report_once.cmd      : report only
- merge_only.cmd           : merge only
- board_only.cmd           : build mapping only

Default paths (override via env vars before run)
------------------------------------------------
OHLCV_DIR   = G:\AI\datahub\ohlcv_daily
MERGED_PATH = G:\AI\datahub\ohlcv_daily_all.parquet
BOARD_CSV   = G:\AI\datahub\metadata\symbol_board.csv
REPORT_XLSX = G:\AI\datahub\reports\market_all_in_one.xlsx
DETAIL_SAMPLE = 2330.TW,2317.TW,1101.TW
TOPN        = 100

Examples
--------
1) Full pipeline with charts
   G:\AI\tw-alpha-stack\scripts\run_all_v2.cmd --with-charts

2) Only report (no merge)
   G:\AI\tw-alpha-stack\scripts\run_report_once.cmd --with-charts

3) Only merge
   G:\AI\tw-alpha-stack\scripts\merge_only.cmd

Notes
-----
- Scripts are ASCII only to avoid console mojibake.
- If .venv\Scripts\python.exe exists, it will be used automatically.
