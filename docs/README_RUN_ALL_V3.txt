快速使用說明

1) 下載並解壓本壓縮檔到任意資料夾（建議放在 repo 根目錄旁）。內含：
   - run_all_v3.ps1                     一鍵全流程（避免 Here-Doc 相容性問題）
   - patch_market_report_all_in_one.py  修補 market_report_all_in_one.py 的小問題
   - README_RUN_ALL_V3.txt              本說明

2) 套用 Python 腳本修補（建議做一次即可）：
   打開 PowerShell：
     python patch_market_report_all_in_one.py --file "G:\AI\tw-alpha-stack\ingest\market_report_all_in_one.py"

3) 執行一鍵流程：
   在 repo 根目錄：
     Unblock-File .\run_all_v3.ps1
     .\run_all_v3.ps1 -WithCharts -CleanReports `
       -OhlcvDir "G:\AI\datahub\ohlcv_daily" `
       -MergedPath "G:\AI\datahub\ohlcv_daily_all.parquet" `
       -BoardCsv "G:\AI\datahub\metadata\symbol_board.csv" `
       -ReportXlsx "G:\AI\datahub\reports\market_all_in_one.xlsx" `
       -DetailSample "2330.TW,2317.TW,1101.TW" `
       -TopN 100

   若不想產生圖表，可以移除 -WithCharts。

4) 每日自動化（可選）：
   工作排程器建立新工作於 18:30 執行：
     Program/script: powershell
     Add arguments: -ExecutionPolicy Bypass -File "G:\AI\tw-alpha-stack\run_all_v3.ps1" -WithCharts -CleanReports
     Start in: G:\AI\tw-alpha-stack

備註：
- patch 內容只有兩點：修正 groupby.apply 的使用方式、設定 matplotlib Agg 後端；不影響既有功能。
- run_all_v3.ps1 會自動檢查/安裝 pandas、pyarrow、xlsxwriter、matplotlib、loguru，避免因缺套件中斷。
