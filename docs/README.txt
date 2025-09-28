AlphaCity Core fix6 v6.2.2 (coordinated, backward-compatible)
---------------------------------------------------------------
這版的目標：讓「每一輪執行都有對應的 metrics CSV」，並且由 PS 腳本穩定綁定到本輪輸出。
- 不改 CLI、不改 JSON Schema（仍為 6.2）
- 其它程式（A–G、排程、CI）全部相容

內容：
1) scripts/ps/Invoke-AlphaVerification.ps1（r6）
   - 先用目錄前後差異抓本輪新增/變大的 metrics；再輔以輸出解析與時間窗。
   - 若子程序未產生 metrics，也不會出錯，因為第 2 項會補一份。

2) scripts/emit_metrics_always.py（新）
   - 介面完全相容：呼叫方式與既有 wrapper 一樣（第一個參數是目標 Python 腳本）。
   - 功能：執行目標腳本後，若本輪沒有產生新的 metrics，就自動生成一份空的 CSV，並印出路徑。
   - 這是附加檔，不會影響其他使用者；PS 腳本會「優先」使用它；不存在時自動退回舊 wrapper。

安裝：
  1) 備份舊 PS：Copy-Item .\scripts\ps\Invoke-AlphaVerification.ps1 .\scripts\ps\Invoke-AlphaVerification.ps1.bak -Force
  2) 解壓本壓縮檔後，覆蓋到專案根目錄
  3) Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1

驗證：
  - 非交易日：ack -Start '2025-09-21' -End '2025-09-21' -SkipFull -Symbol 2330.TW -CalendarCsv .\cal\trading_days.csv -VerboseCmd
    預期：PASS_NOOP / end_is_non_trading_day；並且 metrics 目錄會多一份空 CSV（reason=no_metrics_from_child）。
  - 交易日（首次回補）：ack -Start '2025-09-19' -End '2025-09-19' -SkipFull -Symbol 1101.TW -Workers 6 -Qps 1.6 -CalendarCsv .\cal\trading_days.csv -VerboseCmd
    預期：PASS / write；會產生非空 metrics。

回滾：
  Copy-Item .\scripts\ps\Invoke-AlphaVerification.ps1.bak .\scripts\ps\Invoke-AlphaVerification.ps1 -Force
