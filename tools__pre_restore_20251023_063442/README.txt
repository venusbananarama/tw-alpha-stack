AlphaCity Backfill Toolkit
==========================
- Run-Backfill-All.ps1 : 全市場 price/chip/per/dividend → 升銀 → preflight 驗收
- Monitor-Backfill.ps1 : 監控（追 log、RAW/SILVER 檔量、四資料集 freshness）

用法
----
Set-Location C:\AI\tw-alpha-stack
$env:ALPHACITY_ALLOW='1'
.\tools\Run-Backfill-All.ps1
.\tools\Monitor-Backfill.ps1 -IntervalSec 30

注意
----
- 需在此視窗設 `FINMIND_TOKEN`；（若無 `FINMIND_BEARER` / `FINMIND_BASE_URL`，主腳本會設定）
- --end 不含當日，腳本以 preflight 的 expect_date (=T0+1) 作為上限
- Dividend 採「TaiwanStockDividend（按日全市場）+ dividend（單股補缺）」兩段法
