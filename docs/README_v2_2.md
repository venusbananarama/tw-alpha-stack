# Fetch-All-SingleDateId v2.2 — Simplified Runner

**目的**：避免 ThreadJob/Background Job 的各種兼容問題，用 PowerShell 7 `ForEach-Object -Parallel` 直接並行。
**關鍵**：`ThrottleLimit × QpsPerWorker ≤ 0.16`（FinMind 600/hr 上限）。

## 安裝
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_v2_2.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 全市場（總速率 = 600/hr）
```powershell
cd G:\AI\tw-alpha-stack

.\scripts\ps\Fetch-All-SingleDateId_v2_2.ps1 `
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv" `
  -Datasets @(
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockShareholding",
    "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockGovernmentBankBuySell",
    "TaiwanStockPER","TaiwanStockPBR",
    "TaiwanStockTotalMarginPurchaseShortSale",
    "TaiwanStockTotalInstitutionalInvestors"
  ) `
  -ApiToken "<YOUR_TOKEN>" `
  -ThrottleLimit 4 -QpsPerWorker 0.04 `
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 沒進度時的煙霧測試（Sequential，10 檔）
```powershell
.\scripts\ps\Fetch-All-SingleDateId_v2_2.ps1 `
  -SymbolsTxt "G:\AI\tw-alpha-stack\datahub\_meta\symbols.txt" `
  -Datasets @("TaiwanStockPER","TaiwanStockPBR") `
  -ApiToken "<YOUR_TOKEN>" `
  -Sequential `
  -MaxSymbols 10 -ThrottleLimit 1 -QpsPerWorker 0.08 `
  -Start 2020-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 監看
- 主進度檔：`G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log`
- 單股日誌：`G:\AI\tw-alpha-stack\metrics\single_logs\single_<symbol>.log`
