# Fetch-All-SingleDateId v2.2a — Hotfix

**修正點**：Sequential 模式下出現「Using variable cannot be retrieved」錯誤。  
**作法**：以參數傳遞 `$InvokeSinglePath`，避免在子 ScriptBlock 使用 `$using:`。

## 安裝
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_v2_2a_hotfix.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 煙霧測試（Sequential，10 檔）
```powershell
cd G:\AI\tw-alpha-stack

.\scripts\ps\Fetch-All-SingleDateId_v2_2a_hotfix.ps1 `
  -SymbolsTxt "G:\AI\tw-alpha-stack\datahub\_meta\symbols.txt" `
  -Datasets @("TaiwanStockPER","TaiwanStockPBR") `
  -ApiToken "<YOUR_TOKEN>" `
  -Sequential `
  -MaxSymbols 10 -ThrottleLimit 1 -QpsPerWorker 0.08 `
  -Start 2020-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 全市場（並行，總速率 = 600/hr）
```powershell
.\scripts\ps\Fetch-All-SingleDateId_v2_2a_hotfix.ps1 `
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

## 監看
- 主進度檔：`G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log`
- 單股日誌：`G:\AI\tw-alpha-stack\metrics\single_logs\single_<symbol>.log`
