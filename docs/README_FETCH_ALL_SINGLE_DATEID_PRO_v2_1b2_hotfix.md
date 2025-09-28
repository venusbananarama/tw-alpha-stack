# Fetch-All-SingleDateId Pro v2.1b2 (hotfix)

**修正**
- 呼叫 `Invoke-FMAll.ps1` 改為 `-Datasets @($g)`（陣列）以相容該腳本內部 `.Count` 寫法。

**仍保留**
- 速率保護（Workers×Qps ≤ 0.16）
- 市場級資料集自動分流（映射到 `prices|chip|stock_info`）
- symbol 正規化、斷點續跑、重試/退避、進度顯示

## 安裝
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_Pro_v2_1b2_hotfix.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 使用
```powershell
cd G:\AI\tw-alpha-stack

.\scripts\ps\Fetch-All-SingleDateId_Pro_v2_1b2_hotfix.ps1 `
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
  -ApiToken "<YOUR_TOKEN>" -UseThreadJob `
  -Workers 4 -Qps 0.03 `
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

