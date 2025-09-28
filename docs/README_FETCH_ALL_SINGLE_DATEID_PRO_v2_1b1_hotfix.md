# Fetch-All-SingleDateId Pro v2.1b1 (hotfix)

**修正內容**
- 將市場級資料集自動映射為 `Invoke-FMAll.ps1` 的群組（`prices|chip|stock_info`），逐一執行。
- 單股資料集仍以 `Invoke-FMSingle.ps1` 分檔併發處理。
- 內建速率保護（Workers×Qps ≤ 0.16）。

## 安裝
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_Pro_v2_1b1_hotfix.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 用法
```powershell
cd G:\AI\tw-alpha-stack

.\scripts\ps\Fetch-All-SingleDateId_Pro_v2_1b1_hotfix.ps1 `
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv" `
  -Datasets @(
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockShareholding",
    "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockGovernmentBankBuySell",
    "TaiwanStockPER","TaiwanStockPBR",
    "TaiwanStockTotalMarginPurchaseShortSale",      # 會自動映射成 chip 給 FMAll
    "TaiwanStockTotalInstitutionalInvestors"        # 會自動映射成 chip 給 FMAll
  ) `
  -ApiToken "<YOUR_TOKEN>" -UseThreadJob `
  -Workers 4 -Qps 0.03 `
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

