# Fetch-All-SingleDateId Pro v2.1b5 (hotfix)

**更新點**
- Dataset 級別打點：每個資料集完成會打印 `"[OK] <symbol> | <dataset> | <秒數>s"`，失敗則打印 `[FAIL] ...` 並寫入 ResumeLog。
- 直播輸出：子任務的 python 日誌會即時顯示，並寫入 `metrics/single_logs/single_<symbol>.log`。
- 其他保留：速率保護、Shim 市場級、symbol 正規化、斷點續跑、重試/退避、進度/ETA。

## 安裝
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_Pro_v2_1b5_hotfix.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 使用（建議 4×0.04=0.16）
```powershell
cd G:\AI\tw-alpha-stack

.\scripts\ps\Fetch-All-SingleDateId_Pro_v2_1b5_hotfix.ps1 `
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
  -Workers 4 -Qps 0.04 `
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 快速監看
```powershell
Get-Content "G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log" -Tail 50 -Wait
Get-ChildItem "G:\AI\tw-alpha-stack\metrics\single_logs" | Select Name,Length,LastWriteTime | Sort LastWriteTime -Descending | Select -First 10
```

