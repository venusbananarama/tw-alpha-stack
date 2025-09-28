# Fetch-All-SingleDateId Pro v2.1b3 (hotfix)

**修正目標**
- 你環境中的 `Invoke-FMAll.ps1` 內部型別與 `.Count` 用法不一致，導致我們傳入 `chip` 後仍報 `.Count` 錯。
- 這個版本改為使用 **自帶的 `Invoke-FMAll_Shim.ps1`**，直接呼叫 `finmind_backfill.py` 跑市場級資料集，完全不再依賴你原本的 `Invoke-FMAll.ps1`。

## 安裝
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_Pro_v2_1b3_hotfix.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 使用（替代前一版指令）
```powershell
cd G:\AI\tw-alpha-stack

.\scripts\ps\Fetch-All-SingleDateId_Pro_v2_1b3_hotfix.ps1 `
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv" `
  -Datasets @(
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockShareholding",
    "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockGovernmentBankBuySell",
    "TaiwanStockPER","TaiwanStockPBR",
    "TaiwanStockTotalMarginPurchaseShortSale",      # 自動分流 → Shim (chip)
    "TaiwanStockTotalInstitutionalInvestors"        # 自動分流 → Shim (chip)
  ) `
  -ApiToken "<YOUR_TOKEN>" -UseThreadJob `
  -Workers 4 -Qps 0.03 `
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 附註
- 仍保留：速率保護、symbol 正規化、斷點續跑、重試/退避、進度顯示。
- 若要加入其他市場級資料集（例如指數或總表），在 `Invoke-FMAll_Shim.ps1` 的 `$Map` 中添加即可。
