# Fetch-All-SingleDateId Pro v2.1b

**新功能**
- 速率保護：Workers×Qps 必須 ≤ 0.16（約 600/hr），超過直接拒跑。
- 自動拆分資料集：把「整體市場」資料集獨立用 Invoke-FMAll.ps1 跑一次，單股只跑需要 data_id 的。
- 符號正規化：自動去除 .TW/.TWO 與非數字，確保送到 API 的是數字代碼。

## 安裝
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_Pro_v2_1b.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 使用（建議速率）
```powershell
cd G:\AI\tw-alpha-stack

# 單股 + 整體市場自動拆跑（ThreadJob + Token 注入）
.\scripts\ps\Fetch-All-SingleDateId_Pro_v2_1b.ps1 `
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv" `
  -Datasets @(
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockShareholding",
    "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockGovernmentBankBuySell",
    "TaiwanStockPER","TaiwanStockPBR",
    "TaiwanStockTotalMarginPurchaseShortSale",        # ← 自動拆到 FMAll
    "TaiwanStockTotalInstitutionalInvestors"          # ← 自動拆到 FMAll
  ) `
  -ApiToken "<YOUR_TOKEN>" -UseThreadJob `
  -Workers 4 -Qps 0.03 `
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 疑難排解
- `Start-ThreadJob` 找不到 → 安裝/匯入 ThreadJob 模組：
  ```powershell
  Install-Module ThreadJob -Scope CurrentUser -Force
  Import-Module ThreadJob
  ```
- 查看失敗 Job 詳細錯誤：
  ```powershell
  Get-Job -State Failed | % { "-----"; $_.Name; Receive-Job $_ -Keep }
  ```
