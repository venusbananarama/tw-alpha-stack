# Fetch-All-SingleDateId Pro v2

**功能強化：** 自訂 `-Datasets`、重試/退避、進度/ETA、失敗率告警、支援 `investable_universe.csv` 或 `symbols.txt`。

## 安裝
把 ZIP 展開到專案根：
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_Pro_v2.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 使用
```powershell
cd G:\AI\tw-alpha-stack

# 以 investable_universe.csv 為輸入
.\scripts\ps\Fetch-All-SingleDateId_Pro.ps1 `
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv" `
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd")) `
  -Workers 6 -Qps 1.6

# 或使用 symbols.txt
.\scripts\ps\Fetch-All-SingleDateId_Pro.ps1 `
  -SymbolsTxt "G:\AI\tw-alpha-stack\datahub\_meta\symbols.txt" `
  -Workers 6 -Qps 1.6
```

## 自訂資料集
```powershell
.\scripts\ps\Fetch-All-SingleDateId_Pro.ps1 `
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv" `
  -Datasets @("TaiwanStockPER","TaiwanStockPBR") `
  -Workers 6 -Qps 1.2
```

## 斷點續跑
腳本會把成功/失敗寫到 `metrics\fetch_single_dateid.log`。重跑時自動跳過已 `|ok` 的 symbol。
若只想重跑失敗者：清理該 log 中 `|fail|` 以外的行即可。

## 建議
- 初跑先小一點：`-Workers 4 -Qps 1.2`；穩定後再拉高。
- 失敗率告警觸發（>5%）時，請降低 `-Qps` 或 `-Workers`。
