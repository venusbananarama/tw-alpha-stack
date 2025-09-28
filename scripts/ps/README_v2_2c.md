# Fetch-All-SingleDateId v2.2c — Full Runner

## 安裝
```
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_v2_2c.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 全市場並行（600/hr）
```
cd G:\AI\tw-alpha-stack

.\scripts\ps\Fetch-All-SingleDateId_v2_2c.ps1 ^
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv" ^
  -Datasets @(
    "TaiwanStockInstitutionalInvestorsBuySell",
    "TaiwanStockShareholding",
    "TaiwanStockMarginPurchaseShortSale",
    "TaiwanStockGovernmentBankBuySell",
    "TaiwanStockPER","TaiwanStockPBR"
  ) ^
  -ApiToken "<YOUR_TOKEN>" -ThrottleLimit 4 -QpsPerWorker 0.04 ^
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 煙霧測試（Sequential 10 檔）
```
.\scripts\ps\Fetch-All-SingleDateId_v2_2c.ps1 ^
  -SymbolsTxt "G:\AI\tw-alpha-stack\datahub\_meta\symbols.txt" ^
  -Datasets @("TaiwanStockPER","TaiwanStockPBR") ^
  -ApiToken "<YOUR_TOKEN>" -Sequential ^
  -MaxSymbols 10 -ThrottleLimit 1 -QpsPerWorker 0.08 ^
  -Start 2020-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 監看
```
Get-Content "G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log" -Tail 50 -Wait
```
