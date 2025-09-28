# Fetch-All-SingleDateId（批量抓取「需要 date_id」的單股 API）

安裝（Windows PowerShell）：
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_Package.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

使用：
```powershell
cd G:\AI\tw-alpha-stack

python scripts/build_universe.py `
  --datahub-root "G:\AI\tw-alpha-stack\datahub" `
  --out "G:\AI\tw-alpha-stack\datahub\universe.csv"

.\scripts\ps\Fetch-All-SingleDateId.ps1 `
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\universe.csv" `
  -Start 2015-01-01 -End (Get-Date).ToString("yyyy-MM-dd") `
  -Workers 6 -Qps 1.6
```
