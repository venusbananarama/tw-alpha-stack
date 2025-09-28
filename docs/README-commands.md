# 📌 常用指令片段

## 1. 單股回填（例：台積電）
```powershell
cd G:\AI\tw-alpha-stack
.\scripts\ps\Invoke-FMSingle.ps1 `
  -Start 2024-01-01 -End 2024-01-03 `
  -Symbols @('2330.TW') `
  -Datasets @('prices','chip') `
  -Workers 2 -Qps 2 -VerboseCmd
```

## 2. 全市場（日價 + 籌碼）
```powershell
cd G:\AI\tw-alpha-stack
.\scripts\ps\Invoke-FMAll.ps1 `
  -Start 2015-01-01 `
  -End (Get-Date).ToString("yyyy-MM-dd") `
  -Datasets @("prices","chip") `
  -Universe TSE `
  -Workers 6 -Qps 1.6 -VerboseCmd
```

## 3. 驗證 Silver
```powershell
cd G:\AI\tw-alpha-stack\scripts
.\Check-Silver.ps1 `
  -DatahubRoot "G:\AI\datahub" `
  -SchemaPath  "G:\AI\tw-alpha-stack\schemas\datasets_schema.yaml" `
  -ReportCsv   "G:\AI\datahub\_dq\validate_report.csv" `
  -Strict
```

## 4. 修復 Silver（date → datetime64）
```powershell
cd G:\AI\tw-alpha-stack\scripts
.\Repair-Silver.ps1 `
  -DatahubRoot "G:\AI\datahub" `
  -SchemaPath  "G:\AI\tw-alpha-stack\schemas\datasets_schema.yaml" `
  -Datasets @("prices","chip") `
  -Backup `
  -Strict
```

## 5. 一條龍（回填 → 驗證）
```powershell
cd G:\AI\tw-alpha-stack
.\scripts\ps\Invoke-FMAll.ps1 `
  -Start 2020-01-01 `
  -End (Get-Date).ToString("yyyy-MM-dd") `
  -Datasets @("prices","chip") `
  -Universe TSE `
  -Workers 4 -Qps 1.5 -VerboseCmd

cd G:\AI\tw-alpha-stack\scripts
.\Check-Silver.ps1 `
  -DatahubRoot "G:\AI\datahub" `
  -SchemaPath  "G:\AI\tw-alpha-stack\schemas\datasets_schema.yaml" `
  -Strict
```
