# run_daily_etl.ps1  — 本機排程 / 手動可執行（UTF-8）(FIXED DATASET ALIAS)
param(
  [string]$Root = "G:\AI\tw-alpha-stack"
)

chcp 65001 > $null
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force

Push-Location $Root
try {
  # 啟動 venv（若存在）
  if (Test-Path ".\.venv\Scripts\Activate.ps1") { . .\.venv\Scripts\Activate.ps1 }

  # 全市場（日價，分批）— 使用別名 'prices' 以符合 Invoke-FMAll.ps1 的 ValidateSet
  .\scripts\ps\Invoke-FMAll.ps1 -Start 2015-01-01 -End (Get-Date).ToString('yyyy-MM-dd') -Datasets @('prices') -Universe TSE -Workers 6 -Qps 1.6 -VerboseCmd

  # 昨日→今日增量（prices/chip/macro_others）
  $y=(Get-Date).AddDays(-1).ToString('yyyy-MM-dd'); $t=(Get-Date).ToString('yyyy-MM-dd')
  .\scripts\ps\Invoke-FMBackfill.ps1 -Start $y -End $t -Datasets @('prices','chip','macro_others') -Workers 6 -Qps 1.6 -VerboseCmd

  # 資料驗證（若有 schema）
  if (Test-Path 'schemas\datasets_schema.yaml') {
    python scripts\validate_silver.py --schema schemas\datasets_schema.yaml --root G:\AI\tw-alpha-stack\datahub\silver\alpha --strict yes
  }

  # 冒煙測試
  .\scripts\ps\Run-SmokeTests.ps1
}
finally {
  Pop-Location
}
# === Post-run schema validation ===
& "$PSScriptRoot\..\..\.\.venv\Scripts\python.exe" "$PSScriptRoot\..\..\scripts\validate_silver.py" `
  --datahub-root "G:\AI\tw-alpha-stack\datahub" `
  --schema-path   "G:\AI\tw-alpha-stack\schemas\datasets_schema.yaml" `
  --report-csv    "G:\AI\tw-alpha-stack\metrics\validate_silver_report.csv" `
  --strict
