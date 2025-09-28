# run_daily_etl_weekend_safe.ps1 — 避免週末/例假日造成一堆 EMPTY（UTF-8）
param(
  [string]$Root = "G:\AI\tw-alpha-stack",
  [int]$LookbackDays = 5  # 週末時用來擴大量，以涵蓋最後一個交易日
)

chcp 65001 > $null
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force

function Get-LastWeekday([datetime]$d) {
  while ($d.DayOfWeek -in 'Saturday','Sunday') { $d = $d.AddDays(-1) }
  return $d
}

Push-Location $Root
try {
  if (Test-Path ".\.venv\Scripts\Activate.ps1") { . .\.venv\Scripts\Activate.ps1 }

  # 決定增量區間（避開週末）：$T = 最近一個平日；$Y = T - LookbackDays
  $T = Get-LastWeekday (Get-Date)
  $Y = $T.AddDays(-1 * $LookbackDays)

  Write-Host "[INFO] Incremental range: $($Y.ToString('yyyy-MM-dd')) ~ $($T.ToString('yyyy-MM-dd'))"

  # 全市場（日價，分批）— 使用別名 'prices'
  .\scripts\ps\Invoke-FMAll.ps1 -Start 2015-01-01 -End ($T.ToString('yyyy-MM-dd')) -Datasets @('prices') -Universe TSE -Workers 6 -Qps 1.6 -VerboseCmd

  # 增量（prices/chip/macro_others）使用週末安全區間
  .\scripts\ps\Invoke-FMBackfill.ps1 -Start ($Y.ToString('yyyy-MM-dd')) -End ($T.ToString('yyyy-MM-dd')) -Datasets @('prices','chip','macro_others') -Workers 6 -Qps 1.6 -VerboseCmd

  # （選）資料驗證
  if (Test-Path 'schemas\datasets_schema.yaml') {
    python scripts\validate_silver.py --schema schemas\datasets_schema.yaml --root G:\AI\tw-alpha-stack\datahub\silver\alpha --strict yes
  }

  # 冒煙測試
  .\scripts\ps\Run-SmokeTests.ps1
}
finally {
  Pop-Location
}
