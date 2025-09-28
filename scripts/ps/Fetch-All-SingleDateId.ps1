<#
.SYNOPSIS
  批量抓取「需要 date_id」的單股 API（全市場股票），支援斷點續跑、並行與速率限制。

.DESCRIPTION
  - 從 universe.csv 載入股票清單（需包含 'symbol' 欄）。
  - 對每個股票呼叫 Invoke-FMSingle.ps1 抓取多個需要 date_id 的資料集。
  - 以 Start-Job 控制並行度（Workers），並透過 -Qps 參數由底層指令限制速率。
  - 具備斷點續跑（ResumeLog），重跑時會跳過已成功的股票。

.PARAMETER DatahubRoot
  DataHub 根目錄（影響某些腳本依賴之相對路徑）。

.PARAMETER UniverseCsv
  股票池清單檔案路徑（CSV），需包含 'symbol' 欄位，建議由 build_universe.py 產出。

.PARAMETER Start
  抓取資料的起始日期（yyyy-MM-dd）。

.PARAMETER End
  抓取資料的結束日期（yyyy-MM-dd）。

.PARAMETER Workers
  併發工作數量（建議 4~8 視 QPS 與 API 限流而定）。

.PARAMETER Qps
  每秒請求上限，會傳遞給 Invoke-FMSingle.ps1，請依 API 規範設定。

.PARAMETER ResumeLog
  斷點續跑記錄檔路徑。格式：<symbol>|ok 或 <symbol>|fail|<message>

.EXAMPLE
  PS> .\scripts\ps\Fetch-All-SingleDateId.ps1 `
        -UniverseCsv 'G:\AI\tw-alpha-stack\datahub\universe.csv' `
        -Start '2015-01-01' -End (Get-Date).ToString('yyyy-MM-dd') `
        -Workers 6 -Qps 1.6

.NOTES
  需與 Invoke-FMSingle.ps1 位於同一個 ps 資料夾（$PSScriptRoot），或請調整路徑。
#>

[CmdletBinding()]
param(
  [Parameter(Mandatory=$false)]
  [string]$DatahubRoot = "G:\AI\tw-alpha-stack\datahub",

  [Parameter(Mandatory=$true)]
  [string]$UniverseCsv,

  [Parameter(Mandatory=$false)]
  [string]$Start = "2015-01-01",

  [Parameter(Mandatory=$false)]
  [string]$End = (Get-Date).ToString("yyyy-MM-dd"),

  [Parameter(Mandatory=$false)]
  [ValidateRange(1, 64)]
  [int]$Workers = 6,

  [Parameter(Mandatory=$false)]
  [ValidateRange(0.1, 10.0)]
  [double]$Qps = 1.6,

  [Parameter(Mandatory=$false)]
  [string]$ResumeLog = "G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# === 前置檢查 ===
if (-not (Test-Path $UniverseCsv)) {
  throw "UniverseCsv not found: $UniverseCsv"
}

# 確認 Invoke-FMSingle.ps1 存在
$InvokeSingle = Join-Path $PSScriptRoot "Invoke-FMSingle.ps1"
if (-not (Test-Path $InvokeSingle)) {
  throw "Missing script: $InvokeSingle (請確認與本檔在同一資料夾 scripts\ps\ )"
}

# 建立日誌資料夾
New-Item -ItemType Directory -Force -Path (Split-Path $ResumeLog) | Out-Null

# === 需要 date_id 的資料集清單 ===
# 可依需求增減
$Datasets = @(
  "TaiwanStockInstitutionalInvestorsBuySell",
  "TaiwanStockShareholding",
  "TaiwanStockMarginPurchaseShortSale",
  "TaiwanStockTotalMarginPurchaseShortSale",
  "TaiwanStockGovernmentBankBuySell",
  "TaiwanStockPER",
  "TaiwanStockPBR"
)

# === 載入股票池（去重、過濾空值） ===
try {
  $symbols = Import-Csv -Path $UniverseCsv |
    ForEach-Object { $_.symbol } |
    Where-Object { $_ -and $_.Trim() -ne '' } |
    Select-Object -Unique
} catch {
  throw "讀取 UniverseCsv 失敗：$UniverseCsv，錯誤：$($_.Exception.Message)"
}

if (-not $symbols -or $symbols.Count -eq 0) {
  throw "UniverseCsv 無有效 symbol 欄位資料：$UniverseCsv"
}

# === 斷點續跑：讀取已完成清單 ===
$done = @{}
if (Test-Path $ResumeLog) {
  Get-Content $ResumeLog | ForEach-Object {
    $parts = $_.Split('|')
    if ($parts.Length -ge 2 -and $parts[1] -eq 'ok') {
      $done[$parts[0]] = $true
    }
  }
}

Write-Host ("[INFO] symbols={0} datasets={1} start={2} end={3} workers={4} qps={5}" -f `
  $symbols.Count, $Datasets.Count, $Start, $End, $Workers, $Qps)

# === 併發抓取 ===
$jobs = @()
$submitted = 0
$skipped = 0

foreach ($sym in $symbols) {
  if ($done.ContainsKey($sym)) { $skipped++ ; continue }

  # throttle
  while ($jobs.Count -ge $Workers) {
    $finished = $jobs | Where-Object { $_.State -in 'Completed','Failed','Stopped' }
    foreach ($j in $finished) {
      $jobs = $jobs | Where-Object { $_.Id -ne $j.Id }
    }
    Start-Sleep -Milliseconds 200
  }

  $jobs += Start-Job -Name ("Fetch_{0}" -f $sym) -ScriptBlock {
    param($sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle)
    try {
      & $InvokeSingle -Symbol $sym -Datasets $Datasets -Start $Start -End $End -Qps $Qps -VerboseCmd | Out-Null
      Add-Content -Path $ResumeLog -Value ("{0}|ok" -f $sym)
    } catch {
      Add-Content -Path $ResumeLog -Value ("{0}|fail|{1}" -f $sym, $_.Exception.Message)
      throw
    }
  } -ArgumentList $sym,$Datasets,$Start,$End,$Qps,$ResumeLog,$InvokeSingle

  $submitted++
}

# 等待所有工作結束 + 彙總結果
while ($jobs.Count -gt 0) {
  $finished = $jobs | Where-Object { $_.State -in 'Completed','Failed','Stopped' }
  foreach ($j in $finished) {
    $jobs = $jobs | Where-Object { $_.Id -ne $j.Id }
  }
  Start-Sleep -Milliseconds 500
}

$totalOk = (Select-String -Path $ResumeLog -Pattern "\|ok$" -SimpleMatch -ErrorAction SilentlyContinue).Count
$totalFail = (Select-String -Path $ResumeLog -Pattern "\|fail\|" -SimpleMatch -ErrorAction SilentlyContinue).Count

Write-Host ("[DONE] 提交={0} 跳過(已完成)={1} OK={2} FAIL={3} | 履歷：{4}" -f `
  $submitted, $skipped, $totalOk, $totalFail, $ResumeLog)
