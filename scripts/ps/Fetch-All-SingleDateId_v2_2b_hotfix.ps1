param(
  [string]$UniverseCsv,
  [string]$SymbolsTxt,
  [string[]]$Datasets,
  [string]$Start = '2015-01-01',
  [string]$End   = ((Get-Date).ToString('yyyy-MM-dd')),
  [int]$ThrottleLimit = 4,
  [double]$QpsPerWorker = 0.04,
  [double]$MaxRps = 0.16,
  [string]$ApiToken,
  [switch]$ForceResumeReset,
  [int]$MaxSymbols = 0,
  [switch]$Sequential,
  [string]$ResumeLog = 'G:\AI\tw-alpha-stack\metrics\fetch_single_dateid.log',
  [string]$LogsDir   = 'G:\AI\tw-alpha-stack\metrics\single_logs'
)

if (-not $UniverseCsv -and -not $SymbolsTxt) {
  throw "必須至少指定 -UniverseCsv 或 -SymbolsTxt"
}

Write-Host "[INFO] symbols loader ready."
