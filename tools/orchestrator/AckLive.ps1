Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
function acklive {
  param(
    [Parameter(Mandatory=$true)][string]$Start,
    [Parameter(Mandatory=$true)][string]$End,
    [string]$Symbol,
    [int]$Workers = 6,
    [double]$Qps = 1.6,
    [string]$CalendarCsv = ".\cal\trading_days.csv",
    [string[]]$Datasets = @('TaiwanStockPrice','TaiwanStockInstitutionalInvestorsBuySell'),
    [string]$Universe = 'TSE',
    [string]$PythonExe = ".\.venv\Scripts\python.exe",
    [string]$FinmindScript = ".\scripts\finmind_backfill.py",
    [switch]$NoFsScan
  )
  $args = @(
    '--start', $Start, '--end', $End,
    '--workers', $Workers, '--qps', $Qps,
    '--python-exe', $PythonExe, '--finmind-script', $FinmindScript,
    '--universe', $Universe
  )
  if ($Symbol)       { $args += @('--symbol', $Symbol) }
  if ($Datasets)     { $args += @('--datasets'); $args += $Datasets }
  if ($CalendarCsv)  { $args += @('--calendar-csv', $CalendarCsv) }
  if ($NoFsScan)     { $args += '--no-fs-scan' }
  & ".\.venv\Scripts\python.exe" -u -X utf8 ".\scripts\emit_metrics_v63_live.py" @args
}
