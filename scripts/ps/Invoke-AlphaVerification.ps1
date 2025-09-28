param(
  [Parameter(Mandatory=$true)][string] $Start,
  [Parameter(Mandatory=$true)][string] $End,
  [switch] $SkipFull,
  [string] $Symbol,
  [int] $Workers = 6,
  [double] $Qps = 1.6,
  [string] $SummaryJsonPath = ".\metrics\verify_summary_latest.json",
  [string] $CalendarCsv,
  [int] $PhaseTimeoutMins = 10,
  [string] $Wrapper = ".\scripts\emit_metrics_v63.py",
  [string] $PythonExe = ".\.venv\Scripts\python.exe",
  [switch] $VerboseCmd
)

$ErrorActionPreference = "Stop"; Set-StrictMode -Version Latest
$root = (Get-Location).Path
$logs = Join-Path $root "logs"
$metricsDir = Join-Path $root "metrics"
New-Item -ItemType Directory -Force -Path $logs | Out-Null
New-Item -ItemType Directory -Force -Path $metricsDir | Out-Null

$ts = Get-Date -Format "yyyyMMdd-HHmmss"
$log = Join-Path $logs ("ack-v63-" + $ts + ".log")

$wrapperPath = Resolve-Path $Wrapper
$pyPath = Resolve-Path $PythonExe

# Build args
$argsList = @("--start", $Start, "--end", $End, "--summary-json-path", $SummaryJsonPath)
if ($SkipFull) { $argsList += "--skip-full" }
if ($Symbol)   { $argsList += @("--symbol", $Symbol) }
if ($CalendarCsv) { $argsList += @("--calendar-csv", $CalendarCsv) }
$argsList += @("--workers", $Workers, "--qps", $Qps)

$cmd = @($pyPath, "-X", "utf8", $wrapperPath) + $argsList
if ($VerboseCmd) { Write-Host ("[RUN] " + ($cmd -join " ")) }

try {
  & $pyPath -X utf8 $wrapperPath @argsList 2>&1 | Tee-Object -FilePath $log | Out-Host
  if (-not (Test-Path $SummaryJsonPath)) {
    $obj = [pscustomobject]@{
      status="FAIL"; reason="summary_not_found"; noop=$true; rows=0;
      meta=@{ log=$log; wrapper=(Get-Item $wrapperPath).FullName }
    }
    $obj | ConvertTo-Json -Depth 8 | Set-Content -Path $SummaryJsonPath -Encoding UTF8
  }
  Write-Host ("[OK] Summary → " + (Resolve-Path $SummaryJsonPath))
} catch {
  $err = $_.Exception.Message
  $obj = [pscustomobject]@{
    status="FAIL"; reason="wrapper_error"; noop=$true; rows=0;
    meta=@{ error=$err; log=$log; wrapper=(Get-Item $wrapperPath).FullName }
  }
  $obj | ConvertTo-Json -Depth 8 | Set-Content -Path $SummaryJsonPath -Encoding UTF8
  throw
}