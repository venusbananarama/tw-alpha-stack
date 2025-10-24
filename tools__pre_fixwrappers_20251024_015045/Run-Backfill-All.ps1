[CmdletBinding(SupportsShouldProcess=$true, ConfirmImpact="Medium")]
param(
  [string]$RootPath = "C:\AI\tw-alpha-stack",
  [int]   $Batch = 400,
  [int]   $MaxRetries = 6,
  [int]   $Workers = 4,
  [switch]$FillGaps,
  [switch]$Force,
  [switch]$DryRun,
  [string]$Start = "",
  [string]$End   = ""
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$showVerbose = $PSBoundParameters.ContainsKey('Verbose')
Set-Location $RootPath
if (-not $env:ALPHACITY_ALLOW -or $env:ALPHACITY_ALLOW -ne '1') { throw "ALPHACITY_ALLOW != 1" }

$ts=(Get-Date).ToString('yyyyMMdd_HHmmss')
$reports = Join-Path $RootPath 'reports'
New-Item -ItemType Directory -Force -Path $reports | Out-Null
$runLog=Join-Path $reports ("run_backfill_all_{0}.log" -f $ts)

function RunLog([string]$m){
  $line = "[{0}] {1}" -f (Get-Date).ToString('s'), $m
  $line | Tee-Object -FilePath $runLog -Append | Out-Null
  if ($showVerbose){ Write-Host $line }
}

$PY='.\\.venv\\Scripts\\python.exe'
if (-not (Test-Path -LiteralPath $PY)) { RunLog "Python not found"; exit 3 }

RunLog "RUN preflight_check.py (before)"
& $PY .\scripts\preflight_check.py --rules .\rules.yaml --export .\reports --root . *>> $runLog
$pf = $null
try { $pf = Get-Content .\reports\preflight_report.json -Raw | ConvertFrom-Json } catch {}

$expect_date = $null
if ($pf -and $pf.meta -and $pf.meta.expect_date){ $expect_date=$pf.meta.expect_date }
elseif ($pf -and $pf.expect_date) { $expect_date=$pf.expect_date }
if ($End) { $expect_date=$End } # End 不含
if (-not $expect_date) { RunLog "Cannot determine expect_date"; exit 4 }
if ($pf) {
  RunLog ("PREFLIGHT BEFORE -> prices={0} chip={1} per={2} dividend={3}" -f `
    $pf.freshness.prices.max_date,$pf.freshness.chip.max_date,$pf.freshness.per.max_date,$pf.freshness.dividend.max_date)
}

$workerPath   = Join-Path $RootPath 'tools\Backfill-FullMarket.ps1'
if (-not (Test-Path -LiteralPath $workerPath)) { RunLog "Worker not found: $workerPath"; exit 2 }
$workerStdout = Join-Path $reports ("backfill_worker_stdout_{0}.log" -f $ts)
$workerStderr = Join-Path $reports ("backfill_worker_stderr_{0}.log" -f $ts)

$argList = @(
  '-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
  '-File', $workerPath,
  '-RootPath', $RootPath,
  '-Batch', "$Batch",
  '-MaxRetries', "$MaxRetries",
  '-Workers', "$Workers"
)
if ($FillGaps) { $argList += '-FillGaps' }
if ($Force)    { $argList += '-Force' }
if ($Start)    { $argList += @('-Start', $Start) }
if ($End)      { $argList += @('-End',   $End) }
if ($showVerbose) { $argList += '-Verbose' }

RunLog ("Calling worker: pwsh " + ($argList -join ' '))

if ($DryRun){
  RunLog "[DryRun] Skip invoking worker."
  $exitCode = 0
} else {
  $proc = Start-Process -FilePath 'pwsh' -ArgumentList $argList `
           -Wait -PassThru -NoNewWindow `
           -RedirectStandardOutput $workerStdout `
           -RedirectStandardError  $workerStderr
  $exitCode = $proc.ExitCode
  RunLog ("Worker exit code = {0}. Stdout -> {1}; Stderr -> {2}" -f $exitCode,(Split-Path $workerStdout -Leaf),(Split-Path $workerStderr -Leaf))
}

$workerSummaryFile = Get-ChildItem -Path $reports -Filter 'backfill_summary_*.json' |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1
$workerSummary = $null
if ($workerSummaryFile) { try { $workerSummary = Get-Content $workerSummaryFile.FullName -Raw | ConvertFrom-Json } catch {} }

$runSummary = [ordered]@{ run_ts=$ts; expect_date=$expect_date; worker_exit=$exitCode; worker_summary=$workerSummary }
$runSummaryFile = Join-Path $reports ("backfill_summary_run_{0}.json" -f $ts)
$runSummary | ConvertTo-Json -Depth 10 | Out-File -FilePath $runSummaryFile -Encoding utf8
RunLog ("Wrote run summary {0}" -f (Split-Path $runSummaryFile -Leaf))

RunLog "RUN preflight_check.py (after)"
& $PY .\scripts\preflight_check.py --rules .\rules.yaml --export .\reports --root . *>> $runLog
try {
  $pf2 = Get-Content .\reports\preflight_report.json -Raw | ConvertFrom-Json
  RunLog ("PREFLIGHT AFTER  -> prices={0} chip={1} per={2} dividend={3}" -f `
    $pf2.freshness.prices.max_date,$pf2.freshness.chip.max_date,$pf2.freshness.per.max_date,$pf2.freshness.dividend.max_date)
} catch {}

RunLog "Run-Backfill-All DONE"
if ($exitCode -ne 0) { exit $exitCode } else { exit 0 }
