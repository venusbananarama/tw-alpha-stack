param(
    [Parameter(Mandatory=$false)][string]$Dir = "",
    [Parameter(Mandatory=$false)][switch]$WeeklyPreview,
    [switch]$NoPause
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root
function Get-LatestFolder([string]$base){
    if (-not (Test-Path $base)) { throw "Path not found: $base" }
    Get-ChildItem -Path $base -Directory | Sort-Object LastWriteTime -Descending | Select-Object -First 1
}
if ($Dir -and (Test-Path $Dir)) { $target = Get-Item $Dir } else {
    $base = Join-Path $root "out"
    $target = Get-LatestFolder -base $base
}
if (-not $target) { throw "No output folder found." }
$path = $target.FullName
Write-Host "Opening:" $path
if ($WeeklyPreview) {
    $sum = Join-Path $path "summary.txt"; $pre = Join-Path $path "preview.csv"
    if (Test-Path $sum) { Start-Process notepad.exe $sum }
    if (Test-Path $pre) { Start-Process $pre }
} else {
    $sum = Join-Path $path "summary_backtest.txt"; $nav = Join-Path $path "nav.csv"
    if (Test-Path $sum) { Start-Process notepad.exe $sum }
    if (Test-Path $nav) { Start-Process $nav }
}
Start-Process $path
if (-not $NoPause) { Pause }
