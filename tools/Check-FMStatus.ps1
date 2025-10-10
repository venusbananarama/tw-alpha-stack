Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# File: Check-FMStatus.ps1
param([int]$Tail = 10)

Write-Host "=== [FinMind] Backfill Status ==="

$lastFile = Get-ChildItem -Path "data\finmind\raw" -Recurse -Include *.parquet `
           | Sort-Object LastWriteTime -Descending `
           | Select-Object -First 1

if ($lastFile) {
    Write-Host "Last file:" $lastFile.FullName
    Write-Host "Last modified:" $lastFile.LastWriteTime
} else {
    Write-Host "No parquet files found."
}

Write-Host "`n-- Latest files --"
Get-ChildItem -Path "data\finmind\raw" -Recurse -Include *.parquet `
    | Sort-Object LastWriteTime -Descending | Select-Object -First $Tail `
    | ForEach-Object { "{0}  {1}" -f $_.LastWriteTime.ToString("u"), $_.FullName }

