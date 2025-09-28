#requires -Version 7.2
param(
  [ValidateSet("eod","intraday")][string]$Mode = "eod",
  [switch]$Strict,
  [switch]$DryRun
)
Import-Module "$PSScriptRoot/../../modules/AlphaCity.FinMind.psm1" -Force
Import-Module "$PSScriptRoot/../../modules/AlphaCity.Benchmark.psm1" -Force
Import-Module "$PSScriptRoot/../../modules/AlphaCity.Report.psm1" -Force
Import-Module "$PSScriptRoot/../../modules/AlphaCity.Env.psm1" -Force
Set-ACEncoding

# Gating（簡化示意）
$diff = Compare-ACConstraints -Constraints "configs/env_constraints-311.txt"
if ($diff) { Write-Warning "Package diff found vs constraints. Consider reconciling before EOD." }

if ($Mode -eq "eod") {
  Write-Host "== EOD Flow =="
  Invoke-FMDaily -Mode eod -Strict:$Strict -DryRun:$DryRun
  Invoke-BenchmarkSync -DryRun:$DryRun
  Invoke-MakeReports -DryRun:$DryRun
} else {
  Write-Host "== Intraday Flow =="
  Invoke-FMDaily -Mode intraday -Strict:$Strict -DryRun:$DryRun
}
