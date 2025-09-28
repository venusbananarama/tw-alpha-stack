#requires -Version 7.2
param(
  [string]$Since = "2015-01-01",
  [string[]]$Datasets = @("prices","chip","fund"),
  [switch]$DryRun
)
Import-Module "$PSScriptRoot/../../modules/AlphaCity.FinMind.psm1" -Force
Set-ACEncoding
Invoke-FMPlan -Since $Since -Datasets $Datasets -DryRun:$DryRun
