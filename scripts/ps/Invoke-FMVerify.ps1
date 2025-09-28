#requires -Version 7.2
param(
  [string]$Since = "2024-01-01",
  [string[]]$Datasets = @("prices","chip"),
  [switch]$DryRun
)
Import-Module "$PSScriptRoot/../../modules/AlphaCity.FinMind.psm1" -Force
Set-ACEncoding
Invoke-FMVerify -Since $Since -Datasets $Datasets -DryRun:$DryRun
