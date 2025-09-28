#requires -Version 7.2
param(
  [string[]]$Symbols,
  [int]$Seconds = 5,
  [switch]$DryRun
)
Import-Module "$PSScriptRoot/../../modules/AlphaCity.FinMind.psm1" -Force
Set-ACEncoding
Invoke-FMIntraday -Symbols $Symbols -Seconds $Seconds -DryRun:$DryRun
