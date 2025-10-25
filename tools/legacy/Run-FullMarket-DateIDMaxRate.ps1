#requires -Version 7

# if ($env:ALPHACITY_ALLOW -ne '1') { Write-Error 'ALPHACITY_ALLOW=1 not set. Aborting.' -ErrorAction Stop }
param([Parameter(ValueFromRemainingArguments=$true)] $Args)
$target = Join-Path $PSScriptRoot 'fullmarket\Run-FullMarket-DateID-MaxRate.ps1'
if(-not (Test-Path $target)){ throw "Target missing: $target" }
pwsh -NoProfile -ExecutionPolicy Bypass -File $target @Args

