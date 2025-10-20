param([Parameter(ValueFromRemainingArguments=$true)] $Args)
$engine=Join-Path $PSScriptRoot 'Run-FullMarket-DateID-MaxRate.ps1'
pwsh -NoProfile -ExecutionPolicy Bypass -File $engine @Args
