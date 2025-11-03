param([Parameter(ValueFromRemainingArguments=$true)] $Args)
$target = Join-Path $PSScriptRoot 'gate\Run-WFGate.ps1'
if(Test-Path $target){ pwsh -NoProfile -ExecutionPolicy Bypass -File $target @Args } else { throw "Missing $target" }