# Auto-generated wrapper: DO NOT EDIT
[CmdletBinding()]
param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args)
$target = Join-Path -Path $PSScriptRoot -ChildPath 'orchestrator\Summarize-DateId-Progress.ps1'
if (-not (Test-Path -LiteralPath $target)) { throw "Target not found: $target" }
& $target @Args
