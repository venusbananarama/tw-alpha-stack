# Auto-generated wrapper: DO NOT EDIT
[CmdletBinding()]
param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args)
$target = Join-Path -Path $PSScriptRoot -ChildPath 'repair\Standardize-ProjectLayout.ps1'
if (-not (Test-Path -LiteralPath $target)) { throw "Target not found: $target" }
& $target @Args
