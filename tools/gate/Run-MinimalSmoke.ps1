# === GUARD: prevent self-recursion / call depth overflow ===
if ($global:RUN_MINIMAL_SMOKE_ONCE) { return }
$global:RUN_MINIMAL_SMOKE_ONCE = $true
$PSDefaultParameterValues['Out-File:Encoding']='utf8'
# ================================================
# Auto-generated wrapper: DO NOT EDIT
[CmdletBinding()]
param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args)
$target = Join-Path -Path $PSScriptRoot -ChildPath 'Run-MinimalSmoke.ps1'
if(-not(Test-Path -LiteralPath $target)){throw "Target not found: $target"}
& $target @Args

