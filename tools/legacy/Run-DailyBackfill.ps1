# Auto-generated wrapper: DO NOT EDIT
[CmdletBinding()]
param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Args)
$tries = @('daily\Backfill-FullMarket.ps1', 'legacy\Run-DailyBackfill.ps1')
foreach ($rel in $tries) {
  $target = Join-Path -Path $PSScriptRoot -ChildPath $rel
  if (Test-Path -LiteralPath $target) { & $target @Args; exit $LASTEXITCODE }
}
throw "No target found. Tried: $($tries -join ", ")"
