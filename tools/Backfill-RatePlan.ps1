param([Parameter(ValueFromRemainingArguments=$true)] $Args)
Write-Host '[shim] Backfill-RatePlan.ps1 → .\\tools\\fullmarket\\Run-FullMarket-DateID-MaxRate.ps1' -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File ".\\tools\\fullmarket\\Run-FullMarket-DateID-MaxRate.ps1" @Args
