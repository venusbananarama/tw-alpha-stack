param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] Backfill-RatePlan.fast.ps1 → C:\AI\tw-alpha-stack\tools\fullmarket\Run-FullMarket-DateID-MaxRate.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\fullmarket\Run-FullMarket-DateID-MaxRate.ps1" @Rest
