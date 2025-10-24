param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] Run-Backfill-All.ps1 → orchestrator\Run-Backfill-All.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\orchestrator\Run-Backfill-All.ps1" @Rest
