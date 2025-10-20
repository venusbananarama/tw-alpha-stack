param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] Tune-S1.ps1 → orchestrator\Tune-S1.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\orchestrator\Tune-S1.ps1" @Rest
