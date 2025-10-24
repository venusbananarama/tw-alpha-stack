param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] Clean-Phase1-Rollback.ps1 → repair\Clean-Phase1-Rollback.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\repair\Clean-Phase1-Rollback.ps1" @Rest
