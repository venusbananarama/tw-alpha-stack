param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] Run-DailyBackfill.ps1 → daily\Run-DailyBackfill.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\daily\Run-DailyBackfill.ps1" @Rest
