param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] Run-WFGate.ps1 → gate\Run-WFGate.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\gate\Run-WFGate.ps1" @Rest
