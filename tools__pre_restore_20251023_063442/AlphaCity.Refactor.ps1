param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] AlphaCity.Refactor.ps1 → devops\AlphaCity.Refactor.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\devops\AlphaCity.Refactor.ps1" @Rest
