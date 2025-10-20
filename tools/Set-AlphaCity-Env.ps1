param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] Set-AlphaCity-Env.ps1 → devops\Set-AlphaCity-Env.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\devops\Set-AlphaCity-Env.ps1" @Rest
