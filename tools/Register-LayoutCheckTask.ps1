param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] Register-LayoutCheckTask.ps1 → devops\Register-LayoutCheckTask.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\devops\Register-LayoutCheckTask.ps1" @Rest
