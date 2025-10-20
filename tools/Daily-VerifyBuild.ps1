param([Parameter(ValueFromRemainingArguments=$true)][object[]]$Rest)
Write-Host "[shim] Daily-VerifyBuild.ps1 → devops\Daily-VerifyBuild.ps1" -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File "C:\AI\tw-alpha-stack\tools\devops\Daily-VerifyBuild.ps1" @Rest
