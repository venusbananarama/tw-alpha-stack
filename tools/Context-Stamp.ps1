param([Parameter(ValueFromRemainingArguments=$true)] $Args)
Write-Host '[shim] Context-Stamp.ps1 → .\\tools\\devops\\Test-AlphaEnv.ps1' -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File ".\\tools\\devops\\Test-AlphaEnv.ps1" @Args
