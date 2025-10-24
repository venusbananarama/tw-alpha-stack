param([Parameter(ValueFromRemainingArguments=$true)] $Args)
Write-Host '[shim] OneShot-Phase1.ps1 → .\\tools\\Run-WFGate.ps1' -ForegroundColor Yellow
pwsh -NoProfile -ExecutionPolicy Bypass -File ".\\tools\\Run-WFGate.ps1" @Args
