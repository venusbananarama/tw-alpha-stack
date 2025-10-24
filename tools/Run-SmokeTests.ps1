if ($env:ALPHACITY_ALLOW -ne '1') { Write-Error 'ALPHACITY_ALLOW=1 not set.' -ErrorAction Stop }
# 由根層 tools\ 轉呼 gate\Run-SmokeTests.ps1
$target = Join-Path $PSScriptRoot 'gate\Run-SmokeTests.ps1'
if(-not (Test-Path $target)){ throw "Missing gate\Run-SmokeTests.ps1" }
& pwsh -NoProfile -ExecutionPolicy Bypass -File $target @args
