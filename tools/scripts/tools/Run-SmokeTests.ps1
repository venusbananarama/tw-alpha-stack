if ($env:ALPHACITY_ALLOW -ne '1') { Write-Error 'ALPHACITY_ALLOW=1 not set.' -ErrorAction Stop }
# 從 <caller>\tools\ 回到真正的 tools\Run-SmokeTests.ps1（上一層）
$toolsRoot = Split-Path $PSScriptRoot -Parent
$target    = Join-Path $toolsRoot 'Run-SmokeTests.ps1'
if(-not (Test-Path $target)){ throw "Bridge target missing: $target" }
& pwsh -NoProfile -ExecutionPolicy Bypass -File $target @args
