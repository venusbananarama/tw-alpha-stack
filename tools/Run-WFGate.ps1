if ($env:ALPHACITY_ALLOW -ne '1') { Write-Error 'ALPHACITY_ALLOW=1 not set.' -ErrorAction Stop }
$target = Join-Path $PSScriptRoot 'gate\Run-WFGate.ps1'
if (-not (Test-Path $target)) { throw 'Missing tools\gate\Run-WFGate.ps1' }
# 不用 param(...)，避免被 dot-source 時觸發 parser；直接把不明參數透傳
$args2 = @('-NoProfile','-ExecutionPolicy','Bypass','-File', $target) + $args
& pwsh @args2
exit $LASTEXITCODE
