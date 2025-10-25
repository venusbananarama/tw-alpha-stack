#requires -Version 7

if ($env:ALPHACITY_ALLOW -ne '1') { Write-Error 'ALPHACITY_ALLOW=1 not set.' -ErrorAction Stop }
$anc = @(
  (Split-Path $PSScriptRoot -Parent),
  (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent),
  (Split-Path (Split-Path (Split-Path $PSScriptRoot -Parent) -Parent) -Parent)
) | Where-Object { $_ -and (Test-Path (Join-Path $_ 'tools')) }
$repo = $anc | Select-Object -First 1
if(-not $repo){ throw ("Bridge cannot locate repo root from: " + $PSScriptRoot) }
$cands = @(
  (Join-Path $repo 'tools\fullmarket\Run-FullMarket-DateID-MaxRange.ps1'),
  (Join-Path $repo 'tools\Run-FullMarket-DateID-MaxRange.ps1'),
  (Join-Path $repo 'tools\fullmarket\Run-FullMarket-DateID-MaxRate.ps1'),
  (Join-Path $repo 'tools\Run-FullMarket-DateID-MaxRate.ps1'),
  (Join-Path $repo 'tools\fullmarket\Run-FullMarket-DateIDMaxRate.ps1'),
  (Join-Path $repo 'tools\Run-FullMarket-DateIDMaxRate.ps1')
)
$target = $cands | Where-Object { Test-Path $_ } | Select-Object -First 1
if(-not $target){
  $list = ($cands | ForEach-Object { "  - $_" }) -join "`n"
  throw ("Bridge target missing, checked:`n{0}" -f $list)
}
& pwsh -NoProfile -ExecutionPolicy Bypass -File $target @args

