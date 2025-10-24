param()
$root = Split-Path -Parent $PSScriptRoot
$fm   = Join-Path $root 'fullmarket'
$canon= Join-Path $fm 'Run-FullMarket-DateID-MaxRate.ps1'
$alias= Join-Path $fm 'Run-FullMarket-DateIDMaxRate.ps1'

if ((Test-Path $canon) -and -not (Test-Path $alias)) {
  Set-Content -LiteralPath $alias -Encoding UTF8 -Value @(
    "param([Parameter(ValueFromRemainingArguments=`$true)] `$Args)"
    "`$engine = Join-Path `$PSScriptRoot 'Run-FullMarket-DateID-MaxRate.ps1'"
    "pwsh -NoProfile -ExecutionPolicy Bypass -File `$engine @Args"
  )
  Write-Host "✅ ensured alias: $alias"
}
else {
  Write-Host "ℹ️ alias ok: $alias"
}
