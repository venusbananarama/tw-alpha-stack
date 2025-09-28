param([string]$Root=".", [switch]$RemoveScript)
$ErrorActionPreference="Stop"; Set-StrictMode -Version Latest
$root = (Resolve-Path $Root).Path
if ($RemoveScript) {
  $p = Join-Path $root "scripts\emit_metrics_v63_live.py"
  if (Test-Path $p) { Remove-Item $p -Force; Write-Host "[OK] removed $p" }
}
if (Test-Path $PROFILE) {
  $bak = "$PROFILE.bak_" + (Get-Date -Format "yyyyMMdd-HHmmss")
  Copy-Item $PROFILE $bak -Force
  $content = Get-Content $PROFILE -Raw
  $content2 = $content -replace '(?s)#\s*acklive\s*\(v63 live stream\).*?function\s+acklive.*?}\s*', ''
  if ($content2 != $content) {
    Set-Content $PROFILE -Value $content2 -Encoding UTF8
    Write-Host "[OK] removed acklive from `$PROFILE. Backup: $bak"
  } else {
    Write-Host "[SKIP] acklive not found in `$PROFILE"
  }
}