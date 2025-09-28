param([string]$Root=".", [switch]$AddToProfile)
$ErrorActionPreference="Stop"; Set-StrictMode -Version Latest
$root = (Resolve-Path $Root).Path
$scripts = Join-Path $root "scripts"
$tools   = Join-Path $root "tools"
New-Item -ItemType Directory -Force -Path $scripts | Out-Null
New-Item -ItemType Directory -Force -Path $tools   | Out-Null

Copy-Item "$PSScriptRoot\..\scripts\emit_metrics_v63_live.py" (Join-Path $scripts "emit_metrics_v63_live.py") -Force
Copy-Item "$PSScriptRoot\AckLive.ps1" (Join-Path $tools "AckLive.ps1") -Force
Write-Host "[OK] installed scripts/emit_metrics_v63_live.py and tools/AckLive.ps1"

if ($AddToProfile) {
  if (!(Test-Path $PROFILE)) { New-Item -ItemType File -Path $PROFILE -Force | Out-Null }
  $code = Get-Content (Join-Path $tools "AckLive.ps1") -Raw
  if (-not (Select-String -Path $PROFILE -Pattern 'function\s+acklive' -Quiet)) {
    Add-Content -Path $PROFILE -Value "`n# acklive (v63 live stream)`n$code`n"
    Write-Host "[OK] appended acklive to `$PROFILE"
  } else {
    Write-Host "[SKIP] `$PROFILE already has acklive"
  }
}