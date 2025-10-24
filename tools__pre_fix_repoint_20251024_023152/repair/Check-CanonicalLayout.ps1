[CmdletBinding()]
param(
  [string]$Root = ".",
  [switch]$Json,
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Push-Location $Root
try {
  $result = [ordered]@{
    required_ok       = @()
    required_missing  = @()
    disallowed_found  = @()
    stray_scripts     = @()
    misplaced_items   = @()
    ok                = $false
  }

  # required directories
  $required = @("scripts","tools","tasks","schemas","cal","reports","metrics","datahub","src","configs","launchers")
  foreach ($dir in $required) {
    if (Test-Path ("./{0}" -f $dir)) { $result.required_ok += $dir } else { $result.required_missing += $dir }
  }

  # disallowed residuals in root
  $pkg = Get-ChildItem -Directory -Name -Filter "pkg*" -ErrorAction SilentlyContinue
  if ($pkg) { $result.disallowed_found += ($pkg | ForEach-Object { "$_/" }) }
  if (Test-Path "./out") { $result.disallowed_found += "out/" }
  if (Test-Path "./data/reports") { $result.disallowed_found += "data/reports/" }

  # stray scripts in root (not whitelisted)
  $whitelist = '^(QuickStart_.*|Check-FMStatus.*|AlphaCity\.Profile)$'
  $stray = @()
  $stray += Get-ChildItem -File -Name -Filter "*.ps1" | Where-Object { $_ -notmatch $whitelist }
  $stray += Get-ChildItem -File -Name -Filter "*.cmd" | Where-Object { $_ -notmatch $whitelist }
  $result.stray_scripts = $stray

  # misplaced items (ASCII-only checks to avoid encoding issues)
  if (Test-Path "./make_report_safe") { $result.misplaced_items += "make_report_safe/ (should be in scripts/reports/)" }
  Get-ChildItem -File -Name -Filter "install_tw_alpha_reporting*" -ErrorAction SilentlyContinue | ForEach-Object {
    $result.misplaced_items += ("{0} (should be in scripts/install/)" -f $_)
  }
  if (Test-Path "./Check-FMStatus.ps1") { $result.misplaced_items += "Check-FMStatus.ps1 (should be in tools/)" }
  if (Test-Path "./_env_current") { $result.misplaced_items += "_env_current (should be in configs/)" }

  $ok = ($result.required_missing.Count -eq 0 -and
         $result.disallowed_found.Count -eq 0 -and
         $result.stray_scripts.Count -eq 0 -and
         $result.misplaced_items.Count -eq 0)
  $result.ok = $ok

  if ($Json) {
    ($result | ConvertTo-Json -Depth 6)
  } else {
    if ($ok) { Write-Host "[OK] Canonical layout passed." -ForegroundColor Green }
    if ($result.required_missing.Count) { Write-Host "[MISS] Required missing:" -ForegroundColor Yellow; $result.required_missing | % { "  - $_" | Write-Host -ForegroundColor Yellow } }
    if ($result.disallowed_found.Count) { Write-Host "[BAD] Disallowed found:" -ForegroundColor Red; $result.disallowed_found | % { "  - $_" | Write-Host -ForegroundColor Red } }
    if ($result.stray_scripts.Count) { Write-Host "[WARN] Stray scripts in root:" -ForegroundColor Magenta; $result.stray_scripts | % { "  - $_" | Write-Host -ForegroundColor Magenta } }
    if ($result.misplaced_items.Count) { Write-Host "[FIX] Misplaced items:" -ForegroundColor Cyan; $result.misplaced_items | % { "  - $_" | Write-Host -ForegroundColor Cyan } }
  }

  if ($Strict) { if ($ok) { exit 0 } else { exit 1 } }
}
finally { Pop-Location }
