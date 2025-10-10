param(
  [string]$Asof = (Get-Date -Format 'yyyy-MM-dd'),
  [switch]$NoOptional = $false,
  [switch]$DryRun = $false
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info($msg) { Write-Host "[INFO] $msg" -ForegroundColor Cyan }
function Write-Warn($msg) { Write-Warning $msg }
function Fail($msg) { throw $msg }

# --- Required docs (6) ---
$requiredDocs = @(
  "docs/SSOT_master_{0}.md",
  "docs/changelog_{0}.md",
  "docs/page_index_{0}.json",
  "docs/gate_checklist_{0}.md",
  "docs/ssot_keys_{0}.yaml",
  "docs/patch_suggestions_{0}.yaml"
) | ForEach-Object { $_ -f $Asof }

$missing = @()
foreach ($f in $requiredDocs) {
  if (-not (Test-Path -LiteralPath $f)) { $missing += $f }
}
$pf = "reports/preflight/preflight_$Asof.json"
if (-not (Test-Path -LiteralPath $pf)) { $missing += $pf }

if ($missing.Count -gt 0) {
  $list = ($missing | ForEach-Object { " - $_" }) -join "`n"
  Fail "Missing required files:`n$list"
}

# --- Snapshot rules.yaml ---
$rules = "configs/rules.yaml"
if (-not (Test-Path -LiteralPath $rules)) { Fail "Missing $rules" }
$hash8 = (Get-FileHash -LiteralPath $rules -Algorithm SHA256).Hash.Substring(0,8)

$snaps = "configs/snapshots"
New-Item -ItemType Directory -Path $snaps -Force | Out-Null
$newSnap = Join-Path $snaps ("rules_{0}_{1}.yaml" -f $Asof, $hash8)
Copy-Item -LiteralPath $rules -Destination $newSnap -Force
Write-Info ("Snapshot created: {0}" -f $newSnap)

# --- Optional includes (scorecard + factor evals) ---
$opt = @()
if (-not $NoOptional) {
  $score = "reports/polaris/scorecard_$Asof.json"
  if (Test-Path -LiteralPath $score) { $opt += $score }
  if (Test-Path -LiteralPath "reports/factors") {
    $factorEvals = Get-ChildItem -Path "reports/factors" -Recurse -Filter ("eval_{0}.json" -f $Asof) -ErrorAction SilentlyContinue
    if ($factorEvals) { $opt += ($factorEvals | ForEach-Object { $_.FullName }) }
  }
}

# --- Manifest (handoff) ---
$manifest = [ordered]@{
  project = "AlphaCity / 代號4"
  asof = $Asof
  doc_version = ("SSOT_master_{0}.md" -f $Asof)
  rules = @{ path = "configs/rules.yaml"; sha256_8 = $hash8 }
  preflight = $pf
  polaris_scorecard = (Test-Path -LiteralPath ("reports/polaris/scorecard_{0}.json" -f $Asof)) ? ("reports/polaris/scorecard_{0}.json" -f $Asof) : $null
  factor_evals = @(
    $opt | Where-Object { $_ -like "*reports\\factors*eval_*.json" } | ForEach-Object {
      $_ -replace [regex]::Escape((Get-Location).Path + [IO.Path]::DirectorySeparatorChar), ""
    }
  )
  changes = @{ DR=@{}; SYS=@{}; PATH=@{}; CMD=@{}; PH=@{}; GATE=@{}; OPS=@{}; TRB=@{}; APP=@{} }
  next_actions = @()
}
$maniPath = "docs/manifest_{0}.json" -f $Asof
$manifest | ConvertTo-Json -Depth 8 | Set-Content -Path $maniPath -Encoding UTF8
Write-Info ("Manifest written: {0}" -f $maniPath)

# --- Package ---
$zip = "SSOT_monthly_{0}.zip" -f $Asof
$paths = @("docs","configs/snapshots",$pf) + $opt
$paths = $paths | ForEach-Object { $_.ToString() } | Select-Object -Unique

if ($DryRun) {
  Write-Host "[DRY-RUN] Would create $zip with:" -ForegroundColor Yellow
  $paths | ForEach-Object { Write-Host "  - $_" }
  Write-Host "[DRY-RUN] Done."
  exit 0
}

if (Test-Path -LiteralPath $zip) { Remove-Item -LiteralPath $zip -Force }
Compress-Archive -Path $paths -DestinationPath $zip -Force
Write-Host ("Done: {0}" -f $zip) -ForegroundColor Green
Write-Host ("Snapshot: {0}" -f $newSnap)
Write-Host ("Manifest: {0}" -f $maniPath)
