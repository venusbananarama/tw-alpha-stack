
# Install-Phase2-FactorTools.ps1
# One-click installer: creates Phase-2 folder structure and core scripts (no wrappers, direct calls).
param(
  [string]$Root = "C:\AI\tw-alpha-stack"
)
$ErrorActionPreference = 'Stop'

function New-Dir($p) { if(!(Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null } }

Set-Location $Root
if(!(Test-Path ".\tools")) { throw "Not at repo root. tools\ folder not found: $Root" }

# Create folders
$dirs = @(
  ".\tools\factors",
  ".\tools\factors\eval",
  ".\tools\factors\corr",
  ".\tools\factors\combo",
  ".\tools\factors\status"
)
$dirs | ForEach-Object { New-Dir $_ }

# --- Write scripts ---
$ParamBlock = @'
param(
  [string]$Date,
  [ValidateSet('W-FRI')] [string]$Align='W-FRI',
  [ValidateSet('strict')] [string]$LagRule='strict',
  [int[]]$Windows=@(6,12,24),
  [string[]]$Families=@('tech','chip'),
  [string[]]$Neutralize=@('size','industry'),
  [int]$TopN=20,
  [double]$MaxWeightPct=0.05,
  [int]$GuardAdv=5,
  [string]$Output = ".\reports",
  [switch]$Quiet
)
$ErrorActionPreference='Stop'

'@

# Helper to write file if missing or to refresh content
function Write-File($Path, $Content) {
  $dir = Split-Path $Path -Parent
  New-Dir $dir
  Set-Content -Path $Path -Value $Content -Encoding UTF8
  Write-Host "Wrote $Path"
}

# Eval
$EvalScript = @'
param(
  [string]$Date,
  [ValidateSet('W-FRI')] [string]$Align='W-FRI',
  [ValidateSet('strict')] [string]$LagRule='strict',
  [int[]]$Windows=@(6,12,24),
  [string[]]$Families=@('tech','chip'),
  [string[]]$Neutralize=@('size','industry'),
  [int]$TopN=20,
  [double]$MaxWeightPct=0.05,
  [int]$GuardAdv=5,
  [string]$Output = ".\reports",
  [switch]$Quiet
)
$ErrorActionPreference='Stop'

# Directly call python eval (no wrappers).
$python   = ".\.venv\Scripts\python.exe"
$py_eval  = ".\scripts\factor_eval.py"
if(!(Test-Path $python)) { throw "Python venv not found: $python" }
if(!(Test-Path $py_eval)) { throw "Missing script: $py_eval" }

$familiesArg   = $Families -join ' '
$neutralArg    = $Neutralize -join ' '
$windowsArg    = $Windows -join ' '

$cmd = @(
  $py_eval,
  "--date", $Date,
  "--align", $Align,
  "--lag-rule", $LagRule,
  "--families", $familiesArg,
  "--neutralize", $neutralArg,
  "--wf-windows", $windowsArg,
  "--output", $Output
)
& $python @cmd
'@
Write-File ".\tools\factors\eval\Run-FactorEval.ps1" $EvalScript

# Corr
$CorrScript = @'
param(
  [string]$Date,
  [ValidateSet('W-FRI')] [string]$Align='W-FRI',
  [ValidateSet('strict')] [string]$LagRule='strict',
  [int[]]$Windows=@(6,12,24),
  [string[]]$Families=@('tech','chip'),
  [string[]]$Neutralize=@('size','industry'),
  [int]$TopN=20,
  [double]$MaxWeightPct=0.05,
  [int]$GuardAdv=5,
  [string]$Output = ".\reports",
  [switch]$Quiet
)
$ErrorActionPreference='Stop'

$python   = ".\.venv\Scripts\python.exe"
$py_corr  = ".\scripts\factor_corr.py"
if(!(Test-Path $python)) { throw "Python venv not found: $python" }
if(!(Test-Path $py_corr)) { throw "Missing script: $py_corr" }

$familiesArg = $Families -join ' '
$cmd = @(
  $py_corr,
  "--date", $Date,
  "--families", $familiesArg,
  "--output", $Output
)
& $python @cmd
'@
Write-File ".\tools\factors\corr\Run-FactorCorr.ps1" $CorrScript

# Combo
$ComboScript = @'
param(
  [string]$Date,
  [ValidateSet('W-FRI')] [string]$Align='W-FRI',
  [ValidateSet('strict')] [string]$LagRule='strict',
  [int[]]$Windows=@(6,12,24),
  [string[]]$Families=@('tech','chip'),
  [string[]]$Neutralize=@('size','industry'),
  [int]$TopN=20,
  [double]$MaxWeightPct=0.05,
  [int]$GuardAdv=5,
  [string]$Output = ".\reports",
  [switch]$Quiet
)
$ErrorActionPreference='Stop'

$python    = ".\.venv\Scripts\python.exe"
$py_combo  = ".\scripts\factor_combo.py"
if(!(Test-Path $python)) { throw "Python venv not found: $python" }
if(!(Test-Path $py_combo)) { throw "Missing script: $py_combo" }

$familiesArg = $Families -join ' '
$neutralArg  = $Neutralize -join ' '
$windowsArg  = $Windows -join ' '

$cmd = @(
  $py_combo,
  "--date", $Date,
  "--families", $familiesArg,
  "--neutralize", $neutralArg,
  "--topn", $TopN,
  "--max-weight-pct", ("{0:N2}" -f $MaxWeightPct),
  "--guard-adv", $GuardAdv,
  "--align", $Align,
  "--lag-rule", $LagRule,
  "--wf-windows", $windowsArg,
  "--output", $Output
)
& $python @cmd
'@
Write-File ".\tools\factors\combo\Run-FactorCombo.ps1" $ComboScript

# Status (read-only viewer)
$StatusScript = @'
# Show-FactorStatus.ps1
param(
  [string]$Date,
  [string]$Reports = ".\reports"
)
$ErrorActionPreference='Stop'
function Exists($p) { if(Test-Path $p) { $true } else { $false } }
$wf  = Join-Path $Reports "wf_summary.json"
$gate= Join-Path $Reports "gate_summary.json"
$sel = Join-Path $Reports "selection_topn.csv"
Write-Host "== Phase-2 Status =="
if(Exists $wf)   { Write-Host "[wf_summary] " (Get-Item $wf).LastWriteTime; try { (Get-Content $wf -Raw | ConvertFrom-Json).wf | Format-List } catch {} }
if(Exists $gate) { Write-Host "[gate_summary]" (Get-Item $gate).LastWriteTime; try { (Get-Content $gate -Raw | ConvertFrom-Json).overall | Format-List } catch {} }
if(Exists $sel)  { Write-Host "[selection_topn.csv]" ; Import-Csv $sel | Select-Object -First 10 | Format-Table -AutoSize }
'@
Write-File ".\tools\factors\status\Show-FactorStatus.ps1" $StatusScript

# README
$Readme = @'
# Phase-2 Factor Tools (No wrappers, single Gate)

## Entry points
- `tools\factors\eval\Run-FactorEval.ps1`
- `tools\factors\corr\Run-FactorCorr.ps1`
- `tools\factors\combo\Run-FactorCombo.ps1`
- `tools\factors\status\Show-FactorStatus.ps1`

## Typical flow
```
pwsh -NoProfile -File .\tools\factors\eval\Run-FactorEval.ps1 -Date 2025-11-07 -Families tech,chip
pwsh -NoProfile -File .\tools\factors\corr\Run-FactorCorr.ps1 -Date 2025-11-07 -Families tech,chip
pwsh -NoProfile -File .\tools\factors\combo\Run-FactorCombo.ps1 -Date 2025-11-07 -TopN 20 -MaxWeightPct 0.05 -GuardAdv 5
pwsh -NoProfile -File .\tools\gate\Run-WFGate.ps1 -WFDir .\tools\gate\wf_configs
```
'@
Write-File ".\tools\factors\README_factor_tools.md" $Readme

# YAML template (configs\factors.template.yaml)
$factorsYaml = @'
meta:
  profile: phase2
  align_to: W-FRI
  lag_rule: {{ D: "T+1 close", W: "W-FRI close" }}
  cost_model: {{ fee_bps: 8, tax_bps: 30, slip_bps: 10, impact: square_root }}
  wf_windows: [6, 12, 24]
  acceptance:
    rank_ic_min: 0.03
    corr_abs_max: 0.7
    wf_pass_rate_min: 0.80
    guards: {{ psr_min: 0.9, t_min: 2.0, dsr_after_costs_gt: 0, replay_mae_bps_max: 2 }}
portfolio:
  topn_default: 20
  max_weight_pct: 0.05
  capacity_guards: {{ max_adv_participation_pct: 5, single_name_weight_cap_pct: 5 }}

factors:
  - name: mtf_macd_reloaded_12_26_9
    alias: MTF-MACD-R
    family: tech
    source: prices
    freq: W
    window: {{ fast: 12, slow: 26, signal: 9 }}
    transform: [winsor_3sigma, zscore_by_date]
    neutralize: [size, industry]
    scorer: [rank_ic, ic_t, turnover, capacity, after_costs]
    enabled: true
  - name: chip_foreign_netbuy_5d
    family: chip
    source: chip
    freq: W
    window: {{ span_d: 5 }}
    transform: [winsor_3sigma, zscore_by_date]
    neutralize: [size]
    scorer: [rank_ic, turnover, capacity]
    enabled: true
'@
Write-File ".\configs\factors.template.yaml" $factorsYaml

Write-Host "Phase-2 factor tools installed at $Root"
