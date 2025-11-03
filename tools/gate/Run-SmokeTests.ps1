#requires -Version 7.0
[CmdletBinding()]
param([switch]$Quick)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# repo root = tools\gate 的上上層
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

function Find-First([string[]]$candidates){
  foreach($rel in $candidates){
    $p = Join-Path $repoRoot $rel
    if(Test-Path -LiteralPath $p){ return $p }
  }
  return $null
}

$tidy  = Find-First @('tools\tools\Tidy-Tools.ps1','tools\legacy\Tidy-Tools.ps1','tools\Tidy-Tools.ps1')
$apply = Find-First @('tools\tools\Apply-ToolsTidyPlan.ps1')
$guard = Find-First @('tools\tools\Assert-Preflight-Guard.ps1')

if(-not $tidy){ throw "Missing Tidy-Tools.ps1 (tried tools\tools, tools\legacy, tools)" }
if(-not $apply){ throw "Missing Apply-ToolsTidyPlan.ps1 (tools\tools)" }

Write-Host "[SMOKE] repoRoot=$repoRoot"
Write-Host "[SMOKE] tidy=$tidy"
Write-Host "[SMOKE] apply=$apply"

# 1) 產 plan
& $tidy

# 2) 讀最新 plan
$plan = Get-ChildItem (Join-Path $repoRoot 'reports') -Filter 'tools_tidy_plan_*.json' -EA SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $plan) { throw "No plan generated." }

# 3) 數量對齊檢查
$files = (Get-ChildItem -Recurse -File (Join-Path $repoRoot 'tools') -Filter *.ps1).Count
$plans = (Get-Content -LiteralPath $plan.FullName -Raw | ConvertFrom-Json).Count
Write-Host "[SMOKE] Counts files=$files plan=$plans"
if ($files -ne $plans) { throw "Plan count mismatch: FILES=$files PLAN=$plans" }

# 4) Apply（Dry-run + DoIt）
& $apply -PlanJson $plan.FullName
& $apply -PlanJson $plan.FullName -DoIt

# 5) manual CSV 應為 0
$latestManual = Get-ChildItem (Join-Path $repoRoot 'reports') -Filter 'tools_tidy_manual_*.csv' -EA SilentlyContinue |
                Sort-Object LastWriteTime -Descending | Select-Object -First 1
$manualRows = if ($latestManual) { @(Import-Csv -LiteralPath $latestManual.FullName | Where-Object { #requires -Version 7.0
[CmdletBinding()]
param([switch]$Quick)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# repo root = tools\gate 的上上層
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

function Find-First([string[]]$candidates){
  foreach($rel in $candidates){
    $p = Join-Path $repoRoot $rel
    if(Test-Path -LiteralPath $p){ return $p }
  }
  return $null
}

$tidy  = Find-First @('tools\tools\Tidy-Tools.ps1','tools\legacy\Tidy-Tools.ps1','tools\Tidy-Tools.ps1')
$apply = Find-First @('tools\tools\Apply-ToolsTidyPlan.ps1')
$guard = Find-First @('tools\tools\Assert-Preflight-Guard.ps1')

if(-not $tidy){ throw "Missing Tidy-Tools.ps1 (tried tools\tools, tools\legacy, tools)" }
if(-not $apply){ throw "Missing Apply-ToolsTidyPlan.ps1 (tools\tools)" }

Write-Host "[SMOKE] repoRoot=$repoRoot"
Write-Host "[SMOKE] tidy=$tidy"
Write-Host "[SMOKE] apply=$apply"

# 1) 產 plan
& $tidy

# 2) 讀最新 plan
$plan = Get-ChildItem (Join-Path $repoRoot 'reports') -Filter 'tools_tidy_plan_*.json' -EA SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
if (-not $plan) { throw "No plan generated." }

# 3) 數量對齊檢查
$files = (Get-ChildItem -Recurse -File (Join-Path $repoRoot 'tools') -Filter *.ps1).Count
$plans = (Get-Content -LiteralPath $plan.FullName -Raw | ConvertFrom-Json).Count
Write-Host "[SMOKE] Counts files=$files plan=$plans"
if ($files -ne $plans) { throw "Plan count mismatch: FILES=$files PLAN=$plans" }

# 4) Apply（Dry-run + DoIt）
& $apply -PlanJson $plan.FullName
& $apply -PlanJson $plan.FullName -DoIt

# 5) manual CSV 應為 0
$latestManual = Get-ChildItem (Join-Path $repoRoot 'reports') -Filter 'tools_tidy_manual_*.csv' -EA SilentlyContinue |
                Sort-Object LastWriteTime -Descending | Select-Object -First 1
$manualRows = if ($latestManual) { (Import-Csv $latestManual.FullName).Count } else { 0 }
Write-Host "[SMOKE] manual_rows=$manualRows"
if ($manualRows -gt 0) { throw "Manual list not empty: $manualRows" }

# 6) 可選：基礎檢查
if ($guard) { & $guard }

Write-Host "[SMOKE] PASS"
exit 0
 -ne $null }).Count } else { 0 }
Write-Host "[SMOKE] manual_rows=$manualRows"
if ($manualRows -gt 0) { throw "Manual list not empty: $manualRows" }

# 6) 可選：基礎檢查
if ($guard) { & $guard }

Write-Host "[SMOKE] PASS"
exit 0

