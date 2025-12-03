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
