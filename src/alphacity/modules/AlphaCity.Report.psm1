#requires -Version 7.2
Set-StrictMode -Version Latest
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

Import-Module (Join-Path $PSScriptRoot 'AlphaCity.Common.psm1') -Force
Import-Module (Join-Path $PSScriptRoot 'AlphaCity.CostModel.psm1') -Force

function Read-YamlFile {
  param([Parameter(Mandatory)][string]$Path)
  if (-not (Get-Command ConvertFrom-Yaml -ErrorAction SilentlyContinue)) {
    throw "ConvertFrom-Yaml not available. Please Install-Module powershell-yaml or update to PS 7.4+."
  }
  (Get-Content $Path -Raw -Encoding utf8) | ConvertFrom-Yaml
}

function Invoke-MakeReports {
  [CmdletBinding(SupportsShouldProcess=$true)]
  param(
    [string]$ReportCfg = "configs/report.yaml",
    [switch]$DryRun
  )
  $repo = Get-ACRepoRoot
  $reportPath = Join-Path $repo $ReportCfg
  if (-not (Test-Path $reportPath)) { throw "Missing report.yaml at $reportPath" }

  $cost = Get-ACCostModel
  $cfg = Read-YamlFile -Path $reportPath

  Write-Host "=== Report Plan (v3) ==="
  "Costs → fees={0}bps tax={1}bps slip={2}bps impact={3}" -f $cost.fees_bps, $cost.tax_bps, $cost.slippage_bps, ($cost.impact.enabled) | Write-Host
  "Benchmarks → {0}" -f (($cfg.benchmarks | ForEach-Object {$_.id}) -join ', ') | Write-Host
  "Peers      → {0}" -f (($cfg.peers     | ForEach-Object {$_.code}) -join ', ') | Write-Host
  "Metrics    → {0}" -f ($cfg.metrics -join ', ') | Write-Host
  "Thresholds → {0}" -f ($cfg.thresholds | ConvertTo-Json -Compress) | Write-Host

  if ($DryRun) { return }

  Write-Host "NOTE: This is a skeleton. Implement your Python report generator and call it here via Invoke-ACPyscript."
}

Export-ModuleMember -Function * -Alias *
