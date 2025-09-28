#requires -Version 7.2
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

param([switch]$DryRun)
Import-Module "$PSScriptRoot/../../modules/AlphaCity.CostModel.psm1" -Force
Set-ACEncoding
$cost = Get-ACCostModel
if ($DryRun) {
  "CapacityCheck (DRY) with impact.enabled={0}, k={1}, turnover_cap={2}" -f $cost.impact.enabled, $cost.impact.k, $cost.impact.turnover_cap | Write-Host
} else {
  Write-Host "CapacityCheck placeholder. Integrate with your Python backtester to compute capacity."
}
