param(
  [string]$Constraints = "configs/env_constraints-311.txt",
  [switch]$FailOnDiff
)

#requires -Version 7.2
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

Import-Module "$PSScriptRoot/../../modules/AlphaCity.Common.psm1" -Force
Import-Module "$PSScriptRoot/../../modules/AlphaCity.Env.psm1" -Force
Set-ACEncoding

$diff = Compare-ACConstraints -Constraints $Constraints -FailOnDiff:$FailOnDiff
if ($diff) {
  Write-Host "=== Differences found vs $Constraints ===" -ForegroundColor Yellow
  $diff | Format-Table -AutoSize
  if ($FailOnDiff) { exit 2 }
} else {
  Write-Host "No differences. ✅"
}
