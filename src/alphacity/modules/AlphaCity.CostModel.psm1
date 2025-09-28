#requires -Version 7.2
Set-StrictMode -Version Latest
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

Import-Module (Join-Path $PSScriptRoot 'AlphaCity.Common.psm1') -Force

function Read-YamlFile {
  param([Parameter(Mandatory)][string]$Path)
  if (-not (Get-Command ConvertFrom-Yaml -ErrorAction SilentlyContinue)) {
    throw "ConvertFrom-Yaml not available. Please Install-Module powershell-yaml or update to PS 7.4+."
  }
  (Get-Content $Path -Raw -Encoding utf8) | ConvertFrom-Yaml
}

function Get-ACCostModel {
  [CmdletBinding()]
  param([string]$Path = "configs/costs.yaml")
  $repo = Get-ACRepoRoot
  $full = Join-Path $repo $Path
  if (-not (Test-Path $full)) { throw "Missing costs.yaml at $full" }
  $cfg = Read-YamlFile -Path $full
  # 基本校驗
  foreach($k in @('fees_bps','tax_bps','slippage_bps')){
    if ($null -eq $cfg.$k) { throw "costs.yaml missing '$k'" }
  }
  return $cfg
}

Export-ModuleMember -Function * -Alias *
