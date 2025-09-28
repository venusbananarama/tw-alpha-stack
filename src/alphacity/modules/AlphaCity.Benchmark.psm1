#requires -Version 7.2
Set-StrictMode -Version Latest
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

Import-Module (Join-Path $PSScriptRoot 'AlphaCity.Common.psm1') -Force

function Read-YamlFile {
  param([Parameter(Mandatory)][string]$Path)
  # 依賴 PowerShell 7 的 ConvertFrom-Yaml（內建在 PS 7.4+ / Microsoft.PowerShell.Utility >=7.3）
  if (-not (Get-Command ConvertFrom-Yaml -ErrorAction SilentlyContinue)) {
    throw "ConvertFrom-Yaml not available. Please Install-Module powershell-yaml or update to PS 7.4+."
  }
  $text = Get-Content $Path -Raw -Encoding utf8
  return $text | ConvertFrom-Yaml
}

function Invoke-BenchmarkSync {
  [CmdletBinding(SupportsShouldProcess=$true)]
  param([switch]$DryRun)

  $repo = Get-ACRepoRoot
  $benchPath = Join-Path $repo 'configs/benchmarks.yaml'
  $peersPath = Join-Path $repo 'configs/peers.yaml'
  if (-not (Test-Path $benchPath)) { throw "Missing $benchPath" }
  if (-not (Test-Path $peersPath)) { throw "Missing $peersPath" }

  $bench = Read-YamlFile -Path $benchPath
  $peers = Read-YamlFile -Path $peersPath

  Write-Host "=== Benchmark Plan ==="
  foreach($idx in $bench.indices){
    "{0} ← provider={1} freq={2}" -f $idx.id, $idx.provider, $idx.freq | Write-Host
  }
  Write-Host "=== Peers Plan ==="
  foreach($p in $peers.peers){
    "{0} ← TER={1}% src={2}" -f $p.code, $p.fee_total_expense_ratio, $p.nav_source | Write-Host
  }

  if ($DryRun) { return }

  # 實際同步留白：依據提供的 nav_source / provider 由 Python 或下載器實作。
  Write-Host "NOTE: This is a skeleton. Implement actual fetchers in your Python or separate PS fetchers."
}

Export-ModuleMember -Function * -Alias *
