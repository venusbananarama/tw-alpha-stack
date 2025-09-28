#requires -Version 7.2
Set-StrictMode -Version Latest
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

Import-Module (Join-Path $PSScriptRoot 'AlphaCity.Common.psm1') -Force

function Get-ACEnvReport {
  [CmdletBinding()]
  param(
    [string[]]$Pkgs = @('python','numpy','pandas','numba','llvmlite','bottleneck','pyarrow','vectorbt','pandas_ta','talib')
  )
  $py = Get-ACPython
  $pipJson = (& $py -m pip list --format json | ConvertFrom-Json) 2>$null
  $rows = @()
  foreach($p in $Pkgs){
    $nameVariants = @($p, ($p -replace '_','-'), ($p -replace '-','_'))
    $found = $null
    if ($pipJson) {
      $found = $pipJson | Where-Object { $_.name -in $nameVariants } | Select-Object -First 1
    }
    $v = $found.version
    if (-not $v -and $p -eq 'python') {
      $v = (& $py -c "import sys;print('.'.join(map(str,sys.version_info[:3])))")
    }
    $rows += [pscustomobject]@{ Package=$p; Version = ($v ? $v : 'n/a') }
  }
  $rows
}

function Compare-ACConstraints {
  [CmdletBinding()]
  param(
    [string]$Constraints = "configs/env_constraints-311.txt",
    [switch]$FailOnDiff
  )
  $repo = Get-ACRepoRoot
  $path = Join-Path $repo $Constraints
  if (-not (Test-Path $path)) {
    Write-Warning "Constraints file not found: $path (skip compare)"
    return @()
  }
  $want = Get-Content $path -Encoding utf8 | Where-Object { $_ -and $_ -notmatch '^\s*#' }
  $py = Get-ACPython
  $have = (& $py -m pip list --format freeze)
  $diff = Compare-Object -ReferenceObject $want -DifferenceObject $have
  if ($FailOnDiff -and $diff) { exit 2 }
  return $diff
}

Export-ModuleMember -Function * -Alias *
