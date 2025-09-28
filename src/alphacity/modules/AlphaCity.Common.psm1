#requires -Version 7.2
Set-StrictMode -Version Latest
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

<#
.SYNOPSIS
  AlphaCity 共用工具：編碼、日誌、Python 呼叫、檢查點/續傳。
#>

function Set-ACEncoding {
  [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
  $Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
  $PSDefaultParameterValues['*:Encoding'] = 'utf8'
  try { chcp 65001 | Out-Null } catch {}
}

function Get-ACRepoRoot {
  # modules/ → repo root
  return (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
}

function Get-ACPython {
  param([string]$RepoRoot = $(Get-ACRepoRoot))
  $venvPy = Join-Path $RepoRoot '.venv/Scripts/python.exe'
  if (Test-Path $venvPy) { return $venvPy } else { return 'python' }
}

function Write-ACLog {
  param(
    [Parameter(Mandatory)][ValidateSet('INFO','WARN','ERROR','DEBUG')] [string] $Level,
    [Parameter(Mandatory)][string] $Message
  )
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  Write-Host "[$ts][$Level] $Message"
}

function Invoke-ACPy {
  [CmdletBinding()] param(
    [Parameter(Mandatory)][string]$Code,
    [switch]$Quiet
  )
  $py = Get-ACPython
  if ($Quiet) {
    & $py -c $Code *> $null
  } else {
    & $py -c $Code
  }
  if ($LASTEXITCODE -ne 0) {
    throw "Python exited with code $LASTEXITCODE"
  }
}

function Invoke-ACPyscript {
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)][string]$Script,
    [string[]]$Args
  )
  $repo = Get-ACRepoRoot
  $full = Join-Path $repo $Script
  if (-not (Test-Path $full)) {
    throw "Python script not found: $full (repo=$repo)"
  }
  $py = Get-ACPython -RepoRoot $repo
  & $py $full @Args
  if ($LASTEXITCODE -ne 0) {
    throw "Python exited with code $LASTEXITCODE (script=$Script)"
  }
}

function Test-ACWorkDay {
  param([string]$Market='TWSE')
  # 簡化版：周一至周五視為交易日（不含國定假日）
  $d = Get-Date
  return ($d.DayOfWeek -notin @('Saturday','Sunday'))
}

Export-ModuleMember -Function * -Alias *
