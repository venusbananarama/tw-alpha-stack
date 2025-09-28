#requires -Version 7.2
Set-StrictMode -Version Latest
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

Import-Module (Join-Path $PSScriptRoot 'AlphaCity.Common.psm1') -Force

function Invoke-FMPlan {
  [CmdletBinding(SupportsShouldProcess=$true)]
  param(
    [string]$Since = '2015-01-01',
    [string[]]$Datasets = @('prices','chip','fund'),
    [switch]$DryRun
  )
  $args = @('--since', $Since, '--datasets', ($Datasets -join ','))
  $script = "scripts/finmind_plan.py"
  if ($DryRun) {
    Write-Host "[DRYRUN] Plan FinMind → since=$Since, datasets=$($Datasets -join ',')"
    return
  }
  Invoke-ACPyscript -Script $script -Args $args
}

function Invoke-FMBackfill {
  [CmdletBinding(SupportsShouldProcess=$true)]
  param(
    [Parameter(Mandatory)][string]$Start,
    [Parameter(Mandatory)][string]$End,
    [Parameter(Mandatory)][string[]]$Datasets,
    [string]$Universe = "configs/universe.tw_all.txt",
    [switch]$DryRun
  )
  $args=@('--datasets',($Datasets -join ','),'--start',$Start,'--end',$End,'--universe',$Universe)
  $script = "scripts/finmind_backfill.py"
  if ($DryRun) {
    Write-Host "[DRYRUN] Backfill FinMind → $Start..$End, datasets=$($Datasets -join ','), uni=$Universe"
    return
  }
  Invoke-ACPyscript -Script $script -Args $args
}

function Invoke-FMDaily {
  [CmdletBinding(SupportsShouldProcess=$true)]
  param(
    [ValidateSet('eod','intraday')][string]$Mode='eod',
    [switch]$Strict,
    [switch]$DryRun
  )
  $args=@('--mode',$Mode)
  if($Strict){$args+='--strict'}
  $script = "scripts/finmind_daily_update.py"
  if ($DryRun) {
    Write-Host "[DRYRUN] Daily FinMind → mode=$Mode strict=$($Strict.IsPresent)"
    return
  }
  Invoke-ACPyscript -Script $script -Args $args
}

function Invoke-FMVerify {
  [CmdletBinding(SupportsShouldProcess=$true)]
  param(
    [string]$Since='2024-01-01',
    [string[]]$Datasets=@('prices','chip'),
    [switch]$DryRun
  )
  $args=@('--since',$Since,'--datasets',($Datasets -join ','))
  $script = "scripts/finmind_verify.py"
  if ($DryRun) {
    Write-Host "[DRYRUN] Verify FinMind → since=$Since, datasets=$($Datasets -join ',')"
    return
  }
  Invoke-ACPyscript -Script $script -Args $args
}

function Invoke-FMIntraday {
  [CmdletBinding(SupportsShouldProcess=$true)]
  param(
    [string[]]$Symbols,
    [int]$Seconds=5,
    [switch]$DryRun
  )
  $args=@('--symbols',($Symbols -join ','),'--interval',("${Seconds}s"))
  $script = "scripts/finmind_intraday_watch.py"
  if ($DryRun) {
    Write-Host "[DRYRUN] Intraday FinMind → symbols=$($Symbols -join ','), every=${Seconds}s"
    return
  }
  Invoke-ACPyscript -Script $script -Args $args
}

Export-ModuleMember -Function * -Alias *
