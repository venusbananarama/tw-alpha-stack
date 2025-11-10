#requires -Version 7
[CmdletBinding(PositionalBinding = $false)]
param(
  [Parameter(Mandatory)][string]$Start,
  [Parameter(Mandatory)][string]$End,
  [int]$BatchSize = 80,
  [string]$CheckpointRoot = ".\_state\fullmarket",
  [string]$RootPath = "C:\AI\tw-alpha-stack"
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
Set-Location $RootPath

Write-Host "FullMarket aggregate $Start → $End" -ForegroundColor Cyan

$ran = $false
$me = (Resolve-Path $MyInvocation.MyCommand.Path).Path
$cands = @(".\tools\fullmarket\Backfill-FullMarket.ps1",
           ".\tools\daily\Backfill-FullMarket.ps1",
           ".\Backfill-FullMarket.ps1") |
           Where-Object { Test-Path $_ } |
           ForEach-Object { (Resolve-Path $_).Path } |
           Where-Object { $_ -ne $me }

if($cands){
  $impl = $cands | Select-Object -First 1
  Write-Host "Delegating to: $impl" -ForegroundColor DarkCyan
  & pwsh -NoProfile -File $impl -Start $Start -End $End
  $ran = $true
} else {
  $py = @(".\.venv\Scripts\python.exe","python","py -3")
  $pyScript = ".\scripts\fullmarket_aggregate.py"
  foreach($p in $py){
    if(Test-Path $pyScript){
      try{
        & $p $pyScript --start $Start --end $End
        $ran = $true; break
      } catch { Write-Warning $_.Exception.Message }
    }
  }
}

if($ran){
  $perDir = ".\_state\ingest\per"; New-Item -ItemType Directory -Path $perDir -Force | Out-Null
  "" | Out-File -Encoding ascii -Force (Join-Path $perDir "$Start.ok")
  Write-Host "OK fullmarket checkpoint: $Start" -ForegroundColor Green
} else {
  Write-Warning "No fullmarket implementation found; created no output."
}
