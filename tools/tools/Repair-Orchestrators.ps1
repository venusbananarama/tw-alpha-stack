#requires -Version 7.0
[CmdletBinding()]
param(
  [string]$SuspectCsv = (Get-ChildItem .\reports\audit_tools_suspects_*.csv | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName,
  [switch]$DoIt,
  [string]$ArchiveDir = (".\tools\_archive_{0}" -f (Get-Date -Format yyyyMMdd)),
  [string[]]$Categories = @('orchestrator','fullmarket','dateid'),
  [int]$MinScore = 3
)
$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
$root=(Resolve-Path .).Path

if(-not $SuspectCsv){ throw "No suspects CSV found." }
$sus = Import-Csv -LiteralPath $SuspectCsv
$targets = $sus | Where-Object { $_.Category -in $Categories -and [int]$_.Score -ge $MinScore }

if(-not $targets){ Write-Host "[REPAIR] No targets matching criteria. Nothing to do."; return }
if($DoIt -and -not (Test-Path -LiteralPath $ArchiveDir)){ New-Item -ItemType Directory -Force -Path $ArchiveDir | Out-Null }

function Pick-Best([string]$srcPath){
  $dir = Split-Path -Parent $srcPath
  $base = [IO.Path]::GetFileNameWithoutExtension($srcPath)
  $cands = Get-ChildItem -File -LiteralPath $dir -EA SilentlyContinue |
           Where-Object {
             $_.Name -match [Regex]::Escape($base) -and
             $_.Name -match '(final|autofix|rewrite|fix|reheader|bak|_bak|_fix|_rewrite)'
           }
  if(-not $cands){ return $null }
  $scored = foreach($c in $cands){
    $name = $c.Name.ToLowerInvariant()
    $score = 0
    if($name -match 'final'){    $score += 50 }
    if($name -match 'autofix'){  $score += 40 }
    if($name -match 'rewrite'){  $score += 35 }
    if($name -match 'fix'){      $score += 25 }
    if($name -match 'reheader'){ $score += 15 }
    if($name -match 'bak'){      $score += 10 }
    # 避免用 wrapper（幾乎一定不是實作）
    try {
      $txt = Get-Content -LiteralPath $c.FullName -Raw -EA SilentlyContinue
      if($txt -match 'Auto-generated wrapper'){ $score -= 100 }
    } catch { }
    [pscustomobject]@{Path=$c.FullName; Score=$score}
  }
  $best = $scored | Sort-Object Score -Descending | Select-Object -First 1
  return $best.Path
}

$log = New-Object System.Collections.ArrayList
foreach($t in $targets){
  try{
    $src = $t.Path
    if(-not (Test-Path -LiteralPath $src)){ [void]$log.Add([pscustomobject]@{action='skip-missing'; path=$src}); continue }
    $best = Pick-Best -srcPath $src
    if(-not $best){ [void]$log.Add([pscustomobject]@{action='skip-nocand'; path=$src}); continue }
    $act = 'plan-restore'
    if($DoIt){
      $bk = Join-Path $ArchiveDir ([IO.Path]::GetFileName($src))
      Copy-Item -LiteralPath $src -Destination $bk -Force
      Copy-Item -LiteralPath $best -Destination $src -Force
      $act = 'restore'
    }
    [void]$log.Add([pscustomobject]@{action=$act; path=$src; use=$best})
  } catch {
    [void]$log.Add([pscustomobject]@{action='error'; path=$t.Path; note=$_.Exception.Message})
  }
}

$repDir = Join-Path $root 'reports'
$ts = Get-Date -Format yyyyMMdd_HHmmss
$out = Join-Path $repDir ("repair_orchestrators_{0}.csv" -f $ts)
$log | Export-Csv -NoTypeInformation -Path $out
Write-Host ("[REPAIR] log -> {0}  (DoIt={1})" -f $out, $DoIt.IsPresent)