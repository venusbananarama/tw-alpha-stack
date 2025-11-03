#requires -Version 7.0
[CmdletBinding()]
param([Parameter(Mandatory=$true)][string]$PlanJson,[switch]$DoIt,[bool]$KeepTopWrappers=$true,[string[]]$WrapperWhitelist=@())
$ErrorActionPreference='Stop'
$root=(Resolve-Path .).Path
$plan=Get-Content -LiteralPath $PlanJson -Raw|ConvertFrom-Json
$todo=@(); $stat=[ordered]@{Moved=0;Removed=0;Wrapped=0;Kept=0;Manual=0}
function Ensure-Dir([string]$p){$d=Split-Path -Parent $p;if($d -and -not(Test-Path $d)){if($DoIt){New-Item -ItemType Directory -Force -Path $d|Out-Null}}}
foreach($r in $plan){
  $src=$r.Path; $canon=$r.CanonicalPath; $impact=$r.Impact
  if(-not(Test-Path -LiteralPath $src)){ if($canon -and (Test-Path -LiteralPath $canon)){$stat.Kept++} else {$todo+=$r; $stat.Manual++}; continue }
  switch($impact){
    'KEEP' { if($canon -and $src -ne $canon){ Ensure-Dir $canon; if($DoIt){ Move-Item -LiteralPath $src -Destination $canon -Force }; $stat.Moved++ } else { $stat.Kept++ } }
    default { $todo+=$r; $stat.Manual++ }
  }
}
$rep=Join-Path $root 'reports'; if(-not(Test-Path $rep)){New-Item -ItemType Directory -Force -Path $rep|Out-Null}
$out=Join-Path $rep ("tools_tidy_manual_{0}.csv" -f (Get-Date -Format yyyyMMdd_HHmmss))
$todo | Export-Csv -NoTypeInformation -Path $out
Write-Host "Manual check items -> $out"
Write-Host ("Summary: moved={0} removed={1} wrapped={2} kept={3} manual={4} (DoIt={5})" -f $stat.Moved,0,0,$stat.Kept,$stat.Manual,$DoIt.IsPresent)
