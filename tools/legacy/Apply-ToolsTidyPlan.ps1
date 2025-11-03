#requires -Version 7.0
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][string]$PlanJson,
  [switch]$DoIt,
  [bool]$KeepTopWrappers = $true,
  [string[]]$WrapperWhitelist = @('.\tools\Run-WFGate.ps1','.\tools\Run-DailyBackfill.ps1','.\tools\Tidy-Tools.ps1','.\tools\Assert-Preflight-Guard.ps1','.\tools\Resolve-ToolsManual.ps1')
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path .).Path
$plan=Get-Content -LiteralPath $PlanJson -Raw|ConvertFrom-Json
$todo=@()
function Ensure-Dir([string]$p){$d=Split-Path -Parent $p;if($d -and -not(Test-Path $d)){if($DoIt){New-Item -ItemType Directory -Force -Path $d|Out-Null}}}
function MakeAbs([string]$p){if($p -match '^[A-Za-z]:\\'){return $p};Join-Path $root $p}
function NewStd([string]$from,[string]$to){
  $fromDir=(Resolve-Path (Split-Path -Parent $from)).Path
  $toAbs=(Resolve-Path $to).Path
  $rel=[IO.Path]::GetRelativePath($fromDir,$toAbs)
  $wrapper = @'
# Auto-generated wrapper: DO NOT EDIT
#requires -Version 5.1
[CmdletBinding()]
param([Parameter(ValueFromRemainingArguments=$true)][object[]]$ArgList = @())
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$global:LASTEXITCODE = 0

# Robust argv parsing:
# - Rebuild named args; accumulate *all contiguous values* after a -Name into an array
# - Support repeated -Name usage (flatten arrays)
# - Switch (no value) => $true
# - Support `--` to stop parsing (rest are positional)
$named = @{}
$pos   = @()
for ($i=0; $i -lt $ArgList.Count; $i++) {
  $tok = $ArgList[$i]
  if ($tok -is [string] -and $tok -eq '--') {
    if ($i+1 -lt $ArgList.Count) { $pos += $ArgList[($i+1)..($ArgList.Count-1)] }
    break
  }
  if ($tok -is [string] -and $tok.StartsWith('-')) {
    $name = $tok.TrimStart('-')
    $vals = @()
    while ($i+1 -lt $ArgList.Count) {
      $next = $ArgList[$i+1]
      if (($next -is [string]) -and $next.StartsWith('-')) { break }
      $vals += $next
      $i++
    }
    $val = if ($vals.Count -eq 0) { $true } elseif ($vals.Count -eq 1) { $vals[0] } else { $vals }

    if ($named.ContainsKey($name)) {
      $prev = $named[$name]
      if ($prev -is [System.Collections.IList] -and -not ($prev -is [string])) {
        if ($val -is [System.Collections.IList] -and -not ($val -is [string])) { foreach ($v in $val) { [void]$prev.Add($v) } }
        else { [void]$prev.Add($val) }
        $named[$name] = $prev
      } else {
        if ($val -is [System.Collections.IList] -and -not ($val -is [string])) { $named[$name] = @($prev) + $val }
        else { $named[$name] = @($prev, $val) }
      }
    } else {
      $named[$name] = $val
    }
  } else {
    $pos += $tok
  }
}
$target = Join-Path -Path $PSScriptRoot -ChildPath '__REL__'
if(-not(Test-Path -LiteralPath $target)){ throw "Target not found: $target" }
& $target @named @pos
$code=$LASTEXITCODE; if($null -eq $code){$code=0}; exit ([int]$code)
'@
  $body = $wrapper.Replace('__REL__',$rel)
  if($DoIt){ Set-Content -LiteralPath $from -Value $body -Encoding utf8NoBOM }
}
$wlAbs=$WrapperWhitelist|%{Join-Path $root $_}
$stat=[ordered]@{Moved=0;Removed=0;Wrapped=0;Kept=0;Manual=0}
foreach($r in $plan){
  $src=$r.Path; $canon=MakeAbs $r.CanonicalPath; $impact=$r.Impact; $isWrap=[bool]$r.IsWrapper; $target=$r.TargetResolved
  if(-not(Test-Path -LiteralPath $src)){ if(Test-Path -LiteralPath $canon){$stat.Kept++} else {$todo+=$r;$stat.Manual++}; continue }
  switch($impact){
    'KEEP' {
      if($src -ne $canon){ Ensure-Dir $canon; if($DoIt){Move-Item -LiteralPath $src -Destination $canon -Force}; $stat.Moved++ } else { $stat.Kept++ }
    }
    'T0_SAFE_MOVE_PRIMARY' {
      Ensure-Dir $canon; if($src -ne $canon){ if($DoIt){Move-Item -LiteralPath $src -Destination $canon -Force}; $stat.Moved++ } else { $stat.Kept++ }
    }
    'T0_SAFE_REMOVE_ALIAS' {
      $keep = $KeepTopWrappers -and ($wlAbs -contains $src)
      if($keep){
        $to = if($target -and ($target -ne $src)){$target} elseif($r.CanonicalPath){$canon} else {$null}
        if($to -and (Test-Path -LiteralPath $to)){ Ensure-Dir $src; if($DoIt){ NewStd -from $src -to $to }; $stat.Wrapped++ } else { $todo+=$r; $stat.Manual++ }
      } else {
        if($DoIt){ Remove-Item -LiteralPath $src -Force -EA SilentlyContinue }; $stat.Removed++
      }
    }
    'T1_CHECK_MANUAL' {
      if($isWrap){
        $to = if($target -and ($target -ne $src)){$target} elseif($r.CanonicalPath){$canon} else {$null}
        if($to -and (Test-Path -LiteralPath $to)){ Ensure-Dir $src; if($DoIt){ NewStd -from $src -to $to }; $stat.Wrapped++ } else { $todo+=$r; $stat.Manual++ }
      } else { $todo+=$r; $stat.Manual++ }
    }
    default { $todo+=$r; $stat.Manual++ }
  }
}
if(-not (Test-Path .\reports)){ New-Item -ItemType Directory -Force -Path .\reports | Out-Null }
$out = Join-Path $root ("reports\\tools_tidy_manual_"+(Get-Date -Format yyyyMMdd_HHmmss)+".csv")
$todo | Export-Csv -NoTypeInformation -Path $out
Write-Host "Manual check items -> $out"
Write-Host ("Summary: moved={0} removed={1} wrapped={2} kept={3} manual={4} (DoIt={5})" -f $stat.Moved,$stat.Removed,$stat.Wrapped,$stat.Kept,$stat.Manual,$DoIt.IsPresent)