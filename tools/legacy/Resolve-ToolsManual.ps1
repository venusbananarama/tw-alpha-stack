#requires -Version 7.0
[CmdletBinding()]
param(
  [string]$ManualCsv = (Get-ChildItem .\reports\tools_tidy_manual_*.csv | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName,
  [string]$PlanJson  = (Get-ChildItem .\reports\tools_tidy_plan_*.json  | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName,
  [switch]$DoIt,
  [double]$AliasKB = 1.5
)
$ErrorActionPreference='Stop'
$root=(Resolve-Path .).Path
$rows = if(Test-Path $ManualCsv){ Import-Csv $ManualCsv } else { @() }
$plan = if(Test-Path $PlanJson){ Get-Content $PlanJson -Raw | ConvertFrom-Json } else { @() }
$archive=Join-Path $root ("tools\\_archive_"+(Get-Date -Format yyyyMMdd))
$legacy =Join-Path $root "tools\\legacy"
if($DoIt){ New-Item -ItemType Directory -Force -Path $archive,$legacy | Out-Null }
function Ensure-Dir([string]$p){$d=Split-Path -Parent $p; if($DoIt -and $d -and -not (Test-Path -LiteralPath $d)){ New-Item -ItemType Directory -Force -Path $d | Out-Null }}
function NewStd([string]$from,[string]$to){
  $fromDir=(Resolve-Path (Split-Path -Parent $from)).Path; $toAbs=(Resolve-Path $to).Path
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
  $body=$wrapper.Replace('__REL__',$rel)
  if($DoIt){ Set-Content -LiteralPath $from -Value $body -Encoding utf8NoBOM }
}
$stat=[ordered]@{Wrapped=0;Moved=0;Archived=0;Legacy=0;Missing=0;Errors=0}
$log = New-Object System.Collections.ArrayList
foreach($r in $rows){
  try{
    $src=$r.Path; $name=$r.Name; $kb=[double]$r.SizeKB
    $isWrap=([bool]$r.IsWrapper) -or ($kb -le $AliasKB)
    $canon= if($r.CanonicalPath){ Join-Path $root $r.CanonicalPath } else { $null }
    $target=$r.TargetResolved
    if(-not (Test-Path -LiteralPath $src)){ $stat.Missing++; [void]$log.Add([pscustomobject]@{action='missing';path=$src}); continue }
    if($isWrap -and $target -and (Test-Path -LiteralPath $target)){ Ensure-Dir $src; if($DoIt){ NewStd -from $src -to $target }; $stat.Wrapped++; [void]$log.Add([pscustomobject]@{action='wrap';path=$src;to=$target}); continue }
    if($name -match '(\.bak$|^BAK_|_bak_|_bak$|fix_|_fix_|hotfix|rewrite|minrewrite|reheader|lenfix|ac6|^REHEADER_|^HOTFIX_|^FIX_)'){
      $dst=Join-Path $archive $name; Ensure-Dir $dst; if($DoIt){ Move-Item -LiteralPath $src -Destination $dst -Force }
      $stat.Archived++; [void]$log.Add([pscustomobject]@{action='archive';path=$src;to=$dst}); continue
    }
    if($canon){ Ensure-Dir $canon; if($DoIt){ Move-Item -LiteralPath $src -Destination $canon -Force }; $stat.Moved++; [void]$log.Add([pscustomobject]@{action='move';path=$src;to=$canon}); continue }
    $dst=Join-Path $legacy $name; Ensure-Dir $dst; if($DoIt){ Move-Item -LiteralPath $src -Destination $dst -Force }; $stat.Legacy++; [void]$log.Add([pscustomobject]@{action='legacy';path=$src;to=$dst})
  } catch { $stat.Errors++; [void]$log.Add([pscustomobject]@{action='error';path=$r.Path;error=$_.Exception.Message}) }
}
if(-not (Test-Path .\reports)){ New-Item -ItemType Directory -Force -Path .\reports | Out-Null }
$summary=[pscustomobject]@{ ts=(Get-Date).ToString('s'); do_it=[bool]$DoIt; stat=$stat; log=$log }
$sumPath = ".\reports\manual_auto_resolve_summary_{0}.json" -f (Get-Date -Format yyyyMMdd_HHmmss)
$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $sumPath -Encoding utf8NoBOM
Write-Host ("[Manual-Resolve] wrapped={0} moved={1} archived={2} legacy={3} missing={4} errors={5}  (DoIt={6})" -f $stat.Wrapped,$stat.Moved,$stat.Archived,$stat.Legacy,$stat.Missing,$stat.Errors,$DoIt.IsPresent)
Write-Host "Summary -> $sumPath"