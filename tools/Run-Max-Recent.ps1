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
$target = Join-Path -Path $PSScriptRoot -ChildPath 'orchestrator\Run-Max-Recent.ps1'
if(-not(Test-Path -LiteralPath $target)){ throw "Target not found: $target" }
& $target @named @pos
$code=$LASTEXITCODE; if($null -eq $code){$code=0}; exit ([int]$code)
