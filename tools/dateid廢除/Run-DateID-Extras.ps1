#requires -Version 7
<#
  Run-DateID-Extras.ps1 (AC6 minimal, array-safe)
  - Single-day × multi-IDs Date-ID fetch for "extra" datasets.
  - Writes to: datahub\silver\alpha\extra\<Dataset>\yyyymm=*
  - --end is exclusive; if not given, we use (Date + 1).
#>
[CmdletBinding(PositionalBinding=$false)]
param(
  [Parameter(Mandatory)][datetime]$Date,
  [Parameter(Mandatory)][string[]]$IDs,
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 10,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [string]$DataRoot = 'datahub',
  [datetime]$End
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# Root & Python
$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue | Out-Null
$PY = '.\.venv\Scripts\python.exe'
if(-not (Test-Path $PY)){ throw ("Missing {0} (create venv & install requirements)" -f $PY) }
if([string]::IsNullOrWhiteSpace($env:FINMIND_TOKEN)){ throw "FINMIND_TOKEN not set" }
if(-not $env:FINMIND_KBAR_INTERVAL){ $env:FINMIND_KBAR_INTERVAL='5' }

# ===== Helpers: pool & pattern expansion (ALL / 23XXX / 23*) =====
function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\configs\universe.tw_all.txt', '.\configs\universe.tw_all',
    '.\configs\groups\ALL', '.\configs\groups\ALL.txt',
    '.\universe\universe.tw_all.txt', '.\universe\tw_all.txt', '.\universe\all.txt'
  )
  foreach($p in $candidates){
    if(Test-Path $p){
      [string[]]$ids = @( Get-Content -LiteralPath $p | ForEach-Object {
        if($_ -match '^\s*(\d{4})(?:\.TW)?\b'){ $matches[1] }
      } | Sort-Object -Unique )
      if($ids.Length -gt 0){ return $ids }
    }
  }
  throw ("no valid pool file; tried: {0}" -f ($candidates -join ', '))
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW$','').Trim()
  if($t -match '^(ALL|TSE)$'){ return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if($like.Length -ge 4){ break }
    switch -Regex ($ch){
      '^\d$'     { $like += $ch; break }
      '^[Xx\?]$' { $like += '?'; break }
      '^\*$'     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  [string[]]$pool = Get-PoolIDs
  $set = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    $parts = @(); if($tok -is [string] -and $tok -match ','){ $parts = $tok.Split(',') } else { $parts = @($tok) }
    foreach($raw in $parts){
      $t = ($raw -replace '\.TW$','').Trim()
      if(-not $t){ continue }
      if($t -match '^(ALL|TSE)$'){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if($t -match '^[0-9]{4}$'){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if($id -like $like){ [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# Expand IDs → .TW (array-safe)
[string[]]$expandedIDs = @( Expand-IDPatterns @($IDs) )
if($expandedIDs.Length -eq 0){ throw "IDs pattern expands to empty set" }
[string[]]$symbols = $expandedIDs | ForEach-Object { "$_.TW" }

# Window & dataset list (arrays)
$ds    = $Date.ToString('yyyy-MM-dd')
$endEx = if($PSBoundParameters.ContainsKey('End')){ ([datetime]$End).ToString('yyyy-MM-dd') } else { ($Date.AddDays(1)).ToString('yyyy-MM-dd') }
[string[]]$sets = ($Datasets -split ',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }

# Throttle for the Python-side fetcher
$env:FINMIND_THROTTLE_RPM = [string]$RPM

Write-Host ("[DateID-Extras] date={0} end_exclusive={1} ids={2} rpm={3} datasets={4}" -f $ds,$endEx,($symbols.Length),$RPM,($sets -join ','))

# Main loop (array slicing guarded)
$N = $symbols.Length
for($i=0; $i -lt $N; $i+=$Batch){
  $j = [Math]::Min($i+$Batch-1, $N-1)
  if($j -lt $i){ break }
  [string[]]$chunk = @($symbols[$i..$j])
  $idsArg = ($chunk -join ',')
  foreach($d in $sets){
    & $PY .\scripts\fm_dateid_fetch.py `
      --datasets $d --ids $idsArg `
      --date $ds --end $endEx `
      --out-root $DataRoot
  }
  if($i -lt ($N-1)){ Start-Sleep -Milliseconds 500 }
}
Write-Host "Done (extras)."
