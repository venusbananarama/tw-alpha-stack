#requires -Version 7
<#
  Run-DateID-Extras-History.ps1
  Date-ID extras: full-range sweeper (single-thread + checkpoint + backoff)
  - Writes extras to: datahub\silver\alpha\extra\<Dataset>\yyyymm=*
#>
[CmdletBinding(PositionalBinding=$false)]
param(
  [datetime]$Start,
  [datetime]$End,
  [string[]]$IDs,
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_extras_history_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

$root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..')).Path
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

$RUN = '.\tools\Run-DateID-Extras.ps1'
if(-not (Test-Path $RUN)){ throw "Run-DateID-Extras.ps1 not found" }

# --- helpers: pool & pattern expansion (ALL / 23XXX / 23*) ---
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
  throw "no pool file with 4-digit IDs"
}
function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW$','').Trim()
  if($t -match '^(ALL|TSE)$'){ return '????' }
  $like=''
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

# --- calendar & range (clamp to Taipei today) ---
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if(-not $PSBoundParameters.ContainsKey('Start')){ $Start = $liveDays[0] }
if(-not $PSBoundParameters.ContainsKey('End'))  { $End   = $todayTpe }
$days = @($liveDays | Where-Object { $_ -ge $Start.Date -and $_ -le $End.Date })
if($days.Length -eq 0){ throw ("no trading days in range {0}..{1}" -f $Start.ToShortDateString(), $End.ToShortDateString()) }

# --- IDs & datasets ---
[string[]]$ids = @()
if ($PSBoundParameters.ContainsKey('IDs')) { $ids = @(Expand-IDPatterns @($IDs)) } else { $ids = @(Get-PoolIDs) }
if($ids.Length -eq 0){ throw "IDs/Pool is empty" }
[string[]]$syms = $ids | ForEach-Object { "$_.TW" }
[string[]]$sets = ($Datasets -split ',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }

# --- throttle ---
$env:FINMIND_THROTTLE_RPM = [string]$RPM
if(-not $env:FINMIND_KBAR_INTERVAL){ $env:FINMIND_KBAR_INTERVAL='5' }

New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$log = 'reports\dateid_extras_history_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
("POOL={0} dates={1} rpm={2} batch={3} sets={4}" -f $ids.Length,$days.Length,$RPM,$Batch,($sets -join ',')) | Tee-Object -FilePath $log

# --- checkpoint ---
$state=@{}; if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$dirty=$false
function Save-State{ param([switch]$force) if($dirty -or $force){ ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM; $script:dirty=$false } }

function Invoke-Day([datetime]$d){
  $ds = $d.ToString('yyyy-MM-dd')
  $ok=$false
  while(-not $ok){
    try{
      for($i=0; $i -lt $syms.Length; $i+=$Batch){
        $j = [Math]::Min($i+$Batch-1, $syms.Length-1)
        [string[]]$chunk = @($syms[$i..$j])
        foreach($set in $sets){
          pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN `
            -Date $ds -IDs ($chunk -join ',') -Datasets $set -RPM ([int]$env:FINMIND_THROTTLE_RPM) *>> $log
        }
        if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
      }
      $state[$ds]='ok'; $script:dirty=$true; Save-State
      $ok=$true
      Write-Host ("[OK] {0}" -f $ds)
    }catch{
      $msg = $_.Exception.Message
      $cur = [int]($env:FINMIND_THROTTLE_RPM)
      if($cur -gt 6){
        $new = [Math]::Max(6, [int]([double]$cur * 0.7))
        $env:FINMIND_THROTTLE_RPM = [string]$new
        ("[BACKOFF] {0}: {1}  rpm {2}->{3}" -f $ds,$msg,$cur,$new) | Tee-Object -FilePath $log -Append
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { ("[WARN][skip] {0}: {1}" -f $ds,$msg) | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
  Save-State
}

foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host ("[SKIP] {0}" -f $key); continue }
  Invoke-Day $d
}
("Done. Log={0}  Checkpoint={1}" -f $log,$Checkpoint) | Tee-Object -FilePath $log
