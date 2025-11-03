#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [string]$Reports = ".\reports",
  [string]$WFDir   = ".\runs\wf_configs",
  [string]$Out     = ".\reports\wf_summary.json"
)
Set-StrictMode -Version Latest; $ErrorActionPreference='Stop'

function Get-Field($obj,[string]$name){
  if($null -eq $obj){ return $null }
  $p = $obj.PSObject.Properties[$name]; if($p){ return $p.Value }
  if($obj -is [hashtable] -and $obj.ContainsKey($name)){ return $obj[$name] }
  $null
}
function To-Bool($v){
  if($null -eq $v){ return $false }
  if($v -is [bool]){ return $v }
  if($v -is [int] -or $v -is [double]){ return ([double]$v) -ne 0 }
  $s = [string]$v
  return $s -match '^(?i:true|1|ok|pass|success)$'
}
function As-Enumerable($x){
  if($null -eq $x){ return @() }
  if($x -is [System.Collections.IEnumerable] -and -not ($x -is [string])){ return $x }
  ,$x
}
function Count-Passes($objs){
  $tot=0; $ok=0
  foreach($it in (As-Enumerable $objs)){
    $f = $false
    foreach($k in @('ok','pass','status','result')){
      $v = Get-Field $it $k
      if($null -ne $v){
        if($k -in @('status','result')){ $f = $f -or ([string]$v -match '^(?i:ok|pass|success)$') }
        else { $f = $f -or (To-Bool $v) }
      }
    }
    $tot++; if($f){ $ok++ }
  }
  @{ tot=$tot; ok=$ok }
}
function Get-WindowsFromWF([string]$dir){
  $wins=@()
  if(Test-Path $dir){
    Get-ChildItem $dir -File -Include *.yml,*.yaml,*.json -EA SilentlyContinue | ForEach-Object{
      try{
        $t = Get-Content $_.FullName -Raw
        if($t -match 'windows\s*:\s*\[([^\]]+)\]'){
          $cand = ($matches[1] -split ',') | ForEach-Object { $_.Trim() } |
                  Where-Object { $_ -match '^\d+$' } | ForEach-Object {[int]$_}
          if(@($cand).Length -gt 0){ $wins = $cand }
        }
      }catch{}
    }
  }
  if(@($wins).Length -eq 0){ $wins = @(6,12,24) }
  $wins
}
function TryReadJson([string]$Path){
  if(!(Test-Path $Path)){ return $null }
  $raw = Get-Content $Path -Raw -EA Stop
  if($raw -match '^\s*null\s*$'){ return $null }
  try { $raw | ConvertFrom-Json -EA Stop } catch { $null }
}

$wins = Get-WindowsFromWF $WFDir
$srcs = @()
$tot=0; $ok=0

# (A) 舊版 Gate CSV：pass_results / fail_results
$passCsv = Join-Path $Reports 'pass_results.csv'
$failCsv = Join-Path $Reports 'fail_results.csv'
if ((Test-Path $passCsv) -or (Test-Path $failCsv)){
  if(Test-Path $passCsv){ $p = Import-Csv $passCsv; $ok  += @($p).Count; $tot += @($p).Count; $srcs += 'pass_results.csv' }
  if(Test-Path $failCsv){ $f = Import-Csv $failCsv; $tot += @($f).Count; $srcs += 'fail_results.csv' }
}

# (B) wf_smoke_metrics.csv（欄位可能是 ok/pass/status）
if($tot -eq 0){
  $mCsv = Join-Path $Reports 'wf_smoke_metrics.csv'
  if(Test-Path $mCsv){
    $rows = Import-Csv $mCsv
    foreach($r in $rows){ if(To-Bool $r.ok -or To-Bool $r.pass -or To-Bool $r.status){ $ok++ }; $tot++ }
    if($tot -gt 0){ $srcs += 'wf_smoke_metrics.csv' }
  }
}

# (C) wf_results.json（多為陣列）
if($tot -eq 0){
  $wr = TryReadJson (Join-Path $Reports 'wf_results.json')
  if($wr){
    $c = Count-Passes $wr; $ok=$c.ok; $tot=$c.tot; if($tot -gt 0){ $srcs += 'wf_results.json' }
  }
}

# (D) _runner_results.json（多種 schema；或單一物件）
if($tot -eq 0){
  $rr = TryReadJson (Join-Path $Reports '_runner_results.json')
  if($rr){
    $cand = $null
    foreach($k in 'runs','jobs','results','items','entries'){ $v=Get-Field $rr $k; if($v){ $cand=$v; break } }
    if($cand){
      $c = Count-Passes $cand; $ok=$c.ok; $tot=$c.tot; if($tot -gt 0){ $srcs += '_runner_results.json' }
    } else {
      $c = Count-Passes @($rr); $ok=$c.ok; $tot=$c.tot; if($tot -gt 0){ $srcs += '_runner_results.json(single)' }
    }
  }
}

$pr = $null
if($tot -gt 0){ $pr = [math]::Round($ok/$tot,3) }

$outObj = [ordered]@{
  pass_rate = $pr
  windows   = $wins
  totals    = @{ ok = $ok; total = $tot }
  sources   = $srcs
}
($outObj | ConvertTo-Json -Depth 6) | Set-Content -LiteralPath $Out -Encoding UTF8
Write-Host ("[OK] Wrote {0}  pass_rate={1}  windows={2}  totals={3}/{4}  sources={5}" -f $Out,$pr,($wins -join ','),$ok,$tot,($srcs -join ', '))
