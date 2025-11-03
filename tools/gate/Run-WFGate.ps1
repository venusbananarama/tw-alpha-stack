#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [string]$Root='.',
  [string]$Py,
  [string]$RulesFile = '.\rules.yaml',
  [string]$WFDir = '.\runs\wf_configs',   # 建議維持 canonical：runs\wf_configs (可做到 tools\gate\wf_configs 的 Junction)
  [string]$ReportsDir = '.\reports',
  [switch]$DryRun
)
Set-StrictMode -Version Latest; $ErrorActionPreference='Stop'

function Resolve-Python([string]$Hint){
  if($Hint){ return $Hint }
  foreach($p in @('.\.venv\Scripts\python.exe','python.exe')){ if(Test-Path $p){ return (Resolve-Path $p).Path } }
  'python.exe'
}
function TryReadJson([string]$Path){
  if(!(Test-Path $Path)){ return $null }
  $raw = Get-Content $Path -Raw -EA Stop
  if($raw -match '^\s*null\s*$'){ return $null }
  try { $raw | ConvertFrom-Json -EA Stop } catch { $null }
}
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
    # 允許多種欄位
    $f = $false
    foreach($k in @('ok','pass','status','result')){
      $v = Get-Field $it $k
      if($null -ne $v){
        if($k -in @('status','result')) { $f = $f -or ([string]$v -match '^(?i:ok|pass|success)$') }
        else { $f = $f -or (To-Bool $v) }
      }
    }
    $tot++
    if($f){ $ok++ }
  }
  @{ tot = $tot; ok = $ok }
}
function Get-WindowsFromWF([string]$dir){
  $wins=@()
  if(Test-Path $dir){
    Get-ChildItem $dir -File -Include *.yml,*.yaml,*.json -EA SilentlyContinue | ForEach-Object{
      try{
        $t = Get-Content $_.FullName -Raw
        if($t -match 'windows\s*:\s*\[([^\]]+)\]'){
          $cand = ($matches[1] -split ',') | ForEach-Object { $_.Trim() } | Where-Object { $_ -match '^\d+$' } | ForEach-Object {[int]$_}
          if(@($cand).Length -gt 0){ $wins = $cand }
        }
      } catch {}
    }
  }
  if(@($wins).Length -eq 0){ $wins = @(6,12,24) }
  $wins
}
function Compute-PassRate([string]$Reports){
  # (1) wf_summary.json
  $p1 = Join-Path $Reports 'wf_summary.json'
  $j1 = TryReadJson $p1
  if($j1){
    $prDirect = Get-Field $j1 'pass_rate'
    if($null -ne $prDirect){ return @{ pr=[double]$prDirect; source='wf_summary.json' } }
    $c = Count-Passes (As-Enumerable $j1)
    if($c.tot -gt 0){ return @{ pr=[math]::Round($c.ok/$c.tot,3); source='wf_summary.json(array)' } }
  }

  # (2) _runner_results.json（runs/jobs/results/items/entries 或單物件）
  $p2 = Join-Path $Reports '_runner_results.json'
  $j2 = TryReadJson $p2
  if($j2){
    $cand = $null
    foreach($k in 'runs','jobs','results','items','entries'){ $v=Get-Field $j2 $k; if($v){ $cand=$v; break } }
    if($cand){
      $c = Count-Passes (As-Enumerable $cand)
      if($c.tot -gt 0){ return @{ pr=[math]::Round($c.ok/$c.tot,3); source='_runner_results.json' } }
    } else {
      $c = Count-Passes @($j2)
      if($c.tot -gt 0){ return @{ pr=[math]::Round($c.ok/$c.tot,3); source='_runner_results.json(single)' } }
    }
  }

  # (3) wf_results.json
  $p3 = Join-Path $Reports 'wf_results.json'
  $j3 = TryReadJson $p3
  if($j3){
    $c = Count-Passes (As-Enumerable $j3)
    if($c.tot -gt 0){ return @{ pr=[math]::Round($c.ok/$c.tot,3); source='wf_results.json' } }
  }

  # (4) wf_smoke_metrics.csv
  $p4 = Join-Path $Reports 'wf_smoke_metrics.csv'
  if(Test-Path $p4){
    try{
      $rows = Import-Csv $p4
      $c = Count-Passes $rows
      if($c.tot -gt 0){ return @{ pr=[math]::Round($c.ok/$c.tot,3); source='wf_smoke_metrics.csv' } }
    }catch{}
  }
  @{ pr=$null; source='none' }
}

# ----------------- 主流程 -----------------
Push-Location $Root
try{
  $PY = Resolve-Python $Py
  if(!(Test-Path $ReportsDir)){ New-Item -ItemType Directory -Force -Path $ReportsDir | Out-Null }
  $rep = (Resolve-Path $ReportsDir).Path
  if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }

  # 0) Preflight（只列示，不中止；可能會先寫出 null gate_summary.json）
  & $PY .\scripts\preflight_check.py --rules $RulesFile --export $rep --root . | Write-Host

  # 1) 跑 WF（要求它至少寫 _runner_results.json）
  $env:AC_RUNNER_RESULTS = (Join-Path $rep '_runner_results.json')
  & $PY .\scripts\wf_runner.py --dir $WFDir --export $rep | Write-Host

  if($DryRun){ Write-Host '[DRYRUN] Skip summary compose.'; exit 0 }

  # 2) 盡力產出 wf_summary.json（若 core 存在）
  if(Test-Path '.\scripts\wf_runner_core.py'){
    & $PY .\scripts\wf_runner_core.py --dir $WFDir --summary --export $rep | Write-Host
  }

  # 3) 最終組裝 gate_summary.json（一定覆蓋掉前面的 null）
  $gsPath = Join-Path $rep 'gate_summary.json'
  $wins   = Get-WindowsFromWF $WFDir
  $info   = Compute-PassRate -Reports $rep
  $pr     = $info.pr
  $src    = $info.source
  $overall = if($pr -ge 0.80){ 'PASS' } elseif($pr -ge 0){ 'FAIL' } else { 'UNKNOWN' }

  $obj = [ordered]@{
    overall = $overall
    wf      = @{ windows = $wins; pass_rate = $pr }
    meta    = @{ generated_at=(Get-Date).ToString('s'); summary_source=$src }
  }
  ($obj | ConvertTo-Json -Depth 6) | Set-Content -LiteralPath $gsPath -Encoding UTF8
  "Gate Overall: $overall | WF.pass_rate=$pr | windows=$($wins -join ',') | src=$src" | Write-Host
}
catch{
  # 不讓例外把檔案留在 null；至少落地 UNKNOWN 骨架
  $wins = @(6,12,24)
  $obj = [ordered]@{
    overall='UNKNOWN'; wf=@{ windows=$wins; pass_rate=$null }; meta=@{ generated_at=(Get-Date).ToString('s'); summary_source='exception' }
  }
  ($obj | ConvertTo-Json -Depth 6) | Set-Content -LiteralPath (Join-Path $ReportsDir 'gate_summary.json') -Encoding UTF8
  Write-Error $_.Exception.Message
  exit 1
}
finally{ Pop-Location }