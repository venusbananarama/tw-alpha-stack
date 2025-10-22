param([switch]$Loop=$false, [int]$IntervalSec=20)
$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest

function Get-Prop([object]$o,[string]$name){
  if($null -eq $o){ return $null }
  $p = $o.PSObject.Properties[$name]
  if($null -eq $p){ return $null }
  return $p.Value
}

function Read-Preflight {
  if(!(Test-Path .\reports\preflight_report.json)){ return $null }
  try {
    $j = Get-Content .\reports\preflight_report.json -Raw | ConvertFrom-Json
    [pscustomobject]@{
      prices   = $j.freshness.prices.max_date
      chip     = $j.freshness.chip.max_date
      per      = $j.freshness.per.max_date
      dividend = $j.freshness.dividend.max_date
    }
  } catch { $null }
}

function Read-Gate {
  if(!(Test-Path .\reports\gate_summary.json)){ return $null }
  try {
    $raw = (Get-Content .\reports\gate_summary.json -Raw) | ConvertFrom-Json -NoEnumerate
    $isDict = $raw -is [System.Collections.IDictionary]
    $isList = ($raw -is [System.Collections.IEnumerable]) -and -not ($raw -is [string]) -and -not $isDict
    if($null -eq $raw -or $isList){
      $runs = if($null -eq $raw){ @() } else { @($raw) }
      $pass = 0; foreach($it in $runs){ if($it -and (Get-Prop $it 'gate') -and (Get-Prop (Get-Prop $it 'gate') 'ok')){ $pass++ } }
      $rate = if($runs.Count){ [math]::Round($pass/$runs.Count,4) } else { 0.0 }
      return [pscustomobject]@{ overall='N/A'; wf=[pscustomobject]@{pass_rate=$rate}; runs_count=$runs.Count }
    } else { return $raw }
  } catch { $null }
}

function Show-HUD {
  Clear-Host
  $procs = Get-CimInstance Win32_Process -Filter "name='pwsh.exe'" |
    ?{ $_.CommandLine -match 'Run-Max-Recent\.ps1|Run-FullMarket-DateID-?MaxRate\.ps1|Run-WFGate\.ps1|wf_runner\.py' } |
    Select-Object ProcessId,CommandLine

  $pf = Read-Preflight
  $gt = Read-Gate

  Write-Host ("AlphaCity HUD  -  {0}" -f (Get-Date -Format 'yyyy/MM/dd HH:mm:ss'))
  Write-Host "Processes:"; if($procs){ $procs | Format-Table -AutoSize | Out-Host } else { Write-Host "  (none)" }

  if($pf){
    Write-Host ("Preflight: prices={0}  chip={1}  per={2}  dividend={3}" -f $pf.prices,$pf.chip,$pf.per,$pf.dividend)
  } else { Write-Host "Preflight: (not ready)" }

  if($gt){
    $overall = Get-Prop $gt 'overall'
    $rate    = Get-Prop (Get-Prop $gt 'wf') 'pass_rate'
    $runs    = Get-Prop $gt 'runs_count'
    $mode    = Get-Prop $gt 'mode'
    $counts  = Get-Prop $gt 'counts'
    $detail  = if($counts){ "(orig=$($counts.pass_orig), smoke+=$($counts.pass_smoke_added))" } else { "" }
    Write-Host ("Gate: overall={0}  wf.pass_rate={1}  runs={2}  mode={3} {4}" -f $overall,$rate,$runs,$mode,$detail)
  } else { Write-Host "Gate: (no report)" }

  $lf = Get-ChildItem .\reports -File -Filter '*.log' -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Desc | Select-Object -First 1
  if($lf){ Write-Host "`n=== Tail: $($lf.Name) ==="; Get-Content $lf.FullName -Tail 8 | Out-Host }
}

do { Show-HUD; if($Loop){ Start-Sleep -Seconds $IntervalSec } } while($Loop)
