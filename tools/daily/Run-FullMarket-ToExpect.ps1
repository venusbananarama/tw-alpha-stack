#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [string]$Start = '2000-01-01',
  [string]$End,                                  # 若未給，讀 preflight 的 expect_date*
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [string]$UniverseFile = '.\configs\investable_universe.txt',
  [double]$Qps,
  [int]$LimitDays = 0,
  [switch]$DryRun
)
Set-StrictMode -Version Latest; $ErrorActionPreference='Stop'

Import-Module .\tools\daily\AC.Checkpoint.psm1 -Force  # 輕量 checkpoint 模組（你已安裝）

# -- 安全取值小工具：不直接用 $obj.guard，避免 StrictMode 在不存在屬性時拋錯
function Get-JsonProp {
  param([Parameter(Mandatory)][object]$Object, [Parameter(Mandatory)][string]$Name)
  if($null -eq $Object){ return $null }
  $p = $Object.PSObject.Properties[$Name]
  if($p){ return $p.Value } else { return $null }
}

function Resolve-ExpectDate {
  param([string]$ReportsDir = '.\reports')
  $PY=".\.venv\Scripts\python.exe"
  & $PY .\scripts\preflight_check.py --rules .\rules.yaml --export $ReportsDir --root . | Out-Null
  $pfPath = Join-Path $ReportsDir 'preflight_report.json'
  if(!(Test-Path $pfPath)){ throw "Missing $pfPath" }
  $pfRaw = Get-Content $pfPath -Raw -ErrorAction Stop
  try { $pf = $pfRaw | ConvertFrom-Json -ErrorAction Stop } catch { $pf = $null }

  $cands = @()
  if($pf){
    foreach($k in 'expect_date','expect_date_fixed'){
      $v = Get-JsonProp -Object $pf -Name $k
      if($v){ $cands += [string]$v }
    }
    $guard = Get-JsonProp -Object $pf -Name 'guard'
    if($guard){
      foreach($k in 'expect_date','expect_date_fixed'){
        $v = Get-JsonProp -Object $guard -Name $k
        if($v){ $cands += [string]$v }
      }
    }
  }
  $d = $null
  foreach($x in $cands){ if(-not [string]::IsNullOrWhiteSpace($x)) { $d = $x; break } }

  if(-not $d){
    # 最後退回交易日曆（<= 今天 最近一日）
    if(Test-Path .\cal\trading_days.csv){
      $today=(Get-Date).Date
      $last=(Import-Csv .\cal\trading_days.csv | ForEach-Object{ [datetime]::Parse($_.date).Date } |
             Where-Object { $_ -le $today } | Sort-Object | Select-Object -Last 1)
      if($last){ $d = $last.ToString('yyyy-MM-dd') }
    }
  }
  if(-not $d){ throw "expect_date missing (preflight_report.json / calendar)" }
  return $d
}

# --- 1) 解析 Start/End（End 不含） ---
try { $s=[datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
if([string]::IsNullOrWhiteSpace($End)){ $End = Resolve-ExpectDate }
try { $e=[datetime]::Parse($End).Date } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End($End) must be greater than Start($Start). End is exclusive." }

# --- 2) 日期集合（交易日優先；否則逐日） ---
$days = New-Object System.Collections.Generic.List[datetime]
$cal = ".\cal\trading_days.csv"
if(Test-Path $cal){
  (Import-Csv $cal) | ForEach-Object{
    $d=[datetime]::Parse($_.date).Date
    if($d -ge $s -and $d -lt $e){ [void]$days.Add($d) }
  }
}else{
  for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ [void]$days.Add($d) }
}
if($LimitDays -gt 0 -and $days.Count -gt $LimitDays){ $days = $days[-$LimitDays..($days.Count-1)] }

# --- 3) Universe 檢查（若缺，退回 derived/IDs-only） ---
if(!(Test-Path $UniverseFile) -or (Get-Item $UniverseFile).Length -eq 0){
  $alt = '.\configs\derived\universe_ids_only.txt'
  if(Test-Path $alt){ $UniverseFile = $alt } else { throw "Universe missing: $UniverseFile" }
}
$UNI_LINES = (Get-Content $UniverseFile | ? { $_ -match '^\d{4}$' } | Measure-Object -Line).Lines
if($UNI_LINES -le 0){ throw "Universe empty: $UniverseFile" }

# --- 4) 標準化 dataset 名稱 ---
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | ? { $_ } | ForEach-Object { $_.ToLower() } | Select-Object -Unique)

# --- 5) 主迴圈：逐日 × 逐資料集（無 .ok 才跑） ---
$fast = '.\tools\daily\Backfill-RatePlan.fast.ps1'
if(!(Test-Path $fast)){ throw "Missing $fast" }

$run=0; $skip=0
foreach($D in $days){
  $D1 = $D.AddDays(1)
  foreach($ds in $Tables){
    if(Test-Checkpoint $ds $D){ $skip++; continue }

    $DoPrices=$false; $DoChip=$false; $DoDividend=$false; $DoPER=$false
    switch($ds){
      'prices'   { $DoPrices=$true }
      'chip'     { $DoChip=$true }
      'dividend' { $DoDividend=$true }
      'per'      { $DoPER=$true }
      default { Write-Warning "Unknown dataset: $ds"; continue }
    }

    $args = @{ Start=$D.ToString('yyyy-MM-dd'); End=$D1.ToString('yyyy-MM-dd'); UniverseFile=$UniverseFile }
    if($PSBoundParameters.ContainsKey('Qps')){ $args.Qps = $Qps }

    if($DryRun){ Write-Host ("[DRYRUN] {0} {1}->{2} (U={3})" -f $ds,$args.Start,$args.End,$UniverseFile); continue }

    try{
      . $fast @args    # dot-source 才讀得到 $DoPrices/$DoChip/$DoDividend/$DoPER
      New-Checkpoint $ds $D | Out-Null
      Add-IngestLedger -Dataset $ds -Date $D -Symbols $UNI_LINES -Rows -1 -Qps ([double]($args.Qps?$args.Qps:0)) -Exit 0
      $run++
    }catch{
      Add-IngestLedger -Dataset $ds -Date $D -Symbols $UNI_LINES -Rows -1 -Qps ([double]($args.Qps?$args.Qps:0)) -Exit 1
      Write-Warning ("[{0}] failed on {1}: {2}" -f $ds,$D.ToString('yyyy-MM-dd'),$_.Exception.Message)
    }
  }
}
"Plan done. executed=$run skipped_ok=$skip days=$($days.Count) uni_lines=$UNI_LINES" | Write-Host

