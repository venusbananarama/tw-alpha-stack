#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  # 共用時間（End 半開）
    [string] $Start,  [string] $End,

  # 模式：single（單線推進）或 roundrobin（獨立游標輪循）
  [ValidateSet("single","roundrobin")][string]$Mode = "roundrobin",

  # 要跑哪些資料集（single 用；roundrobin 用於順序）
  [ValidateSet("prices","chip","per","dividend")]
  [string[]]$Datasets = @("prices","chip","per","dividend"),

  # 各資料集起跑點
  [string]$StartPrices   = "2015-04-18",
  [string]$StartChip     = "2015-04-04",
  [string]$StartPER      = "2015-04-15",
  [string]$StartDividend = "2004-01-01",

  # 引擎與節流
  [string]$UniverseFile = ".\configs\investable_universe.txt",
  [double]$Qps = 1.33, [int]$Rpm = 80,
  [int]$BatchSize = 80, [int]$MaxConcurrency = 1,
  [int]$MaxRetries = 3, [int]$RetryDelaySec = 10,

  # Checkpoint / ledger
  [switch]$EnableCheckpoint = $true,
  [string]$CheckpointRoot = "_state\mainline",
  [string]$LedgerPath = "metrics\ingest_ledger.jsonl",

  # 通用行為
  [switch]$SkipIfOk = $true,
  [int]$ProgressEvery = 50,

  # 402/429 退避
  [int]$MaxRateRetries = 8,
  [int]$BaseBackoffSec = 15,
  [int]$MaxBackoffSec = 600,  [ValidateSet('live','backfill')][string] $RunType = 'live',
  [switch] $AutoStart = $true,
  [switch] $AutoEnd = $true,
  [string] $RunId)
# === AutoStart(Global) bootstrap (generated) ===
# 目的：當未提供 -Start 時，自動推導全域 Start，並確保後續 [datetime]$Start 轉型不會出錯。
# 策略：取四個 per-dataset 起點中最小值；若都無則 fallback 到 2004-01-01。
if (-not $PSBoundParameters.ContainsKey('Start') -or [string]::IsNullOrWhiteSpace($Start)) {
  $cands = @()
  foreach($v in @($StartPrices,$StartChip,$StartPER,$StartDividend)){
    if($v -and $v -match '^\d{4}-\d{2}-\d{2}$'){ $cands += $v }
  }
  if($cands.Count){ $Start = ($cands | Sort-Object | Select-Object -First 1) } else { $Start = '2004-01-01' }
  Write-Host ("AutoStart(Global) → Start={0}" -f $Start) -ForegroundColor DarkCyan
}

# 若任何 per-dataset 起點為空，一律補為全域 Start（避免後續比較/轉型遇到空）
if(-not $StartPrices){   $StartPrices   = $Start }
if(-not $StartChip){     $StartChip     = $Start }
if(-not $StartPER){      $StartPER      = $Start }
if(-not $StartDividend){ $StartDividend = $Start }

# 嚴格檢查格式，避免後續 [datetime] 轉型在深處才爆
try{
  [void][datetime]::ParseExact($Start,'yyyy-MM-dd',[System.Globalization.CultureInfo]::InvariantCulture,[System.Globalization.DateTimeStyles]::None)
}catch{
  throw "Start bootstrap produced invalid date: '$Start'"
}
# === /AutoStart(Global) bootstrap ===

# === AutoStart bootstrap (generated) ===
function NextStartFromOk {
  param([string]$Dir, [string]$Fallback)
  try{
    if (Test-Path $Dir) {
      $last = Get-ChildItem $Dir -Filter *.ok -Recurse -ErrorAction SilentlyContinue |
              Where-Object { $\_.BaseName -match '^\d{4}-\d{2}-\d{2}$' } |
              Sort-Object Name | Select-Object -Last 1
      if ($last) { return ([datetime]$last.BaseName).AddDays(1).ToString('yyyy-MM-dd') }
    }
  } catch {}
  return $Fallback
}

if ($AutoStart) {
  # 以傳入的 -CheckpointRoot 為優先，否則預設 .\_state\mainline
  $cpRoot = if ($PSBoundParameters.ContainsKey('CheckpointRoot') -and $CheckpointRoot) { $CheckpointRoot } else { '.\_state\mainline' }

  if (-not $PSBoundParameters.ContainsKey('StartPrices')) {
    $fallback = if ($PSBoundParameters.ContainsKey('Start')) { $Start } else { '2015-04-18' }
    $StartPrices = NextStartFromOk (Join-Path $cpRoot 'prices') $fallback
  }
  if (-not $PSBoundParameters.ContainsKey('StartChip')) {
    $fallback = if ($PSBoundParameters.ContainsKey('Start')) { $Start } else { '2015-04-04' }
    $StartChip = NextStartFromOk (Join-Path $cpRoot 'chip') $fallback
  }
  if (-not $PSBoundParameters.ContainsKey('StartPER')) {
    $fallback = if ($PSBoundParameters.ContainsKey('Start')) { $Start } else { '2015-04-15' }
    $StartPER = NextStartFromOk (Join-Path $cpRoot 'per') $fallback
  }
  if (-not $PSBoundParameters.ContainsKey('StartDividend')) {
    $fallback = if ($PSBoundParameters.ContainsKey('Start')) { $Start } else { '2004-01-01' }
    $StartDividend = NextStartFromOk (Join-Path $cpRoot 'dividend') $fallback
  }

  Write-Host ("AutoStart → prices={0} chip={1} per={2} dividend={3}" -f $StartPrices,$StartChip,$StartPER,$StartDividend) -ForegroundColor DarkCyan
}
# === /AutoStart bootstrap ===
# === AutoEnd bootstrap (generated) ===
if ($AutoEnd -and -not $PSBoundParameters.ContainsKey('End')) {
  $End = (Get-Date).AddDays(1).ToString('yyyy-MM-dd')  # 明日（半開，不含當日）
  Write-Host ("AutoEnd → End={0}" -f $End) -ForegroundColor DarkCyan
}
# === /AutoEnd bootstrap ===
# === RunId bootstrap (generated) ===
if (-not $RunId -or [string]::IsNullOrWhiteSpace($RunId)) {
  $RunId = 'mainline-' + (Get-Date -Format 'yyyyMMdd-HHmmss') + '-' + ([guid]::NewGuid().ToString('N').Substring(0,8))
}
# === /RunId bootstrap ===

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (-not $env:ALPHACITY_ALLOW) { $env:ALPHACITY_ALLOW = "1" }

# === 引擎 ===
$ENGINE = Join-Path $PSScriptRoot "Backfill-RatePlan.fast.ps1"
if (-not (Test-Path $ENGINE)) { throw "找不到引擎：$ENGINE" }

# === AC.Checkpoint ===
$ACLoaded = $false
try { Import-Module (Join-Path $PSScriptRoot "..\common\AC.Checkpoint.psm1") -Force; $ACLoaded = $true } catch {}

function Write-Checkpoint-And-Ledger {
  param([string]$Dataset,[datetime]$Date,[double]$QpsLocal=0,[int]$Exit=0,[string]$Source="Mainline-Orchestrator")
  if ($EnableCheckpoint) {
    if ($ACLoaded -and (Get-Command -ea 0 New-Checkpoint)) {
      New-Checkpoint -Dataset $Dataset -Date $Date -Root $CheckpointRoot | Out-Null
    } else {
      $okDir = Join-Path $CheckpointRoot $Dataset
      New-Item -ItemType Directory -Force $okDir | Out-Null
      New-Item -ItemType File -Force (Join-Path $okDir ($Date.ToString("yyyy-MM-dd") + ".ok")) | Out-Null
    }
    Write-Host ("   ✅ OK {0} {1}" -f $Dataset, $Date.ToString("yyyy-MM-dd")) -ForegroundColor Green
  }
  if ($ACLoaded -and (Get-Command -ea 0 Add-IngestLedger)) {
    Add-IngestLedger -Dataset $Dataset -Date $Date -Symbols 0 -Rows 0 -Qps $QpsLocal -Exit $Exit -Source $Source
  } else {
    New-Item -ItemType Directory -Force (Split-Path $LedgerPath) | Out-Null
    $obj = [ordered]@{ ts=(Get-Date).ToString("s"); dataset=$Dataset; date=$Date.ToString("yyyy-MM-dd"); symbols=0; rows=0; qps=$QpsLocal; exit=$Exit; source=$Source ; run_type = $RunType; run_id = $RunId } |
           ConvertTo-Json -Compress
    Add-Content -Encoding UTF8 -Path $LedgerPath -Value $obj
  }
}

function Has-Ok([string]$ds,[datetime]$d){
  $p = Join-Path $CheckpointRoot $ds
  Test-Path (Join-Path $p ($d.ToString('yyyy-MM-dd') + '.ok'))
}

# === 呼叫引擎（子行程）＋ 402/429 退避；先設 Do* 變數，再 dot-source 引擎 ===
function Invoke-Engine-Day {
  param([datetime]$Day,[string]$Dataset)
  $SStr = $Day.ToString("yyyy-MM-dd")
  $EStr = $Day.AddDays(1).ToString("yyyy-MM-dd")

  switch($Dataset){
    'prices'   { $assign = '$script:DoPrices=$true;  $script:DoChip=$false; $script:DoPER=$false; $script:DoDividend=$false;' }
    'chip'     { $assign = '$script:DoPrices=$false; $script:DoChip=$true;  $script:DoPER=$false; $script:DoDividend=$false;' }
    'per'      { $assign = '$script:DoPrices=$false; $script:DoChip=$false; $script:DoPER=$true;  $script:DoDividend=$false;' }
    'dividend' { $assign = '$script:DoPrices=$false; $script:DoChip=$false; $script:DoPER=$false; $script:DoDividend=$true;' }
    default    { $assign = '$script:DoPrices=$true;  $script:DoChip=$true;  $script:DoPER=$false; $script:DoDividend=$false;' }
  }

  $enginePath = (Resolve-Path $ENGINE).Path
  $cmd = "$assign . '$enginePath' -Start '$SStr' -End '$EStr' -UniverseFile '$UniverseFile' -Qps $Qps -BatchSize $BatchSize -MaxConcurrency $MaxConcurrency -MaxRetries $MaxRetries -RetryDelaySec $RetryDelaySec"

  $try=0
  while ($true) {
    try {
      Write-Host ("▶ {0} {1}" -f $Dataset,$SStr) -ForegroundColor Yellow
      & pwsh -NoProfile -Command $cmd
      if ($LASTEXITCODE -ne 0) { throw "Engine exit $LASTEXITCODE" }
      Write-Checkpoint-And-Ledger -Dataset $Dataset -Date $Day -QpsLocal $Qps
      return
    } catch {
      $msg = "$($_.Exception.Message)"
      $try++
      $isRate = ($msg -match '402') -or ($msg -match '429') -or ($msg -match '(?i)rate.+limit') -or ($msg -match '(?i)quota')
      if ($isRate -and $try -le $MaxRateRetries) {
        $delay = [Math]::Min([int]($BaseBackoffSec * [Math]::Pow(1.7,$try-1)), $MaxBackoffSec)
        Write-Warning ("⏳ 流控/配額（#{0}）：{1} → 等 {2}s 後重試..." -f $try,$msg,$delay)
        Start-Sleep -Seconds $delay
        continue
      } else {
        Write-Warning ("❌ 失敗：{0} {1} → {2}" -f $Dataset,$SStr,$msg)
        throw
      }
    } finally {
      # 外層 RPM 控速
      Start-Sleep -Milliseconds ([int][Math]::Ceiling(60000 / [Math]::Max(1,$Rpm)))
    }
  }
}

# === 入口 ===
$S = [datetime]$Start
$E = [datetime]$End
$today = (Get-Date).Date
if ($E -gt $today) { $E = $today }
$cap = [math]::Round($Qps*60,1)

Write-Host ("🏁 {0} → {1} (半開) | Mode={2} | Qps={3} | EngineCap≈{4} rpm | RPM={5}" -f $S.ToString('yyyy-MM-dd'),$E.ToString('yyyy-MM-dd'),$Mode,$Qps,$cap,$Rpm) -ForegroundColor Cyan
Write-Host ("Datasets = {0}" -f ($Datasets -join ',')) -ForegroundColor Yellow

if ($Mode -eq 'single') {
  $map = @{
    prices   = [datetime]$StartPrices
    chip     = [datetime]$StartChip
    per      = [datetime]$StartPER
    dividend = [datetime]$StartDividend
  }
  foreach ($ds in $Datasets) {
    $d = $map[$ds]; if ($d -lt $S) { $d = $S }
    $step=0
    while ($d -lt $E) {
      if ($SkipIfOk -and (Has-Ok $ds $d)) { $d = $d.AddDays(1); continue }
      $t0=Get-Date
      Invoke-Engine-Day -Day $d -Dataset $ds
      $t1=Get-Date; $el=New-TimeSpan -Start $t0 -End $t1
      Write-Host ("✅ DONE day {0} (dataset={1}) ┆ stop {2} ┆ elapsed {3:mm\:ss}" -f $d.ToString('yyyy-MM-dd'),$ds,$t1.ToString('HH:mm:ss'),$el) -ForegroundColor Green
      $step++
      if($ProgressEvery -gt 0 -and ($step % $ProgressEvery -eq 0)){ Write-Host ("📊 Progress {0}: +{1} days" -f $ds,$step) -ForegroundColor DarkYellow }
      $d = $d.AddDays(1)
    }
  }
  Write-Host "[Orchestrator(single)] 完成" -ForegroundColor Green
  return
}

# === roundrobin：獨立游標，各跑一天 ===
$cursors = @{
  prices   = [datetime]$StartPrices
  chip     = [datetime]$StartChip
  per      = [datetime]$StartPER
  dividend = [datetime]$StartDividend
}
foreach($k in $cursors.Keys){ if($cursors[$k] -lt $S){ $cursors[$k] = $S } }

Write-Host ("初始游標  prices={0}  chip={1}  per={2}  dividend={3}" -f $cursors.prices.ToString('yyyy-MM-dd'),$cursors.chip.ToString('yyyy-MM-dd'),$cursors.per.ToString('yyyy-MM-dd'),$cursors.dividend.ToString('yyyy-MM-dd')) -ForegroundColor DarkCyan

$totalSteps=0
while ($true) {
  $progressed = $false
  foreach ($ds in $Datasets) {
    $cur = $cursors[$ds]
    if ($cur -ge $E) { continue }
    if ($SkipIfOk) {
      while($cur -lt $E -and (Has-Ok $ds $cur)) { $cur = $cur.AddDays(1) }
      $cursors[$ds] = $cur
      if ($cur -ge $E) { continue }
    }
    $t0=Get-Date
    Invoke-Engine-Day -Day $cur -Dataset $ds
    $t1=Get-Date; $el=New-TimeSpan -Start $t0 -End $t1
    Write-Host ("✅ DONE day {0} (dataset={1}) ┆ stop {2} ┆ elapsed {3:mm\:ss}" -f $cur.ToString('yyyy-MM-dd'),$ds,$t1.ToString('HH:mm:ss'),$el) -ForegroundColor Green
    $cursors[$ds] = $cur.AddDays(1)
    $progressed = $true
    $totalSteps++
    if($ProgressEvery -gt 0 -and ($totalSteps % $ProgressEvery -eq 0)){
      Write-Host ("📊 Progress (roundrobin) steps={0}  cursors: prices={1} chip={2} per={3} dividend={4}" -f $totalSteps, $cursors.prices.ToString('yyyy-MM-dd'),$cursors.chip.ToString('yyyy-MM-dd'),$cursors.per.ToString('yyyy-MM-dd'),$cursors.dividend.ToString('yyyy-MM-dd')) -ForegroundColor DarkYellow
    }
  }
  $allDone = -not ($cursors.GetEnumerator() | Where-Object { $_.Value -lt $E })
  if ($allDone -or -not $progressed) { break }
}
Write-Host ("完成游標  prices={0}  chip={1}  per={2}  dividend={3}" -f $cursors.prices.ToString('yyyy-MM-dd'),$cursors.chip.ToString('yyyy-MM-dd'),$cursors.per.ToString('yyyy-MM-dd'),$cursors.dividend.ToString('yyyy-MM-dd')) -ForegroundColor DarkCyan
Write-Host "[Orchestrator(roundrobin)] 完成" -ForegroundColor Green
















