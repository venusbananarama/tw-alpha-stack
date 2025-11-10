#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [Parameter(Mandatory)] [string]$Start,
  [Parameter(Mandatory)] [string]$End,

  # Dataset 旗標（有帶任一個 → 顯式開/關；都沒帶 → 沿用引擎常態：prices+chip）
  [switch]$DoPrices,
  [switch]$DoChip,
  [switch]$DoPER,
  [switch]$DoDividend,

  # 引擎常用參數
  [string]$UniverseFile = ".\configs\investable_universe.txt",
  [double]$Qps = 1.33,
  [int]$BatchSize = 80,
  [int]$MaxConcurrency = 1,
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,

  # 調速（預設逐日 + RPM=80）
  [ValidateSet("q3","rpm")] [string]$SpeedMode = "rpm",
  [int]$ChunkDays = 3,     # 只在 q3 使用
  [int]$Rpm = 80,

  # Checkpoint / ledger
  [switch]$EnableCheckpoint = $true,
  [string]$CheckpointRoot = "_state\mainline",
  [string]$LedgerPath = "metrics\ingest_ledger.jsonl",

  # 402/429 退避
  [int]$MaxRateRetries = 8,
  [int]$BaseBackoffSec = 15,
  [int]$MaxBackoffSec = 600,

  # 顯示
  [int]$PlanPreviewN = 8,
  [int]$PlanShowBatches = 5
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"
if (-not $env:ALPHACITY_ALLOW) { $env:ALPHACITY_ALLOW = "1" }

# === 引擎與參數偵測 ===
$ENGINE = Join-Path $PSScriptRoot "Backfill-RatePlan.fast.ps1"
if (-not (Test-Path $ENGINE)) { throw "找不到原引擎：$ENGINE" }
try { $cmd = Get-Command $ENGINE -ErrorAction Stop } catch { throw "無法解析引擎參數：$($_.Exception.Message)" }
$keys = @(); if ($cmd -and $cmd.Parameters) { $keys = $cmd.Parameters.Keys }

function New-EngineArgs {
  param([string]$S,[string]$E)
  $startName = @("Start","From","Begin","S") | Where-Object { $keys -contains $_ } | Select-Object -First 1
  $endName   = @("End","To","Until","E")     | Where-Object { $keys -contains $_ } | Select-Object -First 1
  $dateOnly  = -not $startName -and ($keys -contains "Date")

  $args = @()
  if ($dateOnly) { $args += @("-Date", $S) } else {
    if (-not $startName -or -not $endName) { throw "引擎無 Start/End 且無 Date；無法執行" }
    $args += @("-$startName", $S, "-$endName", $E)
  }

  foreach ($p in @("UniverseFile","Qps","BatchSize","MaxConcurrency","MaxRetries","RetryDelaySec")) {
    if ($keys -contains $p) { $args += @("-$p", (Get-Variable $p -ValueOnly)) }
  }
  [pscustomobject]@{ Args=$args; DateOnly=$dateOnly }
}

# === Universe ===
function Read-Universe {
  param([string]$Path)
  if (-not (Test-Path $Path)) { return @() }
  @(Get-Content $Path | ForEach-Object { $_.Trim() } | Where-Object { $_ -and -not $_.StartsWith("#") })
}

# === Checkpoint / ledger ===
$ACLoaded = $false
try { Import-Module (Join-Path $PSScriptRoot "..\common\AC.Checkpoint.psm1") -Force; $ACLoaded = $true } catch {}

$okCount = @{ prices=0; chip=0; per=0; dividend=0 }

function Write-Checkpoint-And-Ledger {
  param([string]$Dataset,[datetime]$Date,[double]$QpsLocal=0,[int]$Exit=0,[string]$Source="Backfill-history-ok-checkpoint")
  if ($EnableCheckpoint) {
    if ($ACLoaded -and (Get-Command -ea 0 New-Checkpoint)) {
      New-Checkpoint -Dataset $Dataset -Date $Date -Root $CheckpointRoot | Out-Null
    } else {
      $okDir = Join-Path $CheckpointRoot $Dataset
      New-Item -ItemType Directory -Force $okDir | Out-Null
      New-Item -ItemType File -Force (Join-Path $okDir ($Date.ToString("yyyy-MM-dd") + ".ok")) | Out-Null
    }
    $okCount[$Dataset]++
    Write-Host ("   ✅ OK {0} {1}" -f $Dataset, $Date.ToString("yyyy-MM-dd")) -ForegroundColor Green
  }

  if ($ACLoaded -and (Get-Command -ea 0 Add-IngestLedger)) {
    Add-IngestLedger -Dataset $Dataset -Date $Date -Symbols 0 -Rows 0 -Qps $QpsLocal -Exit $Exit -Source $Source
  } else {
    New-Item -ItemType Directory -Force (Split-Path $LedgerPath) | Out-Null
    $obj = [ordered]@{ ts=(Get-Date).ToString("s"); dataset=$Dataset; date=$Date.ToString("yyyy-MM-dd"); symbols=0; rows=0; qps=$QpsLocal; exit=$Exit; source=$Source } |
           ConvertTo-Json -Compress
    Add-Content -Encoding UTF8 -Path $LedgerPath -Value $obj
  }
}

function Checkpoint-Range {
  param([datetime]$s,[datetime]$e,[bool]$P,[bool]$C,[bool]$R,[bool]$D)
  if (-not $EnableCheckpoint) { return }
  $d=$s
  while ($d -lt $e) {
    if ($P) { Write-Checkpoint-And-Ledger -Dataset "prices"   -Date $d -QpsLocal $Qps }
    if ($C) { Write-Checkpoint-And-Ledger -Dataset "chip"     -Date $d -QpsLocal $Qps }
    if ($R) { Write-Checkpoint-And-Ledger -Dataset "per"      -Date $d -QpsLocal $Qps }
    if ($D) { Write-Checkpoint-And-Ledger -Dataset "dividend" -Date $d -QpsLocal $Qps }
    $d = $d.AddDays(1)
  }
}

# === 引擎呼叫（外部進程；非 wrapper） ===
function Invoke-Engine-Safely {
  param([string]$SStr,[string]$EStr,[bool]$P,[bool]$C,[bool]$R,[bool]$D)
  $try=0
  while ($true) {
    try {
      $conf = New-EngineArgs -S $SStr -E $EStr
      # 有帶任一 Do* → 顯式注入四旗標（未帶者 false）
      $explicit = @(@("DoPrices","DoChip","DoPER","DoDividend") | Where-Object { $PSBoundParameters.ContainsKey($_) })
      $flagsScript = ""
      if ($explicit.Count -gt 0) {
        $flagsScript = ("$" + "Script:DoPrices=" + $P.ToString().ToLower() + "; " +
                        "$" + "Script:DoChip="   + $C.ToString().ToLower() + "; " +
                        "$" + "Script:DoPER="    + $R.ToString().ToLower() + "; " +
                        "$" + "Script:DoDividend="+ $D.ToString().ToLower() )
      }

      $argStr = ( @($conf.Args) | ForEach-Object {
        if ($_ -is [string]) { if ($_ -match '^[\-]') { $_ } else { "'" + ($_ -replace "'", "''") + "'" } } else { "$_" }
      } ) -join ' '

      $cmd = "`$ErrorActionPreference='Stop'; " + ($flagsScript + '; ')*[int]([bool]$flagsScript) + ". '$ENGINE' $argStr"

      & pwsh -NoProfile -ExecutionPolicy Bypass -Command $cmd
      if ($LASTEXITCODE -ne 0) { throw "Engine exit $LASTEXITCODE" }
      return 0
    }
    catch {
      $msg = "$($_.Exception.Message)"
      if ($msg -match 'Traceback') { Write-Warning "⚠️  Engine 拋出 Python Traceback（詳見上方輸出）" }
      $try++
      $isRate = ($msg -match '402') -or ($msg -match '429') -or ($msg -match '(?i)rate.+limit') -or ($msg -match '(?i)quota')
      if ($isRate -and $try -le $MaxRateRetries) {
        $delay = [Math]::Min([int]($BaseBackoffSec * [Math]::Pow(1.7,$try-1)), $MaxBackoffSec)
        Write-Warning ("⏳ 流控/配額（#{0}）：{1} → 等 {2}s 後重試..." -f $try,$msg,$delay)
        Start-Sleep -Seconds $delay
        continue
      } else {
        Write-Host ("   ❌ FAIL block {0}~{1} → {2}" -f $SStr,$EStr,$msg) -ForegroundColor Red
        throw
      }
    }
    finally {
      if ($SpeedMode -eq "rpm") {
        Start-Sleep -Milliseconds ([int][Math]::Ceiling(60000 / [Math]::Max(1,$Rpm)))
      }
    }
  }
}

# === 主流程 ===
$S = [datetime]$Start
$E = [datetime]$End
$today = (Get-Date).Date
if ($E -gt $today) { $E = $today }  # End 半開

# 旗標決策：有帶任一 Do* → 嚴格照你的值；否則沿用引擎常態（prices+chip）
$any = @(@("DoPrices","DoChip","DoPER","DoDividend") | Where-Object { $PSBoundParameters.ContainsKey($_) })
$P = $any.Count -gt 0 ? [bool]$DoPrices   : $true
$C = $any.Count -gt 0 ? [bool]$DoChip     : $true
$R = $any.Count -gt 0 ? [bool]$DoPER      : $false
$D = $any.Count -gt 0 ? [bool]$DoDividend : $false
$enabled = @(); if($P){$enabled+='prices'}; if($C){$enabled+='chip'}; if($R){$enabled+='per'}; if($D){$enabled+='dividend'}

# Universe 與批次規劃（只預覽）
$uni = @(Read-Universe -Path $UniverseFile)
$uc  = $uni.Count
$estBatches = if($BatchSize -gt 0){ [math]::Ceiling($uc / [double]$BatchSize) } else { 0 }
$cap = [math]::Round($Qps*60,1)

if ($SpeedMode -eq 'rpm') {
  Write-Host ("🏁 {0} → {1} (半開) | Mode=rpm | Qps={2} | EngineCap≈{3} rpm | RPM={4}" -f $S.ToString('yyyy-MM-dd'),$E.ToString('yyyy-MM-dd'),$Qps,$cap,$Rpm) -ForegroundColor Cyan
} else {
  Write-Host ("🏁 {0} → {1} (半開) | Mode=q3 | Qps={2} | EngineCap≈{3} rpm" -f $S.ToString('yyyy-MM-dd'),$E.ToString('yyyy-MM-dd'),$Qps,$cap) -ForegroundColor Cyan
}
Write-Host ("🧩 Datasets = {0}" -f ($enabled -join ',')) -ForegroundColor Yellow
Write-Host ("🗂️  Universe = {0} symbols @ {1} batchSize → est_batches ≈ {2}" -f $uc,$BatchSize,$estBatches)
if ($uc -gt 0 -and $BatchSize -gt 0) {
  $b=0
  for ($i=0; $i -lt $uc -and $b -lt $PlanShowBatches; $i += $BatchSize) {
    $b++; $j=[math]::Min($i+$BatchSize-1,$uc-1)
    $pv = (@($uni[$i..$j]) | Select-Object -First $PlanPreviewN) -join ','
    Write-Host ("   Plan[{0}/{1}] count={2} preview={3}" -f $b,$estBatches,($j-$i+1),$pv) -ForegroundColor DarkCyan
  }
  if ($estBatches -gt $PlanShowBatches) {
    Write-Host ("   Plan[...] 其餘 {0} 批略" -f ($estBatches-$PlanShowBatches)) -ForegroundColor DarkCyan
  }
}

# 執行
$cursor=$S; $step=0; $totalDays=[math]::Max(0,($E-$S).Days)
$allStart = Get-Date
if ($SpeedMode -eq "rpm") {
  while ($cursor -lt $E) {
    $step++
    $chunkStart = Get-Date
    $chunkEndDt = $cursor.AddDays(1); if($chunkEndDt -gt $E){$chunkEndDt=$E}
    $SStr=$cursor.ToString("yyyy-MM-dd"); $EStr=$chunkEndDt.ToString("yyyy-MM-dd")

    Write-Host ("[{0}/{1}] 📆 {2}  ┆ start {3}" -f $step,$totalDays,$SStr,$chunkStart.ToString('HH:mm:ss')) -ForegroundColor Gray
    Invoke-Engine-Safely -SStr $SStr -EStr $EStr -P:$P -C:$C -R:$R -D:$D | Out-Null
    Checkpoint-Range -s $cursor -e $chunkEndDt -P:$P -C:$C -R:$R -D:$D
    $chunkStop = Get-Date
    $elapsed = New-TimeSpan -Start $chunkStart -End $chunkStop
    Write-Host ("✅ DONE day {0} (datasets={1}) ┆ stop {2} ┆ elapsed {3:mm\:ss}" -f $SStr, ($enabled -join ','), $chunkStop.ToString('HH:mm:ss'), $elapsed) -ForegroundColor Green

    $cursor=$chunkEndDt
  }
}
else {
  $steps=[math]::Ceiling($totalDays/[double][math]::Max(1,$ChunkDays))
  while ($cursor -lt $E) {
    $step++
    $chunkStart = Get-Date
    $chunkEndDt = $cursor.AddDays([math]::Max(1,$ChunkDays)); if($chunkEndDt -gt $E){$chunkEndDt=$E}
    $SStr=$cursor.ToString("yyyy-MM-dd"); $EStr=$chunkEndDt.ToString("yyyy-MM-dd")

    Write-Host ("[{0}/{1}] 📆 {2}~{3}  ┆ start {4}" -f $step,$steps,$SStr,$EStr,$chunkStart.ToString('HH:mm:ss')) -ForegroundColor Gray
    Invoke-Engine-Safely -SStr $SStr -EStr $EStr -P:$P -C:$C -R:$R -D:$D | Out-Null
    Checkpoint-Range -s $cursor -e $chunkEndDt -P:$P -C:$C -R:$R -D:$D
    $chunkStop = Get-Date
    $elapsed = New-TimeSpan -Start $chunkStart -End $chunkStop
    Write-Host ("✅ DONE block {0}~{1} (datasets={2}) ┆ stop {3} ┆ elapsed {4:mm\:ss}" -f $SStr,$EStr,($enabled -join ','),$chunkStop.ToString('HH:mm:ss'),$elapsed) -ForegroundColor Green

    $cursor=$chunkEndDt
  }
}
$allStop = Get-Date
$allElapsed = New-TimeSpan -Start $allStart -End $allStop
Write-Host ("📊 Summary → OK files: prices={0}, chip={1}, per={2}, dividend={3} ┆ total {4:hh\:mm\:ss}" -f $okCount.prices,$okCount.chip,$okCount.per,$okCount.dividend,$allElapsed) -ForegroundColor Yellow
Write-Host "[Overlay] 完成，OK.checkpoint 與 ledger 已同步寫入。"
