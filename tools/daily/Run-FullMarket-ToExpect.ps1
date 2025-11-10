#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [Parameter(Mandatory)][string]$Start,      # 例：'2025-11-05'
  [Parameter(Mandatory)][string]$End,        # 半開：設 '2025-11-06' 才吃到 11/05
  [ValidateSet('prices','chip','per','all')]
  [string]$Dataset = 'all',

  [switch]$SkipIfOk = $true,                 # 有 .ok 就跳過
  [string]$CheckpointRoot = ".\_state\ingest", # ← 只寫 ingest，不碰 mainline
  [string]$RootPath = "C:\AI\tw-alpha-stack",
  [string]$UniversePath = ".\configs\investable_universe.txt",
  [int]$BatchSize = 80
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
Set-Location $RootPath

# ---- 基本環境（半開區間 gate + QPS/RPM）----
$env:EXPECT_DATE_FIXED = $End
$env:EXPECT_DATE       = $End
$env:FINMIND_QPS = $env:FINMIND_QPS ?? '1.67'
$env:FINMIND_RPM = $env:FINMIND_RPM ?? '100'

# ---- Universe 檢查（存在 + 格式）----
if(!(Test-Path $UniversePath)){ throw "Universe file not found: $UniversePath" }
$ucnt = (Get-Content $UniversePath | Where-Object { $_ -match '^\d{4,5}(\.TW|\.TWO|[A-Z])?$' }).Count
if($ucnt -le 0){ throw "Universe is empty: $UniversePath" }
Write-Host ("Universe count: {0}" -f $ucnt) -ForegroundColor DarkCyan

# ---- OK helpers（僅寫 ingest）----
function Get-OkPath([string]$ds,[string]$d){
  Join-Path $CheckpointRoot (Join-Path $ds "$d.ok")
}
function Has-Ok([string]$ds,[string]$d){
  Test-Path (Get-OkPath $ds $d)
}
function Write-Ok([string]$ds,[string]$d){
  $p = Get-OkPath $ds $d
  New-Item -ItemType Directory -Force -Path (Split-Path $p) | Out-Null
  "" | Out-File -Encoding ascii -Force $p
  Write-Host "OK $ds checkpoint: $d @ $CheckpointRoot" -ForegroundColor Green
}

# ---- Lock（避免同時重覆跑）----
function With-Lock([string]$name,[scriptblock]$body){
  $lock = ".\_state\locks\$name.lock"
  if(Test-Path $lock){ Write-Warning "Locked: $name（略過）"; return }
  New-Item -ItemType File -Path $lock -Force | Out-Null
  try { & $body } finally { Remove-Item $lock -ErrorAction SilentlyContinue }
}

# ---- 路徑：偏好日級入口，不在則退回 RatePlan ----
$DailyPrices = ".\tools\daily\Daily-Backfill-Prices.ps1"
$DailyChip   = ".\tools\daily\Daily-Backfill-Chip.ps1"
$FullMarket  = ".\tools\daily\Backfill-FullMarket.ps1"
$RatePlan    = ".\tools\daily\Backfill-RatePlan.ps1"

function Run-Prices {
  if(Test-Path $DailyPrices){
    & pwsh -NoProfile -File $DailyPrices -Start $Start -End $End
  } elseif(Test-Path $RatePlan){
    $env:BACKFILL_DATASETS='prices'
    & pwsh -NoProfile -File $RatePlan -Start $Start -End $End
  } else { throw "找不到 prices 的入口（$DailyPrices / $RatePlan）" }
}

function Run-Chip {
  if(Test-Path $DailyChip){
    & pwsh -NoProfile -File $DailyChip -Start $Start -End $End
  } elseif(Test-Path $RatePlan){
    $env:BACKFILL_DATASETS='chip'
    & pwsh -NoProfile -File $RatePlan -Start $Start -End $End
  } else { throw "找不到 chip 的入口（$DailyChip / $RatePlan）" }
}

function Run-PER {
  # 前置：必須先有 prices / chip 的 .ok（同一日期）
  $need = @('prices','chip') | Where-Object { -not (Has-Ok $_ $Start) }
  if($need){ throw "per $Start 前置 .ok 未齊：$($need -join ', ')" }

  if(Test-Path $FullMarket){
    & pwsh -NoProfile -File $FullMarket -Start $Start -End $End
  } elseif(Test-Path $RatePlan){
    # 部分環境 PER 也可由 rateplan 觸發（若支援）
    $env:BACKFILL_DATASETS='per'
    & pwsh -NoProfile -File $RatePlan -Start $Start -End $End
  } else { throw "找不到 PER 聚合器（$FullMarket / $RatePlan）" }
}

function Run-One([string]$ds,[scriptblock]$invoke){
  if($SkipIfOk -and (Has-Ok $ds $Start)){
    Write-Host "skip $ds $Start（已有 .ok）" -ForegroundColor DarkGray
    return
  }
  With-Lock "$ds-$Start" {
    & $invoke
    Write-Ok $ds $Start
  }
}

switch($Dataset){
  'prices' { Run-One 'prices' { Run-Prices } }
  'chip'   { Run-One 'chip'   { Run-Chip   } }
  'per'    { Run-One 'per'    { Run-PER    } }
  'all'    {
    & $PSCommandPath -Start $Start -End $End -Dataset prices -SkipIfOk:$SkipIfOk -CheckpointRoot $CheckpointRoot -RootPath $RootPath -UniversePath $UniversePath
    & $PSCommandPath -Start $Start -End $End -Dataset chip   -SkipIfOk:$SkipIfOk -CheckpointRoot $CheckpointRoot -RootPath $RootPath -UniversePath $UniversePath
    & $PSCommandPath -Start $Start -End $End -Dataset per    -SkipIfOk:$SkipIfOk -CheckpointRoot $CheckpointRoot -RootPath $RootPath -UniversePath $UniversePath
  }
}
