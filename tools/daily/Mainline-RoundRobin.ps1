#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [Parameter(Mandatory)][string]$Start,
  [Parameter(Mandatory)][string]$End,

  [ValidateSet("prices","chip","per","dividend")]
  [string[]]$Order = @("prices","chip","per","dividend"),

  [string]$StartPrices   = "2015-04-18",
  [string]$StartChip     = "2015-04-04",
  [string]$StartPER      = "2015-04-15",
  [string]$StartDividend = "2004-01-01",

  [string]$UniverseFile = ".\configs\investable_universe.txt",
  [double]$Qps = 1.33, [int]$Rpm = 80,
  [int]$BatchSize = 80, [int]$MaxConcurrency = 1,
  [int]$MaxRetries = 3, [int]$RetryDelaySec = 10,

  [switch]$SkipIfOk = $true,
  [int]$SleepBetweenDatasetsMs = 0
)

$ErrorActionPreference='Stop'
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }

$OVERLAY = Join-Path $PSScriptRoot 'Backfill-history-ok-checkpoint.ps1'
if(!(Test-Path $OVERLAY)){ throw "找不到 overlay：$OVERLAY" }

# 工具
function Has-Ok([string]$ds,[datetime]$d){
  $p = Join-Path '_state\mainline' $ds
  Test-Path (Join-Path $p ($d.ToString('yyyy-MM-dd') + '.ok'))
}
function NextStr([datetime]$d){ $d.ToString('yyyy-MM-dd') }

# 邊界
$S=[datetime]$Start; $E=[datetime]$End
$today=(Get-Date).Date; if($E -gt $today){ $E = $today }

# 為每個 dataset 建立「獨立游標」
$cursors = @{
  prices   = [datetime]$StartPrices
  chip     = [datetime]$StartChip
  per      = [datetime]$StartPER
  dividend = [datetime]$StartDividend
}
# 全域下界保護：各自游標不得早於全域 Start
foreach($k in $cursors.Keys){ if($cursors[$k] -lt $S){ $cursors[$k] = $S } }

Write-Host ("[RoundRobin v2] 直到 {0} (半開) | order={1} | Qps={2} | RPM={3}" -f $E.ToString('yyyy-MM-dd'), ($Order -join '→'), $Qps, $Rpm) -ForegroundColor Cyan
Write-Host ("初始游標  prices={0}  chip={1}  per={2}  dividend={3}" -f (NextStr $cursors.prices),(NextStr $cursors.chip),(NextStr $cursors.per),(NextStr $cursors.dividend)) -ForegroundColor DarkCyan

# 主循環：直到所有游標都達到 End
while ($true) {
  $progressed = $false

  foreach($ds in $Order){
    $cur = $cursors[$ds]
    if ($cur -ge $E) { continue }  # 此 dataset 已完成

    # 已落 ok 的日子（可能來自先前跑過）→ 快轉到下一天
    if ($SkipIfOk) {
      while ($cur -lt $E -and (Has-Ok $ds $cur)) { $cur = $cur.AddDays(1) }
      $cursors[$ds] = $cur
      if ($cur -ge $E) { continue }
    }

    $one = NextStr $cur; $two = NextStr ($cur.AddDays(1))
    $flags = @(); switch($ds){
      'prices'   { $flags += '-DoPrices' }
      'chip'     { $flags += '-DoChip' }
      'per'      { $flags += '-DoPER' }
      'dividend' { $flags += '-DoDividend' }
    }

    Write-Host ("   ▶ {0} {1}" -f $ds,$one) -ForegroundColor Yellow
    & pwsh -NoProfile -File $OVERLAY `
      -Start $one -End $two @flags `
      -UniverseFile $UniverseFile -Qps $Qps -BatchSize $BatchSize -MaxConcurrency $MaxConcurrency -MaxRetries $MaxRetries -RetryDelaySec $RetryDelaySec `
      -SpeedMode rpm -Rpm $Rpm

    if ($LASTEXITCODE -ne 0) {
      Write-Warning ("   ⚠ {0} {1} 失敗（exit={2}），先換下一個" -f $ds,$one,$LASTEXITCODE)
    } else {
      # 成功 → 該 dataset 游標 +1 天
      $cursors[$ds] = $cur.AddDays(1)
      $progressed = $true
    }

    if($SleepBetweenDatasetsMs -gt 0){ Start-Sleep -Milliseconds $SleepBetweenDatasetsMs }
  }

  # 全部都無法前進 → 結束
  $allDone = -not ($cursors.GetEnumerator() | Where-Object { $_.Value -lt $E })
  if ($allDone -or -not $progressed) { break }
}

Write-Host ("完成游標  prices={0}  chip={1}  per={2}  dividend={3}" -f (NextStr $cursors.prices),(NextStr $cursors.chip),(NextStr $cursors.per),(NextStr $cursors.dividend)) -ForegroundColor DarkCyan
Write-Host "[RoundRobin v2] 完成" -ForegroundColor Green
