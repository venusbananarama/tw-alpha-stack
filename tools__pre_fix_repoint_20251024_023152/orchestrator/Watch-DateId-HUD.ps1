[CmdletBinding()]
param(
  [string]$Tag = "",                # 只看指定 Tag（留空 = 最新）
  [int]$RefreshSec = 2,             # 更新頻率（秒）
  [int]$RpmWindowSec = 60,          # rpm 粗估視窗（秒）
  [string]$CheckpointPath = ".\state\dateid_checkpoint.json"
)

$ErrorActionPreference='Stop'
Set-StrictMode -Version Latest
$host.UI.RawUI.WindowTitle = "Date-ID HUD"

function Get-LatestLog([string]$tag){
  $pat = if([string]::IsNullOrWhiteSpace($tag)){ 'fullmarket_maxrate_*_*.log' } else { "fullmarket_maxrate_${tag}_*.log" }
  Get-ChildItem .\reports -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -like $pat } |
    Sort-Object LastWriteTime -Desc | Select-Object -First 1
}

function Parse-Headers([string]$logPath){
  if(-not (Test-Path $logPath)){ return @() }
  @( Select-String -Path $logPath -Pattern '^===\s+(\d{4}-\d{2}-\d{2})\s+->\s+(\d{4}-\d{2}-\d{2})\s+===' -ErrorAction SilentlyContinue )
}

# rpm 估計用的時間/大小樣本
$hist = New-Object System.Collections.Generic.List[pscustomobject]

Write-Host "[HUD] 啟動；Refresh=$RefreshSec s, RPMwindow=$RpmWindowSec s" -ForegroundColor Cyan
while($true){
  $lf = Get-LatestLog -tag $Tag
  if(-not $lf){ Write-Host "[HUD] 尚無 fullmarket log，等候…" -ForegroundColor Yellow; Start-Sleep $RefreshSec; continue }

  $logPath = $lf.FullName
  $size = (Get-Item $logPath).Length
  $tsNow = Get-Date

  # 保留最近 RpmWindowSec 的尺寸樣本，估計 bytes/s → 行/秒（保守，以 bytes/s 展示）
  $hist.Add([pscustomobject]@{ Ts=$tsNow; Size=$size }) | Out-Null
  $hist = $hist | Where-Object { ($tsNow - $_.Ts).TotalSeconds -le $RpmWindowSec }

  $bytesPerSec = 0
  if($hist.Count -ge 2){
    $h0 = $hist[0]; $h1 = $hist[$hist.Count-1]
    $dt = [math]::Max(1, ($h1.Ts - $h0.Ts).TotalSeconds)
    $bytesPerSec = ($h1.Size - $h0.Size) / $dt
  }
  $bytesPerMin = [math]::Round($bytesPerSec*60,0)

  # 解析狀態
  $hdrs   = Parse-Headers $logPath
  $wins   = $hdrs.Count
  $exitOk = (Select-String -Path $logPath -Pattern '\[Invoke\]\s+exit=0' -ErrorAction SilentlyContinue | Measure-Object).Count
  $http402= (Select-String -Path $logPath -SimpleMatch 'HTTP 402' -ErrorAction SilentlyContinue | Measure-Object).Count
  $hbTail = (Select-String -Path $logPath -Pattern '^\[HB\]\s+\d{2}:\d{2}:\d{2}' -AllMatches -ErrorAction SilentlyContinue | Select-Object -Last 1)

  $curWin = ""
  if($wins -gt 0){ $m=$hdrs[$wins-1].Matches; $curWin = "{0}->{1}" -f $m.Groups[1].Value,$m.Groups[2].Value }
  $lastCompleted = ""
  if($wins -ge 2){ $m=$hdrs[$wins-2].Matches; $lastCompleted = $m.Groups[2].Value }

  $ckLast = ""
  if(Test-Path $CheckpointPath){
    try { $ck = Get-Content $CheckpointPath -Raw | ConvertFrom-Json; $ckLast = $ck.last_end } catch {}
  }

  Clear-Host
  Write-Host "================= Date-ID HUD =================" -ForegroundColor Cyan
  Write-Host ("Log    : {0}" -f $logPath)
  Write-Host ("Updated: {0:s}" -f (Get-Item $logPath).LastWriteTime)
  Write-Host ("Size   : {0:N0} KB  (Δ≈{1:N0} bytes/min)" -f ($size/1KB), $bytesPerMin)
  Write-Host ""
  Write-Host ("Windows: {0}   ExitOK: {1}   HTTP402: {2}" -f $wins, $exitOk, $http402)
  Write-Host ("Current: {0}" -f ($curWin ?? ""))
  Write-Host ("PrevEnd: {0}" -f ($lastCompleted ?? ""))
  Write-Host ("CK last_end: {0}" -f ($ckLast ?? "(無)")) -ForegroundColor Yellow
  if($hbTail){ Write-Host ("HB Tail: {0}" -f $hbTail.Line) }

  Write-Host "================================================"
  Start-Sleep -Seconds $RefreshSec
}
