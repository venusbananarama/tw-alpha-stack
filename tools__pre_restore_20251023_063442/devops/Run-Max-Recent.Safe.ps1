[CmdletBinding()]
param(
  [Parameter(Mandatory)][string]$Start,
  [Parameter(Mandatory)][string]$End,    # 半開 end：欲含當日就傳當日字串或給明日
  [int]$WindowDays=1,
  [int]$Workers=1,
  [int]$BatchSize=60,
  [switch]$NoLock
)
$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest

# 1) 日期正規化（字串 yyyy-MM-dd）
try { $Start = (Get-Date $Start).ToString('yyyy-MM-dd') } catch {}
try { $End   = (Get-Date $End).ToString('yyyy-MM-dd') }   catch {}

# 2) 鎖處理（wrapper 層）
$lockDir = '.\state'
if (Test-Path $lockDir) {
  if ($NoLock) {
    Get-ChildItem $lockDir -Filter *.lock -ErrorAction SilentlyContinue |
      Remove-Item -Force -ErrorAction SilentlyContinue
  } else {
    Get-ChildItem $lockDir -Filter *.lock -ErrorAction SilentlyContinue |
      Where-Object { $_.LastWriteTime -lt (Get-Date).AddMinutes(-30) } |
      Remove-Item -Force -ErrorAction SilentlyContinue
  }
}

# 3) 定位引擎（正名優先，否則找變體）
$engine = '.\tools\Run-FullMarket-DateID-MaxRate.ps1'
if (-not (Test-Path $engine)) {
  $alt = Get-ChildItem .\tools -Filter 'Run-FullMarket-DateID*MaxRate.ps1' -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($alt) { $engine = $alt.FullName } else { throw '找不到引擎 Run-FullMarket-DateID*MaxRate.ps1' }
}

# 4) 構造參數並執行（只傳引擎吃得懂的參數）
$ea = @(
  '-NoProfile','-ExecutionPolicy','Bypass','-File', $engine,
  '-Start', $Start, '-End', $End,
  '-BatchSize', $BatchSize, '-WindowDays', $WindowDays, '-Workers', $Workers
)
Write-Host ('CMD> pwsh ' + ($ea -join ' ')) -ForegroundColor Yellow
& (Get-Command pwsh).Source @ea
