param(
  [string]$Symbol = '2330.TW',
  [int]$Workers = 6,
  [double]$Qps = 1.6,
  [string]$CalendarCsv = '.\cal\trading_days.csv',
  # 允許手動覆寫，若空則會由交易日曆自動計算（W-FRI 錨）
  [string]$Start = '',
  [string]$End   = ''
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$cmd = '.\scripts\ps\Invoke-AlphaVerification.ps1'

# === 1) 旗標偵測：SkipFull / Quick ===
$flag = $null
try {
  $paramNames = (Get-Command $cmd -ErrorAction Stop).Parameters.Keys
  if ($paramNames -contains 'SkipFull') { $flag = 'SkipFull' }
  elseif ($paramNames -contains 'Quick') { $flag = 'Quick' }
} catch { }

# === 2) 交易日曆 → 自動推算 Start/End（W-FRI 錨） ===
# 規則：
#   - End  = 今天（yyyy-MM-dd）
#   - Start= 最近一個「交易日的週五」（若今天前 14 個交易日內找不到週五，退回到最近第 5 個交易日）
function Get-WFriWindow {
  param([string]$CalPath)
  $today = (Get-Date).ToString('yyyy-MM-dd')

  # 讀 CSV，需有 'date' 欄（Normalize-Calendar.ps1 會生成）
  $cal = Import-Csv $CalPath
  $dates = $cal | Where-Object { $_.date -le $today } | Select-Object -ExpandProperty date
  if (-not $dates -or $dates.Count -lt 3) {
    throw "交易日曆不足或缺少 'date' 欄：$CalPath"
  }

  # 從最後往前找最近的"週五"交易日
  $lastN = $dates | Select-Object -Last 14
  $wfri = $null
  foreach ($d in ($lastN | Sort-Object)) {
    $dow = ([datetime]::ParseExact($d,'yyyy-MM-dd',$null)).DayOfWeek
    if ($dow -eq 'Friday') { $wfri = $d }
  }

  if (-not $wfri) {
    # 退回：最近第 5 個交易日當起點
    $wfri = ($dates | Select-Object -Last 5 | Select-Object -First 1)
  }

  return [pscustomobject]@{ Start=$wfri; End=$today }
}

if (-not $Start -or -not $End) {
  if (-not (Test-Path $CalendarCsv)) {
    throw "找不到交易日曆：$CalendarCsv（請先執行 tools\Normalize-Calendar.ps1）"
  }
  $win = Get-WFriWindow -CalPath $CalendarCsv
  if (-not $Start) { $Start = $win.Start }
  if (-not $End)   { $End   = $win.End }
}

# === 3) 建立參數（Hashtable splatting，避免位置錯位） ===
$params = @{
  Start       = $Start
  End         = $End
  Symbol      = $Symbol
  Workers     = $Workers
  Qps         = $Qps
  CalendarCsv = $CalendarCsv
}
if ($flag) { $params[$flag] = $true }

# === 4) 記錄日志（metrics\smoke_latest.log） ===
$metricsDir = Join-Path (Get-Location) 'metrics'
New-Item -ItemType Directory -Force $metricsDir | Out-Null
$logFile = Join-Path $metricsDir 'smoke_latest.log'

function Write-Log($msg) {
  $ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
  "$ts  $msg" | Add-Content -Encoding UTF8 $logFile
}

# 命令預覽（人看）
$preview = @($cmd)
foreach ($kv in $params.GetEnumerator()) {
  if ($kv.Value -is [bool]) { $preview += "-$($kv.Key)" }
  else { $preview += "-$($kv.Key) $($kv.Value)" }
}
$previewLine = ($preview -join ' ')

Write-Host ">> $previewLine" -ForegroundColor Yellow
Write-Log  "RUN  $previewLine"

# === 5) 執行與量測 ===
$sw = [System.Diagnostics.Stopwatch]::StartNew()
try {
  & $cmd @params
  $sw.Stop()
  Write-Host "✔ Weekly Factors Smoke OK ($($sw.Elapsed.ToString()))" -ForegroundColor Green
  Write-Log  "OK   elapsed=$($sw.Elapsed.ToString()) start=$Start end=$End"
} catch {
  $sw.Stop()
  Write-Host "✗ Weekly Factors Smoke FAIL ($($sw.Elapsed.ToString()))" -ForegroundColor Red
  Write-Host $_.Exception.Message -ForegroundColor Red
  Write-Log  "ERR  elapsed=$($sw.Elapsed.ToString()) err=$($_.Exception.Message)"
  throw
}
