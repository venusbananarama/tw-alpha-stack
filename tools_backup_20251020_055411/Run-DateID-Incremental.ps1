param(
  [string]$UniversePath = ".\configs\derived\universe_ids_only.txt",
  [int]$BatchSize = 50,
  [switch]$Strict
)
# 以腳本所在目錄回到專案根
$ROOT = Resolve-Path "$PSScriptRoot\.."
Set-Location $ROOT

# 預設節流與 KBar 顆粒
if(-not $env:FINMIND_THROTTLE_RPM){ $env:FINMIND_THROTTLE_RPM='6' }
if(-not $env:FINMIND_KBAR_INTERVAL){ $env:FINMIND_KBAR_INTERVAL='5' }

# 確保 derived IDs 存在/每日更新（不動原始清單）
$src = @(".\configs\universe.tw_all",".\configs\universe.tw_all.txt") | Where-Object { Test-Path $_ } | Select-Object -First 1
if($src){ pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Build-IDsFromUniverse.ps1 -In $src -Out $UniversePath -Overwrite }
if(-not (Test-Path $UniversePath)){ throw "Universe not found: $UniversePath" }

$all = Get-Content $UniversePath | Where-Object { $_ -match '^\d{4}$' }
if(-not $all){ throw "Universe empty: $UniversePath" }

# 水位檔：last_end（YYYY-MM-DD，表示上次處理完成到的「不含」終點日）
$wmFile = ".\reports\dateid_daily_watermark.json"
if(!(Test-Path $wmFile)){ '{"last_end":""}' | Set-Content $wmFile -Encoding UTF8 }
$wm = Get-Content $wmFile -Raw -Encoding UTF8 | ConvertFrom-Json

$today = (Get-Date).Date
# 預設從「昨天」開始；若有水位則從水位往後
$defaultStart = $today.AddDays(-1)
$startDate = $defaultStart
if($wm.last_end){
  try { $dLast = [datetime]::Parse($wm.last_end) } catch { $dLast = $defaultStart }
  if($dLast -gt $startDate){ $startDate = $dLast }
}

if($startDate -ge $today){
  "Nothing to do. start=$($startDate.ToString('yyyy-MM-dd')) >= today"
  exit 0
}

for($d=$startDate; $d -lt $today; $d=$d.AddDays(1)){
  $s = $d.ToString('yyyy-MM-dd')
  $e = $d.AddDays(1).ToString('yyyy-MM-dd')
  Write-Host ("▶ Incremental A: {0}→{1}" -f $s,$e)

  for($j=0; $j -lt $all.Count; $j+=$BatchSize){
    $slice = $all[$j..([Math]::Min($j+$BatchSize-1,$all.Count-1))]
    $ids = ($slice -join ',')
    $args = @('-Start',$s,'-End',$e,'-IDs',$ids,'-Group','A')
    if($Strict){ $args += '-FailOnError' }

    pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Run-DateID-Extras.ps1 @args
    if($LASTEXITCODE -ne 0 -and $Strict){
      Write-Error "Stop due to strict failure at $s→$e"
      exit 1
    }
    Start-Sleep -Seconds 1
  }

  # 前推水位到 e；（寬鬆模式下，即便有 FAIL 也前推，讓日更不卡；補漏交由 Heal 線負責）
  $wm.last_end = $e
  ($wm | ConvertTo-Json -Depth 2) | Set-Content $wmFile -Encoding UTF8
}
"✅ Incremental done up to $($wm.last_end)"
