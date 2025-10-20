param(
  [string]$UniversePath=".\\configs\\derived\\universe_ids_only.txt",
  [int]$BatchSize=50,
  [int]$LookbackDays=7
)
$ROOT=Resolve-Path "$PSScriptRoot\\.."; Set-Location $ROOT
if(-not $env:FINMIND_THROTTLE_RPM){ $env:FINMIND_THROTTLE_RPM='6' }
if(-not $env:FINMIND_KBAR_INTERVAL){ $env:FINMIND_KBAR_INTERVAL='5' }

$all=Get-Content $UniversePath | Where-Object { $_ -match '^\d{4}$' }
if(-not $all){ throw "Universe empty: $UniversePath" }

$today=(Get-Date).Date
$start=$today.AddDays(-[Math]::Max(1,$LookbackDays))
for($d=$start; $d -lt $today; $d=$d.AddDays(1)){
  $s=$d.ToString('yyyy-MM-dd'); $e=$d.AddDays(1).ToString('yyyy-MM-dd')
  Write-Host ("♻ Heal A: {0}→{1}" -f $s,$e)
  for($j=0; $j -lt $all.Count; $j+=$BatchSize){
    $slice=$all[$j..([Math]::Min($j+$BatchSize-1,$all.Count-1))]
    $ids=($slice -join ',')
    pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Run-DateID-Extras.ps1 -Start $s -End $e -IDs $ids -Group A
    Start-Sleep -Seconds 1
  }
}
"✅ Heal done (lookback=$LookbackDays)"
