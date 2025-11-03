#requires -Version 7

param(
  [string]$UniversePath = ".\configs\derived\universe_ids_only.txt",
  [string]$Start = "2004-01-01",
  [string]$End   = ([datetime]::Now.ToString("yyyy-MM-dd")),
  [int]$BatchSize = 50,
  [int]$WindowDays = 7,
  [switch]$Strict
)
$ROOT = Resolve-Path "$PSScriptRoot\.."; Set-Location $ROOT
if(-not (Test-Path $UniversePath)){
  $src = @(".\configs\universe.tw_all",".\configs\universe.tw_all.txt") | ?{Test-Path $_} | Select-Object -First 1
  if($src){ pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Build-IDsFromUniverse.ps1 -In $src -Out $UniversePath -Overwrite }
}
$all = Get-Content $UniversePath | ?{ $_ -match '^\d{4}$' }
if(-not $all){ throw "Universe empty: $UniversePath" }
if(-not $env:FINMIND_THROTTLE_RPM){ $env:FINMIND_THROTTLE_RPM='6' }
if(-not $env:FINMIND_KBAR_INTERVAL){ $env:FINMIND_KBAR_INTERVAL='5' }

for($j=0; $j -lt $all.Count; $j+=$BatchSize){
  $slice=$all[$j..([math]::Min($j+$BatchSize-1,$all.Count-1))]
  $ids=($slice -join ',')
  for($d=[datetime]$Start; $d -lt [datetime]$End; $d=$d.AddDays($WindowDays)){
    $s=$d.ToString('yyyy-MM-dd'); $E=([datetime]$s).AddDays($WindowDays); if($E -gt [datetime]$End){ $E=[datetime]$End }
    $args=@('-Start',$s,'-End',($E.ToString('yyyy-MM-dd')),'-IDs',$ids,'-Group','A'); if($Strict){ $args += '-FailOnError' }
    pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\dateid\Run-DateID-Extras.ps1 @args
    if($LASTEXITCODE -ne 0 -and $Strict){ throw "Strict stop at batch $j ($s→$($E.ToString('yyyy-MM-dd')))" }
    Start-Sleep -Seconds 2
  }
}
"✅ Full-market full-history done ($Start → $End) :: $($all.Count) ids"


