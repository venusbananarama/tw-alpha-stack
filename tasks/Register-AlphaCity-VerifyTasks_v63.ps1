param(
  [Parameter(Mandatory=$true)][string] $Root,
  [string] $TaskNameAM = "AlphaCity Verify v6.3 AM",
  [string] $TaskNamePM = "AlphaCity Verify v6.3 PM",
  [string] $StartTimeAM = "08:30",
  [string] $StartTimePM = "17:30"
)
$ErrorActionPreference = "Stop"; Set-StrictMode -Version Latest
$ps = (Get-Command powershell.exe).Source
$veri = Join-Path $Root "scripts\ps\Invoke-AlphaVerification.ps1"
if (-not (Test-Path $veri)) { throw "Not found: $veri (install patch first)" }

$cmdAM = "-NoLogo -NoProfile -ExecutionPolicy Bypass -File `"$veri`" -Start (Get-Date).AddDays(-1).ToString('yyyy-MM-dd') -End (Get-Date).ToString('yyyy-MM-dd') -SkipFull -Workers 6 -Qps 1.6 -CalendarCsv `"$Root\cal\trading_days.csv`""
$cmdPM = $cmdAM

$actAM = New-ScheduledTaskAction -Execute $ps -Argument $cmdAM
$trgAM = New-ScheduledTaskTrigger -Daily -At ([datetime]::Parse($StartTimeAM))
Register-ScheduledTask -TaskName $TaskNameAM -Action $actAM -Trigger $trgAM -Force | Out-Null

$actPM = New-ScheduledTaskAction -Execute $ps -Argument $cmdPM
$trgPM = New-ScheduledTaskTrigger -Daily -At ([datetime]::Parse($StartTimePM))
Register-ScheduledTask -TaskName $TaskNamePM -Action $actPM -Trigger $trgPM -Force | Out-Null

Write-Host "[OK] Registered tasks:`n - $TaskNameAM @ $StartTimeAM`n - $TaskNamePM @ $StartTimePM"