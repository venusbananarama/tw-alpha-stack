param(
  [string] $TaskNameAM = "AlphaCity Verify v6.3 AM",
  [string] $TaskNamePM = "AlphaCity Verify v6.3 PM"
)
$ErrorActionPreference = "Stop"; Set-StrictMode -Version Latest
Get-ScheduledTask -TaskName $TaskNameAM -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$false
Get-ScheduledTask -TaskName $TaskNamePM -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$false
Write-Host "[OK] Unregistered tasks (if existed): $TaskNameAM, $TaskNamePM"