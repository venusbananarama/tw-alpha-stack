param(
  [Parameter(Mandatory=$true)][string]$ID,
  [string]$Start = "2004-01-01",
  [string]$End   = ([datetime]::Now.ToString("yyyy-MM-dd")),
  [int]$WindowDays = 30,
  [switch]$Strict
)
$ROOT = Resolve-Path "$PSScriptRoot\.."; Set-Location $ROOT
if(-not $env:FINMIND_THROTTLE_RPM){ $env:FINMIND_THROTTLE_RPM='6' }
if(-not $env:FINMIND_KBAR_INTERVAL){ $env:FINMIND_KBAR_INTERVAL='5' }
for($d=[datetime]$Start; $d -lt [datetime]$End; $d=$d.AddDays($WindowDays)){
  $s=$d.ToString('yyyy-MM-dd'); $E=([datetime]$s).AddDays($WindowDays); if($E -gt [datetime]$End){ $E=[datetime]$End }
  $args=@('-Start',$s,'-End',($E.ToString('yyyy-MM-dd')),'-IDs',$ID,'-Group','A'); if($Strict){ $args += '-FailOnError' }
  pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Run-DateID-Extras.ps1 @args
  if($LASTEXITCODE -ne 0 -and $Strict){ throw "Strict stop at $s→$($E.ToString('yyyy-MM-dd'))" }
  Start-Sleep -Seconds 1
}
"✅ Single full-history done for $ID ($Start → $End)"
