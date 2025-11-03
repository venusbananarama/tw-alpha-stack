[CmdletBinding(SupportsShouldProcess=$true)]
param(
  [string]$RootPath = "C:\AI\tw-alpha-stack",
  [string]$TaskName = "AlphaCity-DailyEOD",
  [string]$DailyTime = "21:05",  # local time, HH:mm
  [ValidateSet("base","override")] [string]$RulesMode = "base",
  [string]$RulesOverride = "",
  [switch]$AsSystem,   # run as LocalSystem (highest)
  [switch]$Remove,     # unregister task
  [switch]$RunNow,     # start once after register
  [switch]$Force       # overwrite existing
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$showVerbose = $PSBoundParameters.ContainsKey('Verbose')

function _Echo($m){ Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $m) }

if ($Remove) {
  Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction SilentlyContinue
  _Echo "Removed task: $TaskName"
  return
}

if (-not (Test-Path -LiteralPath $RootPath)) { throw "RootPath not found: $RootPath" }
$script = Join-Path $RootPath 'tools\Invoke-Nightly.ps1'
if (-not (Test-Path -LiteralPath $script)) { throw "Missing entry: $script (請先安裝 Invoke-Nightly.ps1)" }

# Locate pwsh
$pwsh = (Get-Command pwsh -ErrorAction SilentlyContinue | Select-Object -First 1).Source
if (-not $pwsh) { $pwsh = Join-Path $PSHOME 'pwsh.exe' }
if (-not (Test-Path -LiteralPath $pwsh)) { throw "pwsh not found" }

# Build arguments (keep quoting strict; do NOT add -Verbose to param(), use common parameter)
$argList = @(
  '-NoProfile','-NonInteractive','-ExecutionPolicy','Bypass',
  '-File',("`"{0}`"" -f $script),
  '-Root',("`"{0}`"" -f $RootPath),
  '-RulesMode', $RulesMode
)
if ($RulesMode -eq 'override' -and $RulesOverride) { $argList += @('-RulesOverride',("`"{0}`"" -f $RulesOverride)) }
# 讓日誌更完整（使用共用 -Verbose，而非自定參數）
if ($showVerbose) { $argList += '-Verbose' }
$arguments = ($argList -join ' ')

# Trigger at DailyTime (local)
try   { $ts = [TimeSpan]::Parse($DailyTime) }
catch { throw "DailyTime 格式錯誤（需 HH:mm）：$DailyTime" }
$today = [datetime]::Today + $ts
$trigger = New-ScheduledTaskTrigger -Daily -At $today

# Action (WorkingDirectory 若有支援則帶入)
$canWD = (Get-Command New-ScheduledTaskAction).Parameters.ContainsKey('WorkingDirectory')
$action = if ($canWD) {
  New-ScheduledTaskAction -Execute $pwsh -Argument $arguments -WorkingDirectory $RootPath
} else {
  New-ScheduledTaskAction -Execute $pwsh -Argument $arguments
}

# Settings：避免重入、允許電池狀態、3 小時上限
$settings = New-ScheduledTaskSettingsSet `
  -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
  -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 3)

# Principal：預設用當前使用者；AsSystem 走 LocalSystem
if ($AsSystem) {
  $principal = New-ScheduledTaskPrincipal -UserId "SYSTEM" -LogonType ServiceAccount -RunLevel Highest
  Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Description "AlphaCity Daily EOD (SSOT=$RulesMode)" -Principal $principal -Force:$Force | Out-Null
} else {
  Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings `
    -Description "AlphaCity Daily EOD (SSOT=$RulesMode)" -Force:$Force | Out-Null
}

_Echo ("Registered: {0}  At: {1}" -f $TaskName,$DailyTime)
_Echo ("Action: {0} {1}" -f $pwsh,$arguments)

# Show schedule info
$info = Get-ScheduledTaskInfo -TaskName $TaskName
_Echo ("NextRun: {0}  LastRun: {1}  LastResult: {2}" -f $info.NextRunTime,$info.LastRunTime,$info.LastTaskResult)

if ($RunNow) {
  Start-ScheduledTask -TaskName $TaskName
  _Echo "Triggered run now."
}
