param([string]$Root = ".")
# 檢查是否具管理員權限
$IsAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()
).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
if (-not $IsAdmin) {
  Write-Warning "⚠️ 建議以系統管理員身分執行，以避免排程註冊失敗。"
}

$pwsh = (Get-Command pwsh).Source

# 先清掉舊的同名任務，避免衝突
$tasks = "AlphaCity_DailyETL","AlphaCity_Daily_Quick","AlphaCity_WeeklyBacktest","AlphaCity_Weekly_Strict"
foreach($t in $tasks){
  try { Unregister-ScheduledTask -TaskName $t -Confirm:$false -ErrorAction SilentlyContinue } catch {}
}

# Daily Quick
$actionDaily = New-ScheduledTaskAction -Execute $pwsh -Argument "-NoProfile -NoLogo -Command `"cd '$Root'; .\scripts\ps\Invoke-AlphaVerification.ps1 -Quick -Symbol 2330.TW -Workers 6 -Qps 1.6 -CalendarCsv .\cal\trading_days.csv`""
$triggerDaily = New-ScheduledTaskTrigger -Daily -At 19:30
Register-ScheduledTask -TaskName "AlphaCity_Daily_Quick" -Action $actionDaily -Trigger $triggerDaily -RunLevel Highest -Force | Out-Null

# Weekly Strict
$actionWeekly = New-ScheduledTaskAction -Execute $pwsh -Argument "-NoProfile -NoLogo -Command `"cd '$Root'; .\scripts\ps\Invoke-AlphaVerification.ps1 -Quick -Symbol 2330.TW -Workers 6 -Qps 1.6 -CalendarCsv .\cal\trading_days.csv -StrictExitCode -DisallowNoopSingle`""
$triggerWeekly = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Friday -At 21:00
Register-ScheduledTask -TaskName "AlphaCity_Weekly_Strict" -Action $actionWeekly -Trigger $triggerWeekly -RunLevel Highest -Force | Out-Null

Write-Host "[OK] AlphaCity verification tasks registered."