<#
    .SYNOPSIS
        Creates or updates Windows Task Scheduler entries for daily ETL and weekly backtest jobs.

    .DESCRIPTION
        This script uses the built‑in schtasks.exe utility to register two scheduled tasks:

        1. AlphaCity_DailyETL  – Runs every day at 02:00 and executes the run_daily_etl.ps1 script.
        2. AlphaCity_WeeklyBacktest – Runs every Friday at 18:00 and executes the run_weekly_backtest.ps1 script.

        If tasks with the same names already exist, they will be updated (/F switch). This script assumes
        PowerShell 7 (pwsh) is available on the system PATH and that the target scripts exist relative
        to the provided root path.

    .PARAMETER Root
        The root folder of the tw‑alpha‑stack project. Defaults to G:\AI\tw-alpha-stack.

    .EXAMPLE
        # Create or update scheduled tasks using default paths
        ./setup_schedules.ps1

        # Create tasks for a different root location
        ./setup_schedules.ps1 -Root "D:\Projects\tw-alpha-stack"

    .NOTES
        Because this script uses schtasks.exe, it must be run on Windows with appropriate
        privileges to register scheduled tasks. For per‑user tasks, run in a standard session;
        for system‑wide tasks, run elevated.
#>

param(
    [string]$Root = "G:\AI\tw-alpha-stack"
)

<#
    Creates two scheduled tasks using schtasks.exe.  By passing each command-line switch as a
    separate token, this avoids quoting issues when multiple modifiers are combined (e.g. /D and /ST).
#>

# Define task names
$dailyTaskName  = 'AlphaCity_DailyETL'
$weeklyTaskName = 'AlphaCity_WeeklyBacktest'

# Build the full command lines for the scheduled tasks.  When registering
# a scheduled task with schtasks.exe, the entire command string passed to
# /TR must be enclosed in its own quotes; otherwise, individual
# arguments such as -NoProfile may be misinterpreted as switches to schtasks
# itself.  We therefore assemble the command and then wrap it in
# double‑quotes.
$unquotedDaily  = "pwsh -NoProfile -ExecutionPolicy Bypass -File `"$Root\scripts\noagent\run_daily_etl.ps1`" -Root `"$Root`""
$unquotedWeekly = "pwsh -NoProfile -ExecutionPolicy Bypass -File `"$Root\scripts\noagent\run_weekly_backtest.ps1`" -Root `"$Root`""
$dailyAction  = '"' + $unquotedDaily  + '"'
$weeklyAction = '"' + $unquotedWeekly + '"'

# Create or update the daily ETL task (runs every day at 02:00)
$argsDaily = @('/Create', '/TN', $dailyTaskName, '/TR', $dailyAction, '/SC', 'DAILY', '/ST', '02:00', '/F')
Start-Process -FilePath 'schtasks.exe' -ArgumentList $argsDaily -NoNewWindow -Wait

# Create or update the weekly backtest task (runs every Friday at 18:00)
$argsWeekly = @('/Create', '/TN', $weeklyTaskName, '/TR', $weeklyAction, '/SC', 'WEEKLY', '/D', 'FRI', '/ST', '18:00', '/F')
Start-Process -FilePath 'schtasks.exe' -ArgumentList $argsWeekly -NoNewWindow -Wait

Write-Host "Scheduled tasks have been created or updated successfully."