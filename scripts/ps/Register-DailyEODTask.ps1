#requires -Version 7.2
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$Global:OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$PSDefaultParameterValues['*:Encoding'] = 'utf8'
try { chcp 65001 | Out-Null } catch {}

param([string]$Time='16:20',[string]$Script='.\\scripts\\ps\\Invoke-FMDaily.ps1 -Mode eod -Strict')
Set-ACEncoding
$action = New-ScheduledTaskAction -Execute 'pwsh.exe' -Argument "-NoProfile -File `"$Script`""
$trigger = New-ScheduledTaskTrigger -Daily -At ([datetime]::ParseExact($Time,'HH:mm',$null))
Register-ScheduledTask -TaskName 'AlphaCity-EOD' -Action $action -Trigger $trigger -Description 'AlphaCity daily EOD DAG (UTF-8)'
Write-Host "Registered 'AlphaCity-EOD' at $Time"
