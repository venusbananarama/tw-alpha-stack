$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest
Get-CimInstance Win32_Process -Filter "name='pwsh.exe'" |
?{ $_.CommandLine -match 'tw-alpha-stack\\tools\\(orchestrator|fullmarket)\\|Run-Max-Recent\.ps1|Run-FullMarket-DateID-?MaxRate\.ps1' } |
%{ try { Stop-Process -Id $_.ProcessId -Force -ErrorAction Stop; "[stopped] PID $($_.ProcessId)" } catch {} }
Get-CimInstance Win32_Process -Filter "name='pwsh.exe'" |
?{ $_.CommandLine -match 'Run-Max-Recent\.ps1|Run-FullMarket-DateID-?MaxRate\.ps1' } |
Select ProcessId,CommandLine
