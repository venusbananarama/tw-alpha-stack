@echo off
setlocal enableextensions
set ROOT=C:\AI\tw-alpha-stack
if not exist "%ROOT%" ( echo ERROR: %ROOT% not found. & exit /b 2 )
cd /d "%ROOT%"
where pwsh >nul 2>&1
if %ERRORLEVEL% EQU 0 ( set "PS=pwsh" ) else ( set "PS=powershell" )
set ALPHACITY_ALLOW=1
%PS% -NoProfile -ExecutionPolicy Bypass -File ".\tools\Run-DateID-Extras-Fixed.ps1" -Group All -IDsFile ".\configs\investable_universe.txt" -DataHubRoot ".\datahub" -Retries 3
set ERR=%ERRORLEVEL%
if %ERR% NEQ 0 ( echo Run-DateID-Extras finished WITH ERRORS (exit %ERR%). See reports\Run-DateID-Extras_YYYYMMDD.log ) else ( echo Run-DateID-Extras completed successfully. )
endlocal
