@echo off
call "%~dp0stack_paths.cmd"

echo [Clean] Deleting old reports...
del /Q "%PROJ_ROOT%\reports\*.*"
echo [DONE] Reports folder cleaned.

pause
