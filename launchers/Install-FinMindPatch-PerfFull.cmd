@echo off
cd /d %~dp0
powershell -ExecutionPolicy Bypass -File ".\Install-FinMindPatch-PerfFull.ps1"
pause
