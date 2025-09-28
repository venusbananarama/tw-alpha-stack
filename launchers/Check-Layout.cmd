@echo off
setlocal
REM Quick layout check launcher
set SCRIPT=%~dp0..\tools\Check-CanonicalLayout.ps1
pwsh -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT%" -Strict
set ERR=%ERRORLEVEL%
if %ERR% NEQ 0 (
  echo [FAIL] Canonical layout check failed. Errorlevel=%ERR%
  exit /b %ERR%
) else (
  echo [OK] Canonical layout passed.
  exit /b 0
)
