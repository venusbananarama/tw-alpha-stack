@echo off
setlocal
set PY=.\.venv\Scripts\python.exe
if not exist "%PY%" set PY=python
%PY% run_all_backtests.py %*
endlocal
