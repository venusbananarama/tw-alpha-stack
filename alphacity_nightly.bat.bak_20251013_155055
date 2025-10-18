@echo off
setlocal enableextensions
set ROOT=C:/AI/tw-alpha-stack
cd /d %ROOT%
set ALPHACITY_ALLOW=1
set PY=%ROOT%/.venv/Scripts/python.exe
for /f "tokens=1-3 delims=/ " %%a in ('date /t') do set DATE=%%c-%%a-%%b
set LOG=reports/nightly_%DATE%.log

rem 1) Preflight（schema/as-of/freshness）
"%PY%" scripts/preflight_check.py --rules rules.yaml --export reports --root . 1>>"%LOG%" 2>&1

rem 2) 建投資池（確保 universe 更新）
"%PY%" scripts/build_universe.py --config configs/universe.yaml --rules rules.yaml --out configs/investable_universe.txt --drop-empty 1>>"%LOG%" 2>&1

rem 3) Walk-forward 與 Gate
"%PY%" scripts/wf_runner.py --dir runs/wf_configs --export reports 1>>"%LOG%" 2>&1
powershell -NoProfile -ExecutionPolicy Bypass -File tools/Run-WFGate.ps1 1>>"%LOG%" 2>&1

endlocal
