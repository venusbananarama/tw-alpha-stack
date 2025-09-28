@echo off
REM Simple Windows runner (assumes you already activated your venv)
SET SCRIPT_DIR=%~dp0
python "%SCRIPT_DIR%run_model_pipeline.py" --config "%SCRIPT_DIR%config.yaml"
