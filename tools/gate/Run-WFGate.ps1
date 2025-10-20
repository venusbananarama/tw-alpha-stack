param([string]$Python="./.venv/Scripts/python.exe",[string]$Runner="./scripts/wf_runner.py",[string]$Dir="./runs/wf_configs",[string]$Export="./reports")
$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest
if(-not(Test-Path $Export)){ New-Item -ItemType Directory -Force -Path $Export|Out-Null }
& $Python $Runner --dir $Dir --export $Export
"Gate summary: " + (Resolve-Path "$Export/gate_summary.json").Path
