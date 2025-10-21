param(
  [string]$Python="./.venv/Scripts/python.exe",
  [string]$Runner="./scripts/wf_runner.py",
  [string]$Dir="./runs/wf_configs",
  [string]$Export="./reports"
)
if ($env:ALPHACITY_ALLOW -ne "1") { Write-Error "ALPHACITY_ALLOW=1 not set. Aborting." -ErrorAction Stop }

$ErrorActionPreference='Stop'; Set-StrictMode -Version Latest
if(-not (Test-Path $Export)){ New-Item -ItemType Directory -Force -Path $Export|Out-Null }
$outFile = Join-Path $Export 'gate_summary.json'

& $Python $Runner --dir $Dir --export $outFile
"Gate summary: " + (Get-Item $outFile).FullName
