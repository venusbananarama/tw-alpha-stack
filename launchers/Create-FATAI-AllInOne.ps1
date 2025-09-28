# Create-FATAI-AllInOne.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

# dirs
New-Item -ItemType Directory -Force -Path (Join-Path $root "scripts") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $root "backtest") | Out-Null

# -----------------------------
# 1) weekly: check_weekly_after_patch.ps1
# -----------------------------
@'
param(
    [Parameter(Mandatory=$false)][string]$Factors = "composite_score",
    [Parameter(Mandatory=$false)][string]$OutDir = "",
    [Parameter(Mandatory=$false)][string]$Start = "",
    [Parameter(Mandatory=$false)][string]$End = "",
    [Parameter(Mandatory=$false)][string]$FactorsPath = "",
    [Parameter(Mandatory=$false)][string]$Config = "configs\backtest_topN_example.yaml"
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

if (-not $OutDir -or $OutDir -eq "") {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $OutDir = Join-Path $root ("out\weekly_check_" + $stamp)
}
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

$venvPy = Join-Path $root ".venv\Scripts\python.exe"
if (Test-Path $venvPy) { $python = $venvPy } else { $python = "python" }

$args = @("scripts\project_check.py","--factors",$Factors,"--outdir",$OutDir)
if ($Start) { $args += @("--start",$Start) }
if ($End) { $args += @("--end",$End) }
if ($FactorsPath) { $args += @("--factors-path",$FactorsPath) }
if ($Config) { $args += @("--config",$Config) }

Write-Host "Running:" $python $args
& $python $args
if ($LASTEXITCODE -ne 0) { throw "project_check.py failed with exit code $LASTEXITCODE" }
Write-Host "`n✓ Done. Outputs under: $OutDir"
'@ | Set-Content -Encoding UTF8 (Join-Path $root "check_weekly_after_patch.ps1")

# -----------------------------
# 2) weekly: scripts/project_check.py
# -----------------------------
@'
#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse, os, sys, json
from datetime import datetime
import pandas as pd
try: import yaml
except Exception: yaml = None

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--factors","-f",type=str,default="composite_score")
    p.add_argument("--outdir","-o",type=str,required=True)
    p.add_argument("--start","-s",type=str,default="")
    p.add_argument("--end","-e",type=str,default="")
    p.add_argument("--factors-path","-p",type=str,default="")
    p.add_argument("--config","-c",type=str,default="")
    return p.parse_args()

def normalize_factor_list(s): return list(dict.fromkeys([x.strip() for x in s.replace(","," ").split() if x.strip()]))

def load_config(path):
    if not path or not os.path.exists(path): return {}
    if yaml is None:
        print("[warn] pyyaml not installed; ignore config", file=sys.stderr); return {}
    try:
        with open(path,"r",encoding="utf-8") as f: return yaml.safe_load(f) or {}
    except Exception as ex:
        print(f"[warn] bad YAML {path}: {ex}", file=sys.stderr); return {}

def find_default_factors_path():
    for c in [r"G:\AI\datahub\alpha\alpha_factors_fixed.parquet",
              r"G:\AI\datahub\alpha\alpha_factors.parquet",
              r"data\alpha_factors.parquet"]:
        if os.path.exists(c): return c
