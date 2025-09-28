param(
    [Parameter(Mandatory=$false)][string]$Factors = "composite_score",
    [Parameter(Mandatory=$false)][string]$OutDir = "",
    [Parameter(Mandatory=$false)][string]$Start = "",
    [Parameter(Mandatory=$false)][string]$End = "",
    [Parameter(Mandatory=$false)][string]$FactorsPath = "",
    [Parameter(Mandatory=$false)][string]$Config = "configs\backtest_topN_example.yaml",
    [switch]$NoPause
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root
if (-not $OutDir -or $OutDir -eq "") {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $OutDir = Join-Path $root ("out\weekly_check_" + $stamp)
}
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
$logPath = Join-Path $OutDir "powershell.log"
try { Start-Transcript -Path $logPath -Force | Out-Null } catch {}
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
try { Stop-Transcript | Out-Null } catch {}
if (-not $NoPause) { Pause }
