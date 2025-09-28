param(
    [Parameter(Mandatory=$false)][string]$Factors = "composite_score",
    [Parameter(Mandatory=$false)][string]$OutDir = "",
    [Parameter(Mandatory=$false)][string]$Start = "",
    [Parameter(Mandatory=$false)][string]$End = "",
    [Parameter(Mandatory=$false)][string]$FactorsPath = "",
    [Parameter(Mandatory=$false)][string]$Config = "configs\backtest_topN_example.yaml",
    [Parameter(Mandatory=$false)][int]$TopN = 50,
    [Parameter(Mandatory=$false)][string]$Rebalance = "W",
    [Parameter(Mandatory=$false)][double]$Costs = 0.0005,
    [Parameter(Mandatory=$false)][int]$Seed = 42,
    [switch]$NoPause
)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root
if (-not $OutDir -or $OutDir -eq "") {
    $stamp = Get-Date -Format "yyyyMMdd_HHmmss"
    $OutDir = Join-Path $root ("out\backtest_" + $stamp)
}
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null
# Transcript logging
$logPath = Join-Path $OutDir "powershell.log"
try { Start-Transcript -Path $logPath -Force | Out-Null } catch {}
$venvPy = Join-Path $root ".venv\Scripts\python.exe"
if (Test-Path $venvPy) { $python = $venvPy } else { $python = "python" }
$fixed = "backtest\longonly_topN_fixed.py"
if (Test-Path $fixed) {
    $args = @($fixed,"--factors",$Factors,"--outdir",$OutDir,"--config",$Config)
    if ($Start) { $args += @("--start",$Start) }
    if ($End) { $args += @("--end",$End) }
    if ($FactorsPath) { $args += @("--factors-path",$FactorsPath) }
    if ($TopN) { $args += @("--topn",$TopN) }
    if ($Rebalance) { $args += @("--rebalance",$Rebalance) }
    if ($Costs) { $args += @("--costs",$Costs) }
    if ($Seed) { $args += @("--seed",$Seed) }
    Write-Host "Running fixed wrapper -> original backtest ..."
    & $python $args
    if ($LASTEXITCODE -eq 0) {
        Write-Host "`n✓ Backtest finished via wrapper. Outputs in:" $OutDir
        try { Stop-Transcript | Out-Null } catch {}
        if (-not $NoPause) { Pause }
        exit 0
    } else { Write-Warning "Wrapper path failed. Trying original then fallback..." }
}
$orig = "backtest\longonly_topN.py"; $useFallback = $false
if (Test-Path $orig) {
    try {
        $args = @($orig,"--factors",$FactorsPath,"--out-dir",$OutDir,"--config",$Config)
        Write-Host "Running original backtest with classic args..."
        & $python $args
        if ($LASTEXITCODE -ne 0) { $useFallback = $true }
    } catch { $useFallback = $true }
} else { $useFallback = $true }
if ($useFallback) {
    Write-Warning "Falling back to simple TopN engine (simulate_topN.py)."
    $args = @("backtest\simulate_topN.py","--factors",$Factors,"--outdir",$OutDir,"--topn",$TopN,"--rebalance",$Rebalance,"--costs",$Costs)
    if ($Start) { $args += @("--start",$Start) }
    if ($End) { $args += @("--end",$End) }
    if ($FactorsPath) { $args += @("--factors-path",$FactorsPath) }
    Write-Host "Running:" $python $args
    & $python $args
    if ($LASTEXITCODE -ne 0) { throw "simulate_topN.py failed with exit code $LASTEXITCODE" }
    Write-Host "`n✓ Backtest finished via fallback. Outputs in:" $OutDir
}
try { Stop-Transcript | Out-Null } catch {}
if (-not $NoPause) { Pause }
