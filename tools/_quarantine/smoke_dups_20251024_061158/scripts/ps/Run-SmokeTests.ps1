param(
    [string]$PythonExe = ".\.venv\Scripts\python.exe",
    [string]$Config = "configs\backtest_topN_fixed.yaml",
    [string]$FactorsPath = "G:\AI\datahub\alpha\alpha_factors_fixed.parquet",
    [string]$TmpDir = "_smoketest"
)

Write-Host "=== AlphaCity 升級護欄：冒煙測試開始 ===" -ForegroundColor Cyan

if (!(Test-Path $TmpDir)) {
    New-Item -ItemType Directory -Path $TmpDir | Out-Null
}

$tests = @(
    @{
        Name = "weekly_factors_check"
        Cmd  = "$PythonExe scripts\weekly_factors_check.py --factors 'composite_score mom_252_21 vol_20' --out $TmpDir\_weekly --start 2019-01-01 --end 2019-12-31 --factors-path $FactorsPath --config $Config"
    },
    @{
        Name = "longonly_topN"
        Cmd  = "$PythonExe backtest\longonly_topN.py --factors $FactorsPath --out-dir $TmpDir\_longonly --config $Config"
    },
    @{
        Name = "tech_grid"
        Cmd  = "$PythonExe run_batch_backtests.py --grid-yaml configs\batch_grid_minimal.yaml --backtest-cmd 'python backtest/longonly_topN.py --factors {factors} --out-dir {out_dir} --config {config} {extra}' --out-root $TmpDir\_grid --reports no"
    }
)

$failures = @()
foreach ($t in $tests) {
    Write-Host "`n[TEST] $($t.Name) ..." -ForegroundColor Yellow
    try {
        Invoke-Expression $t.Cmd
        if ($LASTEXITCODE -ne 0) {
            throw "ExitCode=$LASTEXITCODE"
        }
        Write-Host "[PASS] $($t.Name)" -ForegroundColor Green
    }
    catch {
        Write-Host "[FAIL] $($t.Name): $_" -ForegroundColor Red
        $failures += $t.Name
    }
}

if ($failures.Count -eq 0) {
    Write-Host "`n=== 🟢 全部冒煙測試通過，可以安心使用最新版 ===" -ForegroundColor Green
    exit 0
} else {
    Write-Host "`n=== 🔴 測試失敗：$($failures -join ', ') ===" -ForegroundColor Red
    Write-Host "建議回退到 strict constraints (env_constraints-311-strict.txt)" -ForegroundColor Magenta
    exit 1
}
