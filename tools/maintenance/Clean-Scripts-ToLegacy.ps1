# Clean-Scripts-ToLegacy.ps1
# 用途：把 scripts 底下「非核心」的 .py/.sql 檔搬到 scripts\legacy\
# 用法：
#   1) 只預覽：pwsh -NoProfile -File .\tools\maintenance\Clean-Scripts-ToLegacy.ps1 -Root . -WhatIf
#   2) 實際搬動：pwsh -NoProfile -File .\tools\maintenance\Clean-Scripts-ToLegacy.ps1 -Root .

param(
    [string]$Root = ".",
    [switch]$WhatIf
)

$ErrorActionPreference = "Stop"

Set-Location $Root
$ScriptsDir = Join-Path $Root "scripts"
if (-not (Test-Path $ScriptsDir)) {
    throw "找不到 scripts 目錄：$ScriptsDir"
}

$LegacyDir = Join-Path $ScriptsDir "legacy"
if (-not (Test-Path $LegacyDir)) {
    New-Item -ItemType Directory -Path $LegacyDir | Out-Null
}

# ---------------- 核心檔名（只認檔名，不含路徑） ----------------
$KeepFileNames = @(
    'README.txt',
    '__init__.py',

    # FinMind 共用 + 主線抓資料
    'p1_finmind_common.py',
    'finmind_backfill.py',
    'fm_dateid_fetch.py',
    'fm_dateid_fetch_fallback.py',
    'p1_backfill_ingest_ok_from_ledger.py',
    'build_investable_universe.py',
    'import_boss_yearly_history.py',
    'inspect_universe_from_silver.py',

    # Gate / preflight / WF runner
    'p1_preflight_check.py',
    'wf_gate_helper.py',
    'wf_runner.py',
    'wf_runner_core.py',
    'wf_runner_safe.py',

    # 因子層主線 / 工具
    'factor_registry.py',
    'factor_eval.py',
    'factor_status.py',
    'factor_engine.py',
    'factor_slo_lib.py',
    'factor_slo_preview.py',
    'factor_corr.py',
    'factor_combo.py',
    'compose_factors_to_wf.py',

    # 健康檢查 / 驗證
    'check_factors.py',
    'data_health_check.py',
    'debug_weekly_snapshot.py',
    'diagnose_weekly.py',
    'validate_silver.py',
    'validate_factors.py',
    'weekly_factors_check.py',
    'verify_env.py'
)

# ---------------- 需要保留的相對路徑（含子資料夾） ----------------
$KeepRelativePaths = @(
    # weekly anchor / as-of 檢查
    'checks\check_asof_weekly.py',
    'checks\verify_weekly_anchor.py',

    # dateID pipeline（預留）
    'pipelines\fetch_dateid_for_universe.py'
)

# ---------------- 依名稱模式保留（regexp） ----------------
$KeepNamePatterns = @(
    '^factor_.*\.py$'            # 所有 factor_* 工具
)

function Test-IsCoreScript {
    param(
        [string]$RelativePath,
        [string]$Name
    )

    # 已在 legacy 底下的就略過（不再搬動）
    if ($RelativePath -like 'legacy\*') { return $true }

    if ($KeepFileNames -contains $Name) { return $true }
    if ($KeepRelativePaths -contains $RelativePath) { return $true }

    foreach ($pat in $KeepNamePatterns) {
        if ($Name -match $pat) { return $true }
    }

    return $false
}

Write-Host "== Clean-Scripts-ToLegacy ==" -ForegroundColor Cyan
Write-Host "Root     = $Root"
Write-Host "Scripts  = $ScriptsDir"
Write-Host "Legacy   = $LegacyDir"
if ($WhatIf) {
    Write-Host "[模式] WhatIf（只顯示不搬動）" -ForegroundColor Yellow
} else {
    Write-Host "[模式] 實際搬動檔案" -ForegroundColor Yellow
}

$allFiles = Get-ChildItem $ScriptsDir -File -Recurse |
            Where-Object { $_.Extension -in '.py', '.sql', '.txt' }

$moveList = @()

foreach ($f in $allFiles) {
    $rel = $f.FullName.Substring($ScriptsDir.Length + 1)
    if (Test-IsCoreScript -RelativePath $rel -Name $f.Name) {
        continue
    }
    $moveList += [pscustomobject]@{
        Name         = $f.Name
        RelativePath = $rel
        FullName     = $f.FullName
    }
}

Write-Host ("共找到 {0} 個檔案，其中 {1} 個將搬到 legacy" -f $allFiles.Count, $moveList.Count) -ForegroundColor Cyan

if (-not $moveList) {
    Write-Host "沒有需要搬動的檔案，結束。" -ForegroundColor Green
    return
}

foreach ($item in $moveList) {
    $src = $item.FullName
    $dst = Join-Path $LegacyDir $item.RelativePath
    $dstDir = Split-Path -Parent $dst
    if (-not (Test-Path $dstDir)) {
        New-Item -ItemType Directory -Path $dstDir -Force | Out-Null
    }

    if ($WhatIf) {
        Write-Host "[DRY] $($item.RelativePath) → legacy\$($item.RelativePath)" -ForegroundColor DarkCyan
    } else {
        Write-Host "MOVE  $($item.RelativePath) → legacy\$($item.RelativePath)" -ForegroundColor DarkYellow
        Move-Item -Path $src -Destination $dst -Force
    }
}

if (-not $WhatIf) {
    Write-Host "搬動完成，建議再人工掃一眼 scripts\ 與 scripts\legacy\。" -ForegroundColor Green
}
