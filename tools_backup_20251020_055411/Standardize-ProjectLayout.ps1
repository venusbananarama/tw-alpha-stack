Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
param([switch]$Apply)
$TAG = Get-Date -Format 'yyyyMMdd_HHmm'
$ARCH = "./_archive/$TAG"

function Move-Safe($src,$dst){
  if ($Apply){
    New-Item -ItemType Directory -Force (Split-Path $dst) > $null
    Move-Item -LiteralPath $src -Destination $dst -Force
  } else {
    Write-Output ("MOVE '{0}' -> '{1}'" -f $src, $dst)
  }
}

# B-1) 封存 pkgB_profile_v6
if (Test-Path ./pkgB_profile_v6) { Move-Safe './pkgB_profile_v6' "$ARCH/pkgB_profile_v6/" }

# B-2) 併入 src
foreach($d in 'ingest','model_pipeline','modules'){
  if (Test-Path "./$d") { Move-Safe "./$d" "./src/alphacity/$d/" }
}

# B-3) out -> reports/tmp
if (Test-Path ./out) { Move-Safe './out' './reports/tmp/' }

# B-4) 根目錄包裝器 → launchers/（白名單保留 QuickStart_*、Check-FMStatus）
$keep = '^QuickStart_.*|^Check-FMStatus'
Get-ChildItem -File -Path . -Include *.ps1,*.cmd | ForEach-Object {
  $n = $_.Name
  if ($n -notmatch $keep -and $n -notmatch '^AlphaCity\.Profile$' -and $n -notmatch '^Invoke-AlphaVerification\.fix\.ps1$') {
    Move-Safe $_.FullName "./launchers/$n"
  }
}
# 專門處理 fix 檔：封存
if (Test-Path ./Invoke-AlphaVerification.fix.ps1) { Move-Safe './Invoke-AlphaVerification.fix.ps1' "$ARCH/Invoke-AlphaVerification.fix.ps1" }

# B-5) 分散的 Python 腳本 → scripts/
$pyTargets = @('backtest_patch_core_weekly_fri.py','check_factors.py','data_health_check.py','grid_run.py',
  'longonly_topN_v2.py','make_watchlists.py','nav_cleaner.py','run_all_backtests.py',
  'run_all_v2_core.py','run_batch_backtests.py','standardize_project.py',
  'summarize_performance.py','verify_env.py')
foreach($p in $pyTargets){
  Get-ChildItem -Path . -Filter $p -File -ErrorAction SilentlyContinue | % { Move-Safe $_.FullName "./scripts/$($_.Name)" }
}

# B-6) 報表目錄統一：把 data/reports 搬到 reports
if (Test-Path ./data/reports){ Move-Safe './data/reports' './reports/_migrated_from_data/' }

# C-1) make_report_safe → scripts/reports/
if (Test-Path ./make_report_safe) { Move-Safe './make_report_safe' './scripts/reports/make_report_safe/' }

# C-2) install_tw_alpha_reporting.* → scripts/install/
Get-ChildItem -Path . -Filter "install_tw_alpha_reporting*" -File -ErrorAction SilentlyContinue | % {
  Move-Safe $_.FullName "./scripts/install/$($_.Name)"
}

# C-3) Check-FMStatus.ps1 → tools/
if (Test-Path ./Check-FMStatus.ps1) { Move-Safe './Check-FMStatus.ps1' './tools/Check-FMStatus.ps1' }

# C-4) _env_current → configs/
if (Test-Path ./_env_current) { Move-Safe './_env_current' './configs/_env_current' }

# C-5) 新文件/新文字文件 → _archive/$TAG/misc/
$miscFiles = @('新文件 1','新文字文件')
foreach($m in $miscFiles){
  if (Test-Path "./$m"){ Move-Safe "./$m" "$ARCH/misc/$m" }
}

if (-not $Apply){
  Write-Output '--- Dry-Run 完成：上面列出將執行的 MOVE；加上 -Apply 才會真正搬移。'
} else {
  Write-Output ("--- 已套用：封存位置 -> {0}" -f $ARCH)
}

