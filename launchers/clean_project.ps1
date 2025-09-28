param(
    [string]$Root = (Get-Location).Path,
    [switch]$Delete = $false,
    [switch]$DryRun = $false
)
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$Archive = Join-Path $Root ("archive_" + $stamp)
$archiveItems = @('patch_backtest_core.py', 'patch_backtest_core_v2.py', 'patch_backtest_core_v3.py', 'patch_fix_rbdates.py', 'fix_factor_columns.py', 'fix_factor_columns_v2.py', 'patch_backtest_core_v4.py', 'replace_longonly_topN_v2.py')
$deleteItems  = @('__pycache__\core.cpython-311.pyc', '__pycache__\longonly_topN.cpython-311.pyc', '__pycache__\factors_core.cpython-311.pyc', 'twalpha\data\speed_patch.zip', 'twalpha\data\__pycache__\adapter_fatai.cpython-310.pyc', 'twalpha\data\__pycache__\downloader_bulk.cpython-310.pyc', 'twalpha\data\__pycache__\downloader_twse.cpython-310.pyc', 'twalpha\data\__pycache__\symbols_tw.cpython-310.pyc', 'twalpha\data\__pycache__\__init__.cpython-310.pyc', 'twalpha\features\__pycache__\ta_basic.cpython-310.pyc', 'twalpha\features\__pycache__\__init__.cpython-310.pyc', 'twalpha\report\__pycache__\daily_md.cpython-310.pyc', 'twalpha\report\__pycache__\__init__.cpython-310.pyc', 'twalpha\signals\__pycache__\ensemble.cpython-310.pyc', 'twalpha\signals\__pycache__\regime.cpython-310.pyc', 'twalpha\signals\__pycache__\__init__.cpython-310.pyc', 'twalpha\__pycache__\__init__.cpython-310.pyc')

if (-not $Delete -and $archiveItems.Count -gt 0 -and -not $DryRun) {
    New-Item -ItemType Directory -Force -Path $Archive | Out-Null
}

function Do-Move([string]$Item) {
    $src = Join-Path $Root $Item
    if (-not (Test-Path $src)) { Write-Host "[SKIP] not found: $src"; return }
    $dst = Join-Path $Archive $Item
    $dstDir = Split-Path $dst -Parent
    if (-not (Test-Path $dstDir)) { New-Item -ItemType Directory -Force -Path $dstDir | Out-Null }
    if ($DryRun) { Write-Host "[DRYRUN][MOVE]" $src "->" $dst; return }
    Move-Item -LiteralPath $src -Destination $dst -Force
    Write-Host "[MOVED]" $Item
}

function Do-Delete([string]$Item) {
    $src = Join-Path $Root $Item
    if (-not (Test-Path $src)) { Write-Host "[SKIP] not found: $src"; return }
    if ($DryRun) { Write-Host "[DRYRUN][DEL]" $src; return }
    Remove-Item -LiteralPath $src -Recurse -Force
    Write-Host "[DELETED]" $Item
}

Write-Host "Root =" $Root
Write-Host "Archive =" $Archive
Write-Host "Mode =" ($(if($Delete){"DELETE"} else {"ARCHIVE"})) ($(if($DryRun){"DRYRUN"} else {"REAL"}))

if ($archiveItems.Count -gt 0) {
    if ($Delete) {
        foreach ($i in $archiveItems) { Do-Delete $i }
    } else {
        foreach ($i in $archiveItems) { Do-Move $i }
    }
} else {
    Write-Host "[INFO] No archive candidates."
}

if ($deleteItems.Count -gt 0) {
    foreach ($i in $deleteItems) { Do-Delete $i }
} else {
    Write-Host "[INFO] No delete candidates."
}
