#requires -Version 7.0
<#
  AlphaCity Repo Cleanup v1.1.1
  - 排除 .trash / datahub\_archive / .venv（避免「清垃圾桶」與誤動封存/venv）
  - 修正 manifests 單一/零項時 .Count 例外（強制陣列）
  - 預設 Dry-Run；-NoDryRun 才執行；-HardDelete 直接刪除，否則搬到 .trash\<ts>
#>
param(
  [string]$Root = 'C:\AI\tw-alpha-stack',
  [int]$KeepReportsDays = 45,
  [int]$KeepManifests = 5,
  [switch]$IncludePipCache,
  [switch]$HardDelete,
  [switch]$FixDuplicatePaths,
  [switch]$NoDryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $Root)) { throw "Root not found: $Root" }

$dry = -not $NoDryRun
$ts  = Get-Date -Format 'yyyyMMdd_HHmmss'
$trashRoot = Join-Path $Root ".trash\$ts"

# --- 排除根（避免誤動 .trash/_archive/.venv） ---
$trashDir   = Join-Path $Root '.trash'
$archiveDir = Join-Path $Root 'datahub\_archive'
$venvDir    = Join-Path $Root '.venv'
$excludeRoots = @($trashDir, $archiveDir, $venvDir) | Where-Object { $_ -and (Test-Path $_) }

function Test-Excluded([string]$FullName) {
  if (-not $FullName) { return $false }
  foreach ($ex in $excludeRoots) {
    if ($FullName.StartsWith($ex, [System.StringComparison]::OrdinalIgnoreCase)) { return $true }
  }
  return $false
}

function Ensure-Dir([string]$Path) {
  if (-not $dry) { New-Item -ItemType Directory -Force -Path $Path | Out-Null }
}

function Assert-ProjectRoot {
  param([string]$R)
  $sentinels = @(
    'scripts\preflight_check.py',
    'tools\Run-WFGate.ps1',
    'cal\trading_days.csv'
  )
  foreach ($s in $sentinels) {
    if (-not (Test-Path -LiteralPath (Join-Path $R $s))) {
      throw "Not a valid project root (missing: $s)"
    }
  }
  if ((Get-Location).Path.StartsWith((Join-Path $R '.trash'), [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "Refusing to run inside .trash. Please Set-Location to project root."
  }
}

function Remove-OrTrash([string]$Path) {
  if (-not (Test-Path -LiteralPath $Path)) { return }
  if (Test-Excluded $Path) { return }  # 不處理 .trash / _archive / .venv
  if ($HardDelete) {
    Remove-Item -LiteralPath $Path -Recurse -Force -WhatIf:$dry
  } else {
    $rel  = [System.IO.Path]::GetRelativePath($Root, $Path) -replace '^[.\\]+',''
    $dest = Join-Path $trashRoot $rel
    Ensure-Dir (Split-Path -Parent $dest)
    Move-Item -LiteralPath $Path -Destination $dest -Force -WhatIf:$dry
  }
}

function Move-ToArchive([string]$AlphaRoot,[string]$Path) {
  if (Test-Excluded $Path) { return }
  $archive = Join-Path $Root 'datahub\_archive'
  Ensure-Dir $archive
  $rel = [System.IO.Path]::GetRelativePath($AlphaRoot,$Path) -replace '^[.\\]+',''
  $dest = Join-Path $archive $rel
  Ensure-Dir (Split-Path -Parent $dest)
  Move-Item -LiteralPath $Path -Destination $dest -Force -WhatIf:$dry
}

Assert-ProjectRoot -R $Root
Write-Host "== AlphaCity Repo Cleanup (v1.1.1, DryRun=$dry, HardDelete=$HardDelete) =="

Push-Location $Root
try {
  # --- A) 清理零風險暫存 ---
  $alphaSilver = Join-Path $Root 'datahub\silver\alpha'
  $dirNames = @('__pycache__','.pytest_cache','.mypy_cache','.ruff_cache','.ipynb_checkpoints','.cache')

  $tmpDirs = Get-ChildItem -Path . -Directory -Recurse -Force -ErrorAction SilentlyContinue |
             Where-Object { ($dirNames -contains $_.Name) -and ($_.FullName -notlike "$alphaSilver*") -and -not (Test-Excluded $_.FullName) }
  foreach ($d in $tmpDirs) { Remove-OrTrash $d.FullName }

  $compiledFiles = Get-ChildItem -Path . -Recurse -File -Force -ErrorAction SilentlyContinue |
                   Where-Object { @('.pyc','.pyo') -contains $_.Extension -and ($_.FullName -notlike "$alphaSilver*") -and -not (Test-Excluded $_.FullName) }
  foreach ($f in $compiledFiles) { Remove-OrTrash $f.FullName }

  # --- B1) reports/ 輪轉與封存 ---
  $reports = Join-Path $Root 'reports'
  if (Test-Path $reports) {
    $keepNames = @('preflight_report.json','gate_summary.json','snapshot.txt')
    $cutoff = (Get-Date).AddDays(-$KeepReportsDays)

    Get-ChildItem $reports -Recurse -File -Filter '*.log' -Force |
      Where-Object { $_.LastWriteTime -lt $cutoff -and -not (Test-Excluded $_.FullName) } |
      ForEach-Object { Remove-OrTrash $_.FullName }

    # ★ 修正點：強制陣列，處理 0/1 檔情形
    $manifests = @(
      Get-ChildItem $reports -Recurse -File -Filter 'manifest_*.json' -Force |
      Sort-Object LastWriteTime -Descending
    )
    $oldManifests = @()
    if ($manifests.Count -gt $KeepManifests) {
      $oldManifests = $manifests | Select-Object -Skip $KeepManifests
    }
    foreach ($m in $oldManifests) { Remove-OrTrash $m.FullName }

    Get-ChildItem $reports -Recurse -File -Force |
      Where-Object {
        $_.LastWriteTime -lt $cutoff -and
        ($keepNames -notcontains $_.Name) -and
        ($_.Name -notlike 'manifest_*.json') -and
        -not (Test-Excluded $_.FullName)
      } | ForEach-Object { Remove-OrTrash $_.FullName }
  }

  # --- B2) datahub 備份/異常分區 → _archive ---
  if (Test-Path $alphaSilver) {
    $bakDirs = Get-ChildItem $alphaSilver -Directory -Recurse -Force -ErrorAction SilentlyContinue |
               Where-Object { $_.Name -match '(\.bak_|_bak_|_old$|^bak_)' -and -not (Test-Excluded $_.FullName) }
    foreach ($b in $bakDirs) { Move-ToArchive $alphaSilver $b.FullName }
  }

  # --- 偵測：重覆路徑 ...\silver\alpha\silver\alpha\ ---
  $datahubRoot = Join-Path $Root 'datahub'
  if (Test-Path $datahubRoot) {
    $dup = Get-ChildItem $datahubRoot -Directory -Recurse -Force -ErrorAction SilentlyContinue |
           Where-Object { $_.FullName -match '\\silver\\alpha\\silver\\alpha\\' } |
           Select-Object -ExpandProperty FullName -Unique
    if ($dup) {
      Write-Warning "Detected duplicated path(s):"
      $dup | ForEach-Object { Write-Warning "  $_" }
      if ($FixDuplicatePaths -and (Test-Path $alphaSilver)) {
        foreach ($dr in $dup) {
          $children = Get-ChildItem $dr -Directory -Force
          foreach ($c in $children) {
            $dest = Join-Path $alphaSilver $c.Name
            Ensure-Dir $dest
            Get-ChildItem $c.FullName -Force | ForEach-Object {
              if (-not (Test-Excluded $_.FullName)) {
                Move-Item -LiteralPath $_.FullName -Destination $dest -Force -WhatIf:$dry
              }
            }
            Remove-OrTrash $c.FullName
          }
        }
      } else {
        Write-Host "Tip: 若要修復，加入 -FixDuplicatePaths（Dry-Run 先預演）。"
      }
    }
  }

  # --- (可選) pip cache ---
  if ($IncludePipCache) {
    $pipCache = Join-Path $env:LOCALAPPDATA 'pip\Cache'
    if (Test-Path $pipCache) { Remove-OrTrash $pipCache }
  }

  if (-not $HardDelete -and -not $dry) { Write-Host "Moved items to $trashRoot" }
  Write-Host "Cleanup finished (DryRun=$dry)."
}
finally {
  Pop-Location
}
