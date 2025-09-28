<#  repo_tidy_v2.ps1 — AlphaCity 專案清理/歸位器（安全版，v2）

用法：
  # 規劃（Dry-run；預設）
  pwsh -File .\tools\repo_tidy_v2.ps1

  # 真正執行（會搬/刪；建議加 -Backup）
  pwsh -File .\tools\repo_tidy_v2.ps1 -Apply -Backup

說明：
- 權威路徑（Canonical）：.\scripts .\tools .\tasks .\schemas .\cal
- 保護資料夾：AlphaCity_v63_live_addon_v2, pkgB_profile_v6, pkgG_smoke_v6_2, pkgF_codex_patch_v6_2, .git, .venv, datahub, logs, metrics, backup, _audit
- 只刪「pkg_*」底下、且與 Canonical **相同雜湊** 的副本
- 有「已知映射」的檔案會自動歸位到 Canonical；不同雜湊 → 先備份再覆蓋

#>

param(
  [switch]$Plan,     # 僅規劃（預設）
  [switch]$Apply,    # 真的執行搬/刪
  [switch]$Backup    # 刪或覆蓋之前改為先備份到 .\backup\
)

$ErrorActionPreference = 'Stop'
$root = (Get-Location).Path
New-Item -ItemType Directory -Force ./_audit | Out-Null

# ---- 規則 ----
$canonicalDirs = @('scripts','tools','tasks','schemas','cal')
$protectTop    = @(
  'AlphaCity_v63_live_addon_v2','pkgB_profile_v6','pkgG_smoke_v6_2','pkgF_codex_patch_v6_2',
  '.git','.venv','datahub','logs','metrics','backup','_audit'
)

# 已知映射
$moveMap = @(
  @{ From='pkg_v63_strict\scripts\ps\Invoke-AlphaVerification.ps1'; To='scripts\ps\Invoke-AlphaVerification.ps1'; Mode='move' },
  @{ From='pkg_v63_strict\scripts\emit_metrics_v63.py';           To='scripts\emit_metrics_v63.py';           Mode='move' },
  @{ From='pkg_v63_strict\tools\Verify-ParquetIntegrity.ps1';     To='tools\Verify-ParquetIntegrity.ps1';     Mode='move' },
  @{ From='pkg_v63_strict\tasks\';                                To='tasks\';                                Mode='move_dir_v63' },
  @{ From='pkg_v63_strict\schemas\';                              To='schemas\';                              Mode='move_dir_all' },
  @{ From='pkg_v63_strict\cal\trading_days.csv';                  To='cal\trading_days.csv';                  Mode='copy_if_missing' },
  @{ From='pkgG_smoke_v6_2\Run-SmokeTests.ps1';                   To='tools\Run-SmokeTests.ps1';              Mode='move_if_new' },
  @{ From='pkgC_calendar_v6\trading_days.sample';                 To='cal\trading_days.csv';                  Mode='copy_if_missing' }
)

function Get-Rel([string]$p){ [IO.Path]::GetRelativePath((Get-Location).Path, (Resolve-Path $p)).Replace('/','\') }
function Ensure-Dir([string]$p){ $d = Split-Path -Parent $p; if ($d -and -not (Test-Path $d)) { New-Item -ItemType Directory -Force $d | Out-Null } }
function SHA256([string]$p){ (Get-FileHash -Algorithm SHA256 -LiteralPath $p).Hash }
function Is-InCanonical([string]$rel){
  foreach($c in $canonicalDirs){ if ($rel -imatch "^[\\/]?$c([\\/]|$)") { return $true } }; return $false
}
function Is-ProtectedTop([string]$rel){
  $head = $rel -replace '^[\\/]?([^\\/]+).*','$1'
  return $protectTop -contains $head
}

# ---- 盤點 ----
$excludeHead = '^(' + (($protectTop + @('backup','_audit')).ForEach({[regex]::Escape($_)}) -join '|') + ')(\\|/|$)'
$all = Get-ChildItem -Recurse -File | Where-Object {
  $rel = Get-Rel $_.FullName
  -not ($rel -match $excludeHead)
} | ForEach-Object {
  [pscustomobject]@{
    rel   = Get-Rel $_.FullName
    name  = $_.Name
    dir   = (Split-Path -Parent (Get-Rel $_.FullName))
    size  = $_.Length
    mtime = $_.LastWriteTimeUtc.ToString('o')
    hash  = SHA256 $_.FullName
  }
}
$all | Export-Csv -UseCulture -NoTypeInformation ./_audit/inventory.csv

# ---- 重複/衝突 ----
$byName = $all | Group-Object name
$dupeSame = @()
$dupeDiff = @()

foreach($g in $byName){
  $byHash = $g.Group | Group-Object hash
  foreach($h in $byHash){
    if ($h.Count -gt 1) { $dupeSame += $h.Group }
  }
  if ($byHash.Count -gt 1) { $dupeDiff += $g.Group | Select-Object -Unique }
}

$dupeSame | Select rel,name,dir,hash,size,mtime | Export-Csv -UseCulture -NoTypeInformation ./_audit/dupe_same_hash.csv
$dupeDiff | Select rel,name,dir,hash,size,mtime | Export-Csv -UseCulture -NoTypeInformation ./_audit/dupe_diff_hash.csv

# ---- 規劃：pkg_* → Canonical ----
$plans = New-Object System.Collections.ArrayList
function Add-Plan($Action,$From,$To,$Reason){ [void]$plans.Add([pscustomobject]@{ action=$Action; from=$From; to=$To; reason=$Reason }) }

foreach($m in $moveMap){
  $from = Join-Path $root $m.From
  if ($m.Mode -eq 'move_dir_v63' -or $m.Mode -eq 'move_dir_all'){
    if (Test-Path $from){
      $files = Get-ChildItem -Recurse -File $from
      foreach($f in $files){
        if ($m.Mode -eq 'move_dir_v63' -and ($f.Name -notmatch 'v63\.ps1$')) { continue }
        $relFrom = Get-Rel $f.FullName
        $tail    = [IO.Path]::GetRelativePath($from, $f.FullName)
        $relTo   = Join-Path $m.To $tail
        $absTo   = Join-Path $root $relTo
        if (Test-Path $absTo){
          if (SHA256 $f.FullName -eq SHA256 $absTo){
            Add-Plan 'delete' $relFrom $relTo 'duplicate(same hash)'
          } else {
            Add-Plan 'backup_then_overwrite' $relFrom $relTo 'conflict(different hash)'
          }
        } else {
          Add-Plan 'move' $relFrom $relTo 'canonicalize'
        }
      }
    }
    continue
  }
  if (Test-Path $from){
    $relFrom = Get-Rel $from
    $to      = Join-Path $root $m.To
    $relTo   = $m.To
    switch($m.Mode){
      'move' {
        if (Test-Path $to){
          if (SHA256 $from -eq SHA256 $to){ Add-Plan 'delete' $relFrom $relTo 'duplicate(same hash)' }
          else { Add-Plan 'backup_then_overwrite' $relFrom $relTo 'conflict(different hash)' }
        } else { Add-Plan 'move' $relFrom $relTo 'canonicalize' }
      }
      'move_if_new' {
        if (-not (Test-Path $to)){ Add-Plan 'move' $relFrom $relTo 'canonicalize' }
        else {
          if (SHA256 $from -eq SHA256 $to){ Add-Plan 'delete' $relFrom $relTo 'duplicate(same hash)' }
          else { Add-Plan 'backup_then_overwrite' $relFrom $relTo 'conflict(different hash)' }
        }
      }
      'copy_if_missing' {
        if (-not (Test-Path $to)){ Add-Plan 'copy' $relFrom $relTo 'seed file (missing)' }
        else { Add-Plan 'noop' $relFrom $relTo 'exists' }
      }
    }
  }
}

# ---- 刪除候選：pkg_* 中與 Canonical 相同雜湊的副本 ----
$canonHashes = $all | Where-Object { Is-InCanonical $_.rel } | Group-Object hash | Select-Object -ExpandProperty Name
$pkgDupes = $all | Where-Object {
  ($_.relpath -match '^[\\/]?pkg[^\\/]+')
  ($canonHashes -contains $_.hash) -and
  -not (Is-ProtectedTop $_.rel)
}
foreach($d in $pkgDupes){ Add-Plan 'delete' $d.rel '' 'duplicate of canonical (by hash)' }

$plans | Export-Csv -UseCulture -NoTypeInformation ./_audit/cleanup_plan.csv

Write-Host "== 規劃輸出 =="
Write-Host "  _audit/inventory.csv"
Write-Host "  _audit/dupe_same_hash.csv"
Write-Host "  _audit/dupe_diff_hash.csv"
Write-Host "  _audit/cleanup_plan.csv"
if (-not $Apply) { Write-Host "`n(預設為 Dry-run；加 -Apply 才會執行)" ; return }

# ---- 執行 ----
$ts = (Get-Date).ToString('yyyyMMdd_HHmmss')
$backupRoot = Join-Path $root ("backup\repo_tidy_" + $ts)
if ($Backup) { New-Item -ItemType Directory -Force $backupRoot | Out-Null }

foreach($p in $plans){
  $absFrom = Join-Path $root $p.from
  $absTo   = if ($p.to) { Join-Path $root $p.to } else { $null }

  switch($p.action){
    'noop' { continue }
    'move' {
      if (Test-Path $absFrom){ Ensure-Dir $absTo; Move-Item -Force -LiteralPath $absFrom -Destination $absTo }
    }
    'copy' {
      if (Test-Path $absFrom){ Ensure-Dir $absTo; Copy-Item -Force -LiteralPath $absFrom -Destination $absTo }
    }
    'backup_then_overwrite' {
      if (Test-Path $absFrom){
        Ensure-Dir $absTo
        if ($Backup){
          $bk = Join-Path $backupRoot ("conflict\" + ($p.from -replace '[\\/]', '_'))
          Ensure-Dir $bk
          Copy-Item -Force -LiteralPath $absFrom -Destination $bk
        }
        Copy-Item -Force -LiteralPath $absFrom -Destination $absTo
      }
    }
    'delete' {
      if (Test-Path $absFrom){
        if ($Backup){
          $bk = Join-Path $backupRoot ("dupe\" + ($p.from -replace '[\\/]', '_'))
          Ensure-Dir $bk
          Copy-Item -Force -LiteralPath $absFrom -Destination $bk
        }
        Remove-Item -Force -LiteralPath $absFrom
      }
    }
  }
}

# 移除空的 pkg_* 目錄（保護名單除外）
Get-ChildItem -Directory -Recurse | Where-Object {
  $_.Name -like 'pkg*' -and -not ($protectTop -contains $_.Name)
} | ForEach-Object {
  if (-not (Get-ChildItem -Recurse -LiteralPath $_.FullName -ErrorAction SilentlyContinue)){
    Remove-Item -Force -Recurse -LiteralPath $_.FullName
  }
}

Write-Host "`n完成：執行明細已輸出到 _audit/cleanup_plan.csv"
if ($Backup) { Write-Host "已備份到：$backupRoot" }
