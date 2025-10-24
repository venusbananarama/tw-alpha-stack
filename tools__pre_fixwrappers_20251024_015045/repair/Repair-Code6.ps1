<#  Repair-Code6.ps1
    目的：一次性修復 / 防呆，對齊手冊目錄與環境（可重複執行、可安全跳過）
    對齊口徑：
      - 設定 ALPHACITY_ALLOW=1、清除 PYTHONSTARTUP（避免 sitecustomize 阻擋）
      - 檢查/建立 reports、metrics、datahub/silver/alpha/{prices,chip,dividend,per}
      - 驗證 .venv Python 與 pandas/pyarrow 版本可用
      - （可選）隔離 datahub\_archive → _archive_off_<timestamp>
      - （可選）偵測並修復 "...\silver\alpha\silver\alpha\..." 重複層級
#>

[CmdletBinding(SupportsShouldProcess = $true)]
param(
  [string]$Root = ".",
  [string]$DataHubRoot = "datahub",
  [switch]$FixDupPaths,
  [switch]$IsolateArchive,
  [switch]$Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Write-Info([string]$msg) {
  if (-not $Quiet) { Write-Host $msg }
}

# 1) 解析路徑與環境解鎖
$ROOT   = (Resolve-Path $Root).Path
$DATAHB = Join-Path $ROOT $DataHubRoot
$REPTS  = Join-Path $ROOT "reports"

$env:ALPHACITY_ALLOW = '1'
Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue

# 2) 找 Python
$PY = Join-Path $ROOT ".venv\Scripts\python.exe"
if (-not (Test-Path $PY)) {
  Write-Warning "未發現 $PY，請先依手冊建立 venv 與安裝套件。"
} else {
  & $PY -V  | Write-Info
  try {
    & $PY -c "import pandas,pyarrow,platform;print('OK pandas/pyarrow:',pandas.__version__,pyarrow.__version__,'python',platform.python_version())" | Write-Info
  } catch {
    Write-Warning "pandas/pyarrow 匯入失敗，請依手冊 §10 重裝依賴。"
  }
}

# 3) 目錄存在性
$dirs = @(
  $REPTS,
  (Join-Path $ROOT "metrics"),
  (Join-Path $DATAHB "silver\alpha\prices"),
  (Join-Path $DATAHB "silver\alpha\chip"),
  (Join-Path $DATAHB "silver\alpha\dividend"),
  (Join-Path $DATAHB "silver\alpha\per")
)
foreach($d in $dirs) { New-Item -ItemType Directory -Force -Path $d | Out-Null }

# 4) 偵測重複 "...\silver\alpha\silver\alpha\..."
$dup = Get-ChildItem -Recurse -Force -ErrorAction SilentlyContinue $DATAHB |
  Where-Object { $_.PSIsContainer -and $_.FullName -match '\\silver\\alpha\\silver\\alpha\\' }
if ($dup) {
  Write-Warning "偵測到重複層級 'silver\alpha\silver\alpha'："
  $dup | ForEach-Object { Write-Warning (" - " + $_.FullName) }
  if ($FixDupPaths) {
    foreach($dir in $dup) {
      $target = ($dir.FullName -replace '\\silver\\alpha\\silver\\alpha\\','\silver\alpha\')
      Write-Info "移動 '$($dir.FullName)' → '$target'"
      New-Item -ItemType Directory -Force -Path (Split-Path -Parent $target) | Out-Null
      Move-Item -Force -LiteralPath $dir.FullName -Destination $target
    }
  } else {
    Write-Info "（提示）若要自動修正，重跑本腳本並加上 -FixDupPaths"
  }
}

# 5) （可選）隔離 _archive
if ($IsolateArchive -and (Test-Path (Join-Path $DATAHB "_archive"))) {
  $ts = Get-Date -Format yyyyMMdd_HHmmss
  $newName = "_archive_off_$ts"
  Rename-Item -LiteralPath (Join-Path $DATAHB "_archive") -NewName $newName
  Write-Info "已將 datahub\_archive → $newName"
}

Write-Info "Repair-Code6 完成。"