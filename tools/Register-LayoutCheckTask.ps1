$ErrorActionPreference = 'Stop'

# 路徑與日誌
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$logDir   = Join-Path $RepoRoot "logs"
$logFile  = Join-Path $logDir  "layout_check.log"
$check    = Join-Path $RepoRoot "tools\Check-CanonicalLayout.ps1"

New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$ts = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
Add-Content -Path $logFile -Value "$ts [START] Running Check-CanonicalLayout.ps1 -Strict"

# 用 Windows PowerShell 或 pwsh 以 Bypass 執行政策執行檢查腳本
# （Register-LayoutCheckTask.ps1 已決定要用哪個執行器）
$executor = $PSVersionTable.PSEdition -eq 'Core' ? 'pwsh.exe' : 'powershell.exe'
& $executor -NoProfile -ExecutionPolicy Bypass -File "$check" -Strict *>> "$logFile"

$code = if ($LASTEXITCODE -ne $null) { $LASTEXITCODE } else { 0 }
$ts2 = (Get-Date).ToString('yyyy-MM-dd HH:mm:ss')
Add-Content -Path $logFile -Value "$ts2 [END] code=$code"
exit $code
