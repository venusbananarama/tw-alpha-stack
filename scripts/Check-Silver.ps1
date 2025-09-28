param(
  [Parameter(Mandatory=$true)][string]$DatahubRoot,
  [string]$SchemaPath,
  [string[]]$Datasets,
  [string]$ReportCsv,
  [switch]$Strict,
  [string]$PythonPath # 選填：指定 python 路徑；預設自動偵測 venv
)

$ErrorActionPreference = 'Stop'

# 嘗試尋找 venv python
if (-not $PythonPath) {
  $repoRoot = Split-Path -Parent $PSCommandPath
  $venvPy = Join-Path $repoRoot "..\.venv\Scripts\python.exe"
  if (Test-Path $venvPy) { $PythonPath = $venvPy } else { $PythonPath = "python" }
}

$scriptPath = Join-Path (Split-Path -Parent $PSCommandPath) "validate_silver.py"

# 組合參數
$argsList = @($scriptPath, "--datahub-root", $DatahubRoot)

if ($SchemaPath) { $argsList += @("--schema-path", $SchemaPath) }
if ($Datasets)   { $argsList += @("--datasets"); $argsList += $Datasets }
if ($ReportCsv)  { $argsList += @("--report-csv", $ReportCsv) }
if ($Strict)     { $argsList += @("--strict") }

Write-Host "CMD> $PythonPath $($argsList -join ' ')" -ForegroundColor Cyan
& $PythonPath @argsList
exit $LASTEXITCODE
