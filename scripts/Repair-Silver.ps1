param(
  [Parameter(Mandatory=$true)][string]$DatahubRoot,
  [string]$SchemaPath,
  [string[]]$Datasets,
  [switch]$Backup,
  [switch]$Strict,
  [string]$PythonPath
)
$ErrorActionPreference = 'Stop'

# 找 venv python
if (-not $PythonPath) {
  $repoRoot = Split-Path -Parent $PSCommandPath
  $venvPy = Join-Path $repoRoot "..\.venv\Scripts\python.exe"
  if (Test-Path $venvPy) { $PythonPath = $venvPy } else { $PythonPath = "python" }
}

$scriptPath = Join-Path (Split-Path -Parent $PSCommandPath) "repair_silver_types.py"

$argsList = @($scriptPath, "--datahub-root", $DatahubRoot)
if ($SchemaPath) { $argsList += @("--schema-path", $SchemaPath) }
if ($Datasets)   { $argsList += @("--datasets"); $argsList += $Datasets }
if ($Backup)     { $argsList += @("--backup") }
if ($Strict)     { $argsList += @("--strict") }

Write-Host "CMD> $PythonPath $($argsList -join ' ')" -ForegroundColor Cyan
& $PythonPath @argsList
exit $LASTEXITCODE
