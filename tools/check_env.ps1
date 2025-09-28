param([switch]$FixPolicy)

$ErrorActionPreference = "Stop"
Write-Host "=== FATAI Environment Check ==="
Write-Host "Working dir:" (Get-Location).Path

# Execution policy
$pol = Get-ExecutionPolicy -Scope CurrentUser
Write-Host "ExecutionPolicy (CurrentUser):" $pol
if ($FixPolicy -and $pol -ne "RemoteSigned") {
    Write-Host "Setting ExecutionPolicy to RemoteSigned for CurrentUser..."
    Set-ExecutionPolicy RemoteSigned -Scope CurrentUser -Force
}

# Python resolution
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$repo = Split-Path -Parent $root
$venvPy = Join-Path $repo ".venv\Scripts\python.exe"
if (Test-Path $venvPy) {
    $python = $venvPy
    Write-Host "Using venv python:" $python
} else {
    $python = "python"
    Write-Host "Using system python on PATH"
}

# Python version
try { & $python --version } catch { Write-Warning "Python not found in PATH or .venv" }

# Package checks (safe fallback)
$code = @"
mods = ['pandas','pyarrow','matplotlib']
missing = []
for m in mods:
    try:
        __import__(m)
    except ImportError:
        missing.append(m)
print('Missing:', ','.join(missing) if missing else 'None')
"@
$temp = [System.IO.Path]::GetTempFileName() + ".py"
Set-Content -Path $temp -Value $code -Encoding UTF8
& $python $temp
Remove-Item $temp -Force
