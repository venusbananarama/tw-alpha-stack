param(
  [Parameter(Mandatory = $true)][string]$Factors,
  [Parameter(Mandatory = $true)][string]$OutDir,
  [Parameter(Mandatory = $false)][string]$Start = "",
  [Parameter(Mandatory = $false)][string]$End = "",
  [Alias("Config")][string]$ConfigPath = "",
  [Parameter(Mandatory = $true)][string]$FactorsPath,
  [string]$Python = "py"
)

Write-Host "== Weekly Factor Health Check =="
Write-Host "Factors =" $Factors
Write-Host "OutDir  =" $OutDir
Write-Host "Start   =" $Start
Write-Host "End     =" $End
Write-Host "Config  =" $ConfigPath
Write-Host "FactorsPath =" $FactorsPath

# Ensure OutDir exists
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# Resolve python exe (prefer local venv)
$pyexe = & $Python -c "import sys, pathlib; print((pathlib.Path('.venv','Scripts','python.exe') if sys.platform=='win32' else pathlib.Path('.venv','bin','python')).as_posix())"
if (Test-Path ".\.venv") {
  if (Test-Path $pyexe) {
    Write-Host "Using venv python:" $pyexe
  } else {
    Write-Host "WARNING: .venv present but python not found, fallback to system python."
    $pyexe = $Python
  }
} else {
  $pyexe = $Python
}

$args = @("--factors", $Factors, "--out", $OutDir, "--factors-path", $FactorsPath)
if ($Start -ne "") { $args += @("--start", $Start) }
if ($End -ne "") { $args += @("--end", $End) }
if ($ConfigPath -ne "") { $args += @("--config", $ConfigPath) }

& $pyexe "scripts\weekly_factors_check.py" @args

if ($LASTEXITCODE -ne 0) {
  throw "weekly_factors_check.py failed with exit code $LASTEXITCODE"
} else {
  Write-Host "Weekly check completed. See $OutDir"
}
