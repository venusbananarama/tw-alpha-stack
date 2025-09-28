param(
  [string]$ProjectDir = (Get-Location).Path,
  [string]$Python = "py",
  [string]$FataiRoot = "",
  [string]$Date = ""          # optional YYYYMMDD, empty = today
)

Write-Host "== TW Alpha One-Click (TWSE 官方版，全市場) =="

# 0) cd to project root
Set-Location -Path $ProjectDir

# 1) venv
if (-Not (Test-Path "$ProjectDir\.venv")) {
  Write-Host "Creating venv with Python 3.10..."
  & $Python -3.10 -m venv "$ProjectDir\.venv"
}
$pyexe = Join-Path "$ProjectDir\.venv" "Scripts\python.exe"

# 2) deps
& $pyexe -m pip install --upgrade pip setuptools wheel
& $pyexe -m pip install -r "$ProjectDir\requirements.txt"

# 3) fetch from TWSE official CSV
if ([string]::IsNullOrWhiteSpace($Date)) {
  & $pyexe "$ProjectDir\scripts\fetch_all.py" --mode twse
} else {
  & $pyexe "$ProjectDir\scripts\fetch_all.py" --mode twse --date $Date
}

# 4) report for whole market
if ($FataiRoot -ne "") {
  & $pyexe "$ProjectDir\scripts\run_daily.py" --out "$ProjectDir\reports" --fatai $FataiRoot
} else {
  & $pyexe "$ProjectDir\scripts\run_daily.py" --out "$ProjectDir\reports"
}

Write-Host "All done. See reports/ for outputs."
