param(
  [string]$Date,
  [ValidateSet('W-FRI')] [string]$Align='W-FRI',
  [ValidateSet('strict')] [string]$LagRule='strict',
  [int[]]$Windows=@(6,12,24),
  [string[]]$Families=@('tech','chip'),
  [string[]]$Neutralize=@('size','industry'),
  [int]$TopN=20,
  [double]$MaxWeightPct=0.05,
  [int]$GuardAdv=5,
  [string]$Output = ".\reports",
  [switch]$Quiet
)
$ErrorActionPreference='Stop'

$python   = ".\.venv\Scripts\python.exe"
$py_corr  = ".\scripts\factor_corr.py"
if(!(Test-Path $python)) { throw "Python venv not found: $python" }
if(!(Test-Path $py_corr)) { throw "Missing script: $py_corr" }

$familiesArg = $Families -join ' '
$cmd = @(
  $py_corr,
  "--date", $Date,
  "--families", $familiesArg,
  "--output", $Output
)
& $python @cmd
