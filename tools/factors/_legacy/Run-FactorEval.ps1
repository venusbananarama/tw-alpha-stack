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

# Directly call python eval (no wrappers).
$python   = ".\.venv\Scripts\python.exe"
$py_eval  = ".\scripts\factor_eval.py"
if(!(Test-Path $python)) { throw "Python venv not found: $python" }
if(!(Test-Path $py_eval)) { throw "Missing script: $py_eval" }

$familiesArg   = $Families -join ' '
$neutralArg    = $Neutralize -join ' '
$windowsArg    = $Windows -join ' '

$cmd = @(
  $py_eval,
  "--date", $Date,
  "--align", $Align,
  "--lag-rule", $LagRule,
  "--families", $familiesArg,
  "--neutralize", $neutralArg,
  "--wf-windows", $windowsArg,
  "--output", $Output
)
& $python @cmd
