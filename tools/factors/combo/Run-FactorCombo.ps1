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

$python    = ".\.venv\Scripts\python.exe"
$py_combo  = ".\scripts\factor_combo.py"
if(!(Test-Path $python)) { throw "Python venv not found: $python" }
if(!(Test-Path $py_combo)) { throw "Missing script: $py_combo" }

$familiesArg = $Families -join ' '
$neutralArg  = $Neutralize -join ' '
$windowsArg  = $Windows -join ' '

$cmd = @(
  $py_combo,
  "--date", $Date,
  "--families", $familiesArg,
  "--neutralize", $neutralArg,
  "--topn", $TopN,
  "--max-weight-pct", ("{0:N2}" -f $MaxWeightPct),
  "--guard-adv", $GuardAdv,
  "--align", $Align,
  "--lag-rule", $LagRule,
  "--wf-windows", $windowsArg,
  "--output", $Output
)
& $python @cmd
