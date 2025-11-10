
# Run-Phase2-OneClick.ps1
# Direct orchestration (no wrappers): eval -> corr -> combo -> Gate
param(
  [string]$Root = "C:\AI\tw-alpha-stack",
  [string]$Date = (Get-Date).ToString('yyyy-MM-dd'),
  [string[]]$Families = @('tech','chip'),
  [int[]]$Windows = @(6,12,24),
  [int]$TopN = 20,
  [double]$MaxWeightPct = 0.05,
  [int]$GuardAdv = 5
)
$ErrorActionPreference='Stop'
Set-Location $Root
$env:ALPHACITY_ALLOW='1'

$python   = ".\.venv\Scripts\python.exe"
$eval_py  = ".\scripts\factor_eval.py"
$corr_py  = ".\scripts\factor_corr.py"
$combo_py = ".\scripts\factor_combo.py"

$familiesArg = $Families -join ' '
$windowsArg  = $Windows -join ' '

# 1) Eval
& $python $eval_py  --date $Date --align W-FRI --lag-rule strict --families $familiesArg --neutralize "size industry" --wf-windows $windowsArg --output .\reports

# 2) Corr
& $python $corr_py  --date $Date --families $familiesArg --output .\reports

# 3) Combo + TopN + capacity
& $python $combo_py --date $Date --families $familiesArg --neutralize "size industry" --topn $TopN --max-weight-pct ("{0:N2}" -f $MaxWeightPct) --guard-adv $GuardAdv --align W-FRI --lag-rule strict --wf-windows $windowsArg --output .\reports

# 4) Gate (single entry)
pwsh -NoProfile -File .\tools\gate\Run-WFGate.ps1 -WFDir .\tools\gate\wf_configs
