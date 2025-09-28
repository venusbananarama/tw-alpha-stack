Param(
  [string]$Factors = "G:\AI\datahub\alpha\alpha_factors_fixed.parquet",
  [string]$OutDir = "G:\AI\datahub\alpha\backtests\topN_50_M",
  [string]$Config = "configs\backtest_topN_fixed.yaml",
  [string]$FactorCol = ""
)

$here = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $here

# Activate venv if exists
$venv = ".\.venv\Scripts\Activate.ps1"
if (Test-Path $venv) { . $venv }

# Ensure longonly_topN.py is upgraded
python replace_longonly_topN_v2.py backtest\longonly_topN.py 2>$null

# Run core weekly fix (safe to re-run)
python patch_backtest_core_v4.py backtest\core.py 2>$null

# Run backtest
$factorArg = ""
if ($FactorCol -ne "") { $factorArg = "--factor `"$FactorCol`"" }

python backtest\longonly_topN.py --factors "$Factors" --out-dir "$OutDir" --config "$Config" $factorArg

# Summarize
python summarize_performance.py --out-dir "$OutDir"
