param(
  [int]$LastNDays = 5,
  [string[]]$Groups = @("prices","chip","derivatives","macro_others"),
  [string]$DatasetsYaml = "configs/datasets.yaml",
  [int]$Workers = 6,
  [double]$Qps = 1.6
)
$ErrorActionPreference = "Stop"
Write-Host "=== [AlphaCity] Daily EOD Update ==="
if (-not $env:FINMIND_TOKEN) { throw 'FINMIND_TOKEN is empty. 隢?嚗?env:FINMIND_TOKEN = \"your-token\"' }
Write-Host "== EOD Flow =="
python scripts/finmind_daily_update.py --last-n-days $LastNDays --datasets-yaml $DatasetsYaml --groups ($Groups -join ",") --workers $Workers --qps $Qps
