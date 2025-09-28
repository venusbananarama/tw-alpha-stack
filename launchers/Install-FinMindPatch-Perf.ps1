
$ErrorActionPreference = "Stop"

# 保證在專案根目錄
if (-not (Test-Path ".\scripts")) {
  Write-Host "請切到含有 scripts/configs 的專案根目錄 (例如 G:\AI\tw-alpha-stack)"
  exit 1
}

# 修改 Invoke-FMBackfill.ps1 增加 Workers / Qps 參數
@'
param(
  [Parameter(Mandatory=$true)][string]$Start,
  [Parameter(Mandatory=$true)][string]$End,
  [Parameter(Mandatory=$true)][string[]]$Datasets,
  [string]$Universe = "configs\\universe.tw_all.txt",
  [string]$Extra = "",
  [int]$Workers = 6,
  [double]$Qps = 1.6
)
$ErrorActionPreference = "Stop"
Write-Host "== FMBackfill =="
$dsList = @()
foreach ($d in $Datasets) { $dsList += ($d -split ",") | ForEach-Object { $_.Trim() } | Where-Object { $_ } }
if ($dsList.Count -eq 0) { throw "No dataset groups provided." }
Write-Host ("Start={0} End={1} Datasets={2} Workers={3} Qps={4}" -f $Start,$End,($dsList -join ","),$Workers,$Qps)
if (-not $env:FINMIND_TOKEN) { throw 'FINMIND_TOKEN is empty. 請先：$env:FINMIND_TOKEN = "your-token-here"' }
$pyArgs = @("scripts/finmind_backfill.py","--start",$Start,"--end",$End,"--datasets") + $dsList + @("--datasets-yaml","configs/datasets.yaml","--universe",$Universe,"--workers",$Workers,"--qps",$Qps)
python @pyArgs
'@ | Out-File -Encoding utf8 -NoNewline scripts/ps/Invoke-FMBackfill.ps1

# 修改 Run-FMDaily.ps1 增加 Workers / Qps 參數
@'
param(
  [int]$LastNDays = 5,
  [string[]]$Groups = @("prices","chip","derivatives","macro_others"),
  [string]$DatasetsYaml = "configs/datasets.yaml",
  [int]$Workers = 6,
  [double]$Qps = 1.6
)
$ErrorActionPreference = "Stop"
Write-Host "=== [AlphaCity] Daily EOD Update ==="
if (-not $env:FINMIND_TOKEN) { throw 'FINMIND_TOKEN is empty. 請先：$env:FINMIND_TOKEN = "your-token-here"' }
Write-Host "== EOD Flow =="
python scripts/finmind_daily_update.py --last-n-days $LastNDays --datasets-yaml $DatasetsYaml --groups ($Groups -join ",") --workers $Workers --qps $Qps
'@ | Out-File -Encoding utf8 -NoNewline scripts/ps/Run-FMDaily.ps1

Write-Host "[DONE] Perf patch applied. Invoke-FMBackfill.ps1 / Run-FMDaily.ps1 now support -Workers and -Qps."
