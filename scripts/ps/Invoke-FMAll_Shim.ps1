<#
.SYNOPSIS
  Invoke-FMAll_Shim.ps1：市場級資料集（prices/chip/stock_info）直接呼叫 finmind_backfill.py。
#>
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)]
  [ValidateSet('prices','chip','stock_info')]
  [string]$Group,

  [string]$Start = '2015-01-01',
  [string]$End   = ((Get-Date).ToString('yyyy-MM-dd')),

  [double]$Qps = 0.04,

  [string]$ApiToken,

  [string]$DatahubRoot = 'G:\AI\tw-alpha-stack\datahub'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$py = 'G:\AI\tw-alpha-stack\.venv\Scripts\python.exe'
$script = 'G:\AI\tw-alpha-stack\scripts\finmind_backfill.py'
if (-not (Test-Path $py))     { throw "找不到 Python：$py" }
if (-not (Test-Path $script)) { throw "找不到 backfill 腳本：$script" }
if ($ApiToken) { $env:FINMIND_TOKEN = $ApiToken }

$Map = @{
  'prices'     = @('TaiwanStockPrice');
  'chip'       = @('TaiwanStockTotalInstitutionalInvestors','TaiwanStockTotalMarginPurchaseShortSale');
  'stock_info' = @('TaiwanStockMarketTradingInfo','TaiwanStockTAIEX','TaiwanStockOTCIndex');
}
$ds = $Map[$Group]
if (-not $ds -or $ds.Count -eq 0) { throw "Group=$Group 沒有對應 datasets" }

Write-Host "== FMAll Shim =="
Write-Host ("Group={0}  Datasets={1}" -f $Group, ($ds -join ','))
Write-Host ("Start={0} End={1} Qps={2} DatahubRoot={3}" -f $Start,$End,$Qps,$DatahubRoot)

$ds_arg = ($ds -join ',')
& $py $script --start $Start --end $End --universe TSE --workers 1 --qps $Qps --datahub-root $DatahubRoot --datasets $ds_arg
