#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [Parameter(Mandatory)][string]$Date,      # yyyy-MM-dd
  [Parameter(Mandatory)][string]$IDs,       # '2330 6669' 或 '2330,6669'
  [string]$Datasets                         # 例：'TaiwanStockKBar,TaiwanStockDelisting'（逗號或空白分隔）
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根（tools\dateid 的上兩層）
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
Set-Location $RepoRoot

# Python 路徑（.venv 為主，找不到改用 python）
$PY = '.\.venv\Scripts\python.exe'
if(-not (Test-Path -LiteralPath $PY)){ $PY = 'python' }

# 取數核心
$Fetcher = '.\scripts\fm_dateid_fetch.py'

# --end 為「不含終點」，請 +1 天
$End    = (Get-Date $Date).AddDays(1).ToString('yyyy-MM-dd')

# ID 清單（接受逗號/空白，僅保留 4 碼）
$IdsArr = ($IDs -split '[,\s]+' | Where-Object { $_ -match '^\d{4}$' })

if(-not $IdsArr -or $IdsArr.Count -eq 0){
  throw "IDs 解析不到任何 4 碼代號；請確認 -IDs 參數（例：'2330 6669' 或 '2330,6669'）。"
}

# Dataset 清單：沒給就維持舊行為（只抓 Price / Chip）
if([string]::IsNullOrWhiteSpace($Datasets)){
  $dsList = @('TaiwanStockPrice','TaiwanStockInstitutionalInvestorsBuySell')
}else{
  $dsList = $Datasets -split '[,\s]+' | Where-Object { $_ -ne '' } | Sort-Object -Unique
}

foreach($ds in $dsList){
  $argv = @('--datasets',$ds,'--ids') + $IdsArr + @('--date',$Date,'--end',$End,'--out-root','datahub')
  Write-Host ">> fm_dateid_fetch $ds  ids=$($IdsArr -join ',')  date=$Date  end(excl)=$End  out=datahub"
  & $PY $Fetcher @argv
  if($LASTEXITCODE -ne 0){
    throw "fetch failed: $ds"
  }
}
Write-Host "[OK] DateID Extras done. Datasets=$(($dsList -join ','))  IDs=$($IdsArr.Count)  Date=$Date  End(excl)=$End" -ForegroundColor Green