#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [string]$Rules        = '.\rules.yaml',
  [string]$UniverseFile = '.\configs\investable_universe.txt',
  [string]$DataRoot     = 'datahub',
  [string]$Start,
  [string]$End
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# --- repo root 與相對路徑解析 ---
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot
if(-not $DataRoot){ $DataRoot = 'datahub' }

# --- Universe fallback（investable -> mini -> tw_all）---
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){ Write-Warning "UniverseFile not found; fallback => $cand"; $UniverseFile = $cand }
  else { throw "UniverseFile not found and no fallback available." }
}

# --- flags（僅控制是否執行；不傳給 Python）---
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }
if (-not (Test-Path variable:BatchSize))  { $Script:BatchSize  = 80    }

# --- Python 探測 ---
if (-not (Test-Path variable:PY)) {
  $Script:PY = Join-Path $RepoRoot '.venv\Scripts\python.exe'
  if (-not (Test-Path -LiteralPath $Script:PY)) { $Script:PY = 'python' }
}

# --- helpers ---
function Get-SymbolsFromFile([string]$path){
  if(-not (Test-Path -LiteralPath $path)) { return @() }
  return Get-Content -LiteralPath $path |
    Where-Object { $_ -match '\S' -and $_ -notmatch '^\s*#' } |
    ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }
}
function New-Batches([object[]]$items, [int]$size){
  if(-not $items -or $items.Count -eq 0){ return @() }
  if($size -le 0){ $size = 80 }
  $out=@()
  for($i=0; $i -lt $items.Count; $i+=$size){
    $r=[Math]::Min($i+$size-1,$items.Count-1)
    $out+=,($items[$i..$r] -join ',')
  }
  return $out
}
function Invoke-FinMind([string]$dataset,[string]$ids,[string]$s,[string]$e){
  # 注意：--end 為不含終點；固定帶 --datahub-root 指到根
  $args = @('.\scripts\finmind_backfill.py','--datasets',$dataset,'--symbols',$ids,'--start',$s,'--end',$e,'--datahub-root',$DataRoot)
  Write-Host ">> finmind_backfill $dataset symbols=$ids start=$s end(exclusive)=$e datahub=$DataRoot"
  & $PY @args
  if($LASTEXITCODE -ne 0){ throw "finmind_backfill failed for $dataset (ids=$ids)" }
}

# --- 預設 3 天視窗（end 為不含終點，預設 +1 天） ---
if(-not $Start -or -not $End){
  $Start = (Get-Date).AddDays(-3).ToString('yyyy-MM-dd')
  $End   = (Get-Date).AddDays(1).ToString('yyyy-MM-dd')  # end is exclusive (+1 day)
}

# --- 切批 ---
$SYMS = @(Get-SymbolsFromFile $UniverseFile)
if($SYMS.Count -eq 0){ throw "Universe empty: $UniverseFile" }
if (-not (Test-Path variable:BatchSymbols)) { $Script:BatchSymbols = New-Batches $SYMS $Script:BatchSize }

# --- Minimal RatePlan ---
if($DoPrices){   foreach($ids in $BatchSymbols){ Invoke-FinMind 'TaiwanStockPrice' $ids $Start $End } }
if($DoChip){     foreach($ids in $BatchSymbols){ Invoke-FinMind 'TaiwanStockInstitutionalInvestorsBuySell' $ids $Start $End } }
if($DoDividend){ foreach($ids in $BatchSymbols){ Invoke-FinMind 'TaiwanStockDividend' $ids '2004-01-01' $End } }
if($DoPER){      foreach($ids in $BatchSymbols){ Invoke-FinMind 'TaiwanStockPER' $ids '2004-01-01' $End } }

Write-Host "[OK] RatePlan minimal backfill done. Rules=$Rules Universe=$UniverseFile DataRoot=$DataRoot Start=$Start End(exclusive)=$End" -ForegroundColor Green
