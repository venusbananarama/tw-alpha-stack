#requires -Version 7.0
[CmdletBinding()]
param(
  [Parameter(Mandatory)][ValidateScript({Test-Path $_ -PathType Leaf})][string]$UniversePath,
  [Parameter(Mandatory)][ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$Start,
  [Parameter(Mandatory)][ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$End,  # exclusive
  [ValidateRange(1,10000)][int]$BatchSize = 80,
  [string]$Datasets = '',
  [ValidateSet('hard','baseline')][string]$Preset = 'hard',
  [string]$CheckpointRoot = ''
)
Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'
$env:ALPHACITY_ALLOW = $env:ALPHACITY_ALLOW ?? '1'

function Get-Ids([string]$path){
  Get-Content -LiteralPath $path | ? { $_ -match '^\s*\d{4}\s*$' } | % { $_.Trim() } | ? { $_ }
}
function Split-Batches([string[]]$ids,[int]$size){
  $chunks=@(); for($i=0;$i -lt $ids.Count;$i+=$size){ $lo=$i;$hi=[Math]::Min($i+$size-1,$ids.Count-1); $chunks+=,($ids[$lo..$hi]) }
  return ,$chunks  # 永遠為「陣列的陣列」
}
function Resolve-ExtrasPath{
  $p1 = Join-Path $PSScriptRoot '..\dateid\Run-DateID-Extras-Fixed.ps1'
  if(Test-Path $p1){ return (Resolve-Path $p1).Path }
  $p2 = '.\tools\dateid\Run-DateID-Extras-Fixed.ps1'
  if(Test-Path $p2){ return (Resolve-Path $p2).Path }
  throw '找不到 tools\dateid\Run-DateID-Extras-Fixed.ps1'
}
function Get-DatasetsByPreset([string]$preset){
  $map = @{
    'hard'     = '.\configs\presets\datasets.hard.txt'
    'baseline' = '.\configs\presets\datasets.baseline.txt'
  }
  $path = $map[$preset]
  if(Test-Path $path){ return (Get-Content -LiteralPath $path -Raw).Trim() }
  if($preset -eq 'hard'){
    return 'TaiwanStockShareholding TaiwanStockKBar TaiwanStockMarketValue TaiwanStockMarketValueWeight TaiwanStockSplitPrice TaiwanStockParValueChange TaiwanStockCapitalReductionReferencePrice TaiwanStockDelisting'
  } else {
    return 'TaiwanStockPrice TaiwanStockInstitutionalInvestorsBuySell'
  }
}
function Invoke-DateIDSingleDay([string]$extrasPath,[string]$dateStr,[string[]]$ids,[string]$dataset){
  $idsArg = ($ids -join ' ')
  $ok=$false;$lastErr=$null
  foreach($try in @(
    { & $extrasPath -Date    $dateStr -IDs $idsArg -Datasets $dataset },
    { & $extrasPath -DateStr $dateStr -IDs $idsArg -Datasets $dataset },
    { & $extrasPath -D       $dateStr -IDs $idsArg -Datasets $dataset }
  )){
    try{ & $try; if($LASTEXITCODE -eq $null -or $LASTEXITCODE -eq 0){ $ok=$true; break } } catch{ $lastErr=$_ }
  }
  if(-not $ok){
    $PY     = '.\.venv\Scripts\python.exe'
    $endStr = (Get-Date $dateStr).AddDays(1).ToString('yyyy-MM-dd')  # --end 不含終點（exclusive）
    $idsCsv = ($ids -join ',')
    & $PY .\scripts\fm_dateid_fetch.py --datasets $dataset --ids $idsCsv --date $dateStr --end $endStr --out-root datahub
    if($LASTEXITCODE -ne 0){ throw "fallback python failed: $($lastErr?.Exception.Message)" }
  }
}

$StartDt=[datetime]::ParseExact($Start,'yyyy-MM-dd',$null)
$EndDt  =[datetime]::ParseExact($End,'yyyy-MM-dd',$null)
if($EndDt -le $StartDt){ throw 'End 必須晚於 Start；且為不含終點（exclusive）。' }  # 與藍圖一致

[string[]]$IDsAll = Get-Ids $UniversePath
$Batches = Split-Batches $IDsAll $BatchSize
$extras  = Resolve-ExtrasPath

# 決定 datasets：優先用 -Datasets；否則依 Preset
$dsText = ($Datasets -and $Datasets.Trim()) ? $Datasets : (Get-DatasetsByPreset -preset $Preset)
[string[]]$dsList = $dsText -split '\s+' | ? { $_ }

for($d=$StartDt;$d -lt $EndDt;$d=$d.AddDays(1)){
  $DayStr=$d.ToString('yyyy-MM-dd')
  foreach($ds in $dsList){
    for($bi=0;$bi -lt $Batches.Count;$bi++){
      [string[]]$batch=$Batches[$bi]; if(-not $batch -or $batch.Count -eq 0){ continue }
      $cpFile=$null
      if($CheckpointRoot){
        $cpDir=Join-Path $CheckpointRoot (Join-Path $DayStr $ds)
        New-Item -ItemType Directory -Force -Path $cpDir | Out-Null
        $cpFile=Join-Path $cpDir ("batch_{0:D4}.ok" -f $bi)
        if(Test-Path $cpFile){ Write-Host "[SKIP] $cpFile"; continue }
      }
      try{
        Invoke-DateIDSingleDay -extrasPath $extras -dateStr $DayStr -ids $batch -dataset $ds
        if($cpFile){ "OK $DayStr $ds batch=$bi" | Set-Content -LiteralPath $cpFile -Encoding ascii }
        Write-Host "[OK] $DayStr $ds batch=$bi"
      }catch{
        Write-Warning "[FAIL] $DayStr $ds batch=$bi : $($_.Exception.Message)"
      }
    }
  }
}
