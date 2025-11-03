#requires -Version 7
[CmdletBinding(PositionalBinding=$false)]
param(
  [string]$Rules        = '.\rules.yaml',
  [string]$UniverseFile = '.\configs\universe.tw_alpha.txt',
  [string]$DataRoot     = 'datahub',
  [string]$Start,
  [string]$End
)
Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = C:\AI\tw-alpha-stack
$RepoRoot = (Get-Item -LiteralPath $PSScriptRoot).Parent.Parent.FullName
function Resolve-FromRoot([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
$Rules        = Resolve-FromRoot $Rules
$UniverseFile = Resolve-FromRoot $UniverseFile
if(-not (Test-Path -LiteralPath $Rules))        { throw "File missing: $Rules" }
if(-not (Test-Path -LiteralPath $UniverseFile)) { throw "File missing: $UniverseFile" }
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function Resolve-FromRoot([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  try { return (Resolve-Path -LiteralPath $p -ErrorAction Stop).Path } catch {}
  try { return (Resolve-Path -LiteralPath (Join-Path $RepoRoot $p) -ErrorAction Stop).Path } catch { return $p }
}
$Rules        = Resolve-FromRoot $Rules
$UniverseFile = Resolve-FromRoot $UniverseFile
if(-not (Test-Path -LiteralPath $Rules))        { throw "File missing: $Rules" }
if(-not (Test-Path -LiteralPath $UniverseFile)) { throw "File missing: $UniverseFile" }
# -- repo-root （tools\daily → 上兩層 = 專案根） --
$ROOT = (Get-Item -LiteralPath $PSScriptRoot).Parent.Parent.FullName
function _Resolve([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $ROOT $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Resolve $Rules
$UniverseFile = _Resolve $UniverseFile
if(-not (Test-Path -LiteralPath $Rules))        { throw "File missing: $Rules" }
if(-not (Test-Path -LiteralPath $UniverseFile)) { throw "File missing: $UniverseFile" }
$ErrorActionPreference = 'Stop'

# -- repo-root 感知路徑解析（tools\daily 往上兩層） --
$__repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
function __Resolve([string]$p){
  if(-not [string]::IsNullOrWhiteSpace($p) -and (Test-Path -LiteralPath $p)){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $__repoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = __Resolve $Rules
$UniverseFile = __Resolve $UniverseFile
if(-not (Test-Path -LiteralPath $Rules))        { throw "File missing: $Rules" }
if(-not (Test-Path -LiteralPath $UniverseFile)) { throw "File missing: $UniverseFile" }

if (-not $DataRoot) { $DataRoot = "datahub" }

$Tools = Split-Path -Parent $PSCommandPath; $Root = Split-Path -Parent $Tools
$Reports = Join-Path $Root 'reports'; New-Item -ItemType Directory -Force -Path $Reports | Out-Null
if (-not (Split-Path -IsAbsolute $UniverseFile)) { $UniverseFile = Join-Path $Root $UniverseFile }
if (-not (Split-Path -IsAbsolute $Rules))        { $Rules        = Join-Path $Root $Rules }
if (-not (Split-Path -IsAbsolute $DataRoot))     { $DataRoot     = Join-Path $Root $DataRoot }

# 防呆：路徑與 KILLSWITCH
$DataRoot = ($DataRoot -replace '[\\/]+','\') -replace '(silver\\alpha\\?)+$',''
if ($env:ALPHACITY_ALLOW -ne '1') { throw "ALPHACITY_ALLOW not set" }
if (-not (Test-Path $Rules -PathType Leaf))        { throw "File missing: $Rules" }
if (-not (Test-Path $UniverseFile -PathType Leaf)) { throw "File missing: $UniverseFile" }
New-Item -ItemType Directory -Force -Path (Join-Path $DataRoot 'silver\alpha') | Out-Null
Write-Host "[paths] root=$Root rules=$Rules universe=$UniverseFile data=$DataRoot"

# Resolve Python
$Py = Join-Path $Root '.venv\Scripts\python.exe'; $PY_ARGS=@()
if (-not (Test-Path $Py)) {
  if (Get-Command py -ErrorAction SilentlyContinue) { $Py='py'; $PY_ARGS=@('-3.11') }
  elseif (Get-Command python -ErrorAction SilentlyContinue) { $Py=(Get-Command python).Path }
  else { throw 'Python not found' }
}

function RunPy([string[]]$ArgList){
  $oldAllow=$env:ALPHACITY_ALLOW; $env:ALPHACITY_ALLOW='1'
  $oldStartup=$env:PYTHONSTARTUP; Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue
  & $Py @PY_ARGS @ArgList; $code=$LASTEXITCODE
  if ($oldStartup) { $env:PYTHONSTARTUP=$oldStartup } else { Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue }
  $env:ALPHACITY_ALLOW=$oldAllow
  if ($code -notin 0,2) { throw ("Exit {0}: {1}" -f $code, ($ArgList -join ' ')) }
}
function RunPyRetry([string[]]$ArgList){
  for($i=0;$i -lt 12;$i++){
    try { RunPy $ArgList; return } catch {
      $m = ($_ | Out-String)
      if($m -match "402|Too Many Requests|rate limit"){ $w=[Math]::Min(300,30*($i+1)); Write-Host ("[retry] 402 wait {0}s retry {1}/12" -f $w,$i+1); Start-Sleep -Seconds $w }
      else { throw }
    }
  }; throw "Run failed after 12 retries"
}

function Read-Symbols([string]$path){
  $syms = Get-Content $path | ForEach-Object { ($_ -split '[\s,]')[0].Trim() } |
          Where-Object { $_ -match '^\d{4}(\.TW|\.TWO)?$' } |
          ForEach-Object { $_.Substring(0,4) } | Select-Object -Unique
  if ($syms.Count -eq 0) { throw "Universe empty: $path" }; return $syms
}
function Split-Array([object[]]$a,[int]$n){ for($i=0;$i -lt $a.Count;$i+=$n){ ,($a[$i..([Math]::Min($i+$n-1,$a.Count-1))]) } }
function DayStr($d){ $d.ToString('yyyy-MM-dd') }

$today=Get-Date; $monthStart=Get-Date -Date (Get-Date -Day 1).Date
$PriceStart=DayStr $monthStart; $ChipStart=DayStr ($monthStart.AddDays(-4)); $DivStart=DayStr ([DateTime]::new($today.Year,1,2)); $END=DayStr $today

function Make-Args([string]$start,[string]$end,[string]$dataset,[string[]]$symbols,[int]$hourCap){
  $args=@('scripts/finmind_backfill.py','--start',$start,'--end',$end,'--datasets',$dataset,'--symbols') + $symbols +
        @('--workers','1','--qps',("{0:0.00}" -f $Qps),'--hourly-cap',$hourCap.ToString(),'--datahub-root',$DataRoot)
  if ($env:FINMIND_TOKEN) { $args += @('--api-token',$env:FINMIND_TOKEN) }; return $args
}

$SYMS=Read-Symbols $UniverseFile
$bootstrap=[string[]]@('2330','2317','6669')
if($DoPrices){ RunPyRetry (Make-Args $PriceStart $END 'TaiwanStockPrice' $bootstrap 60) }
if($DoChip){
  $chipBoot=@('scripts/finmind_backfill.py','--start',$ChipStart,'--end',$END,'--datasets','TaiwanStockInstitutionalInvestorsBuySell','--symbols') + $bootstrap +
           @('--workers','1','--qps',("{0:0.00}" -f $Qps),'--hourly-cap','60','--datahub-root',$DataRoot); if ($env:FINMIND_TOKEN) { $chipBoot += @('--api-token',$env:FINMIND_TOKEN) }
  RunPyRetry $chipBoot
}

if($DoPrices){
  $b=@(Split-Array $SYMS $BatchSymbols); $i=0; $tot=$b.Count
  foreach($grp in $b){ $i++; Write-Host ("[prices] {0}/{1} size={2}" -f $i,$tot,$grp.Count); RunPyRetry (Make-Args $PriceStart $END 'TaiwanStockPrice' $grp $HourCapPrices); Start-Sleep -Seconds $SleepBetween }
}
if($DoChip){
  $b2=@(Split-Array $SYMS $BatchSymbols); $j=0; $tot2=$b2.Count
  foreach($grp in $b2){ $j++; Write-Host ("[chip]   {0}/{1} size={2}" -f $j,$tot2,$grp.Count);
    $args=@('scripts/finmind_backfill.py','--start',$ChipStart,'--end',$END,'--datasets','TaiwanStockInstitutionalInvestorsBuySell','--symbols') + $grp +
          @('--workers','1','--qps',("{0:0.00}" -f $Qps),'--hourly-cap',$HourCapChip,'--datahub-root',$DataRoot); if ($env:FINMIND_TOKEN) { $args += @('--api-token',$env:FINMIND_TOKEN) }
    RunPyRetry $args; Start-Sleep -Seconds $SleepBetween }
}
if($DoDiv){
  $b3=@(Split-Array $SYMS $BatchSymbols); $k=0; $tot3=$b3.Count
  foreach($grp in $b3){ $k++; Write-Host ("[div]    {0}/{1} size={2}" -f $k,$tot3,$grp.Count); RunPyRetry (Make-Args $DivStart $END 'TaiwanStockDividend' $grp $HourCapDiv); Start-Sleep -Seconds $SleepBetween }
}
if($DoPER){
  $b4=@(Split-Array $SYMS $BatchSymbols); $m=0; $tot4=$b4.Count
  foreach($grp in $b4){ $m++; Write-Host ("[per]    {0}/{1} size={2}" -f $m,$tot4,$grp.Count); RunPyRetry (Make-Args "2022-09-29" $END 'TaiwanStockPER' $grp 800); Start-Sleep -Seconds $SleepBetween }
}

RunPyRetry @('scripts/build_universe.py','--rules',$Rules,'--out',(Join-Path $Root 'configs\investable_universe.txt'))
RunPyRetry @('scripts/preflight_check.py','--rules',$Rules,'--export','reports')
RunPyRetry @('scripts/wf_runner.py','--summary','--export',(Join-Path $Reports 'gate_summary.json'))
Write-Host '[Backfill-RatePlan.fast] Completed.'

