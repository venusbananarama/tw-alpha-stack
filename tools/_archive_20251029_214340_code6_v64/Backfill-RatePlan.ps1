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

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標與節流預設（舊內文相容） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }
if (-not (Test-Path variable:Qps))        { $Script:Qps        = 1.50  }
if (-not (Test-Path variable:HourlyCap))  { $Script:HourlyCap  = 6000  }

# === Python 執行路徑預設（舊內文可能依賴 $PY） ===
if (-not (Test-Path variable:PY)) {
  $Script:PY = Join-Path $RepoRoot '.venv\Scripts\python.exe'
  if (-not (Test-Path -LiteralPath $Script:PY)) { $Script:PY = 'python' }
}

# === 統一 $root / $data（內文若使用） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot

# --- v6 hotfix: sanitize args + default batching (auto-injected) ---
function Get-SymbolsFromFile([string]$path){
  if(-not (Test-Path -LiteralPath $path)) { return @() }
  return Get-Content -LiteralPath $path |
    Where-Object { #requires -Version 7
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

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標與節流預設（舊內文相容） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }
if (-not (Test-Path variable:Qps))        { $Script:Qps        = 1.50  }
if (-not (Test-Path variable:HourlyCap))  { $Script:HourlyCap  = 6000  }

# === Python 執行路徑預設（舊內文可能依賴 $PY） ===
if (-not (Test-Path variable:PY)) {
  $Script:PY = Join-Path $RepoRoot '.venv\Scripts\python.exe'
  if (-not (Test-Path -LiteralPath $Script:PY)) { $Script:PY = 'python' }
}

# === 統一 $root / $data（內文若使用） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標補齊（舊內文可能用到） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }

# === 統一 $root / $data（舊內文有時用 $PSScriptRoot） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
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

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else {
    throw "UniverseFile not found and no fallback available."
  }
}
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
if (-not (Test-Path $Rules -PathType Leaf))        { throw "File missing: $Rules" }
if (-not (Test-Path $UniverseFile -PathType Leaf)) { throw "File missing: $UniverseFile" }
New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
Write-Host "[paths] root=$Root rules=$Rules universe=$UniverseFile data=$DataRoot"

$Py = Join-Path $Root '.venv\Scripts\python.exe'; $PY_ARGS=@()
if (-not (Test-Path $Py)) { if (Get-Command py -ErrorAction SilentlyContinue) { $Py='py'; $PY_ARGS=@('-3.11') } elseif (Get-Command python -ErrorAction SilentlyContinue) { $Py=(Get-Command python).Path } else { throw 'No Python found' } }

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

RunPyRetry @('scripts/build_universe.py','--rules',$Rules)
RunPyRetry @('scripts/preflight_check.py','--rules',$Rules,'--export','reports')
RunPyRetry @('scripts/wf_runner.py','--summary','--export',(Join-Path $Reports 'gate_summary.json'))
Write-Host '[Backfill-RatePlan] Completed.'

 -match '\S' -and #requires -Version 7
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

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標與節流預設（舊內文相容） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }
if (-not (Test-Path variable:Qps))        { $Script:Qps        = 1.50  }
if (-not (Test-Path variable:HourlyCap))  { $Script:HourlyCap  = 6000  }

# === Python 執行路徑預設（舊內文可能依賴 $PY） ===
if (-not (Test-Path variable:PY)) {
  $Script:PY = Join-Path $RepoRoot '.venv\Scripts\python.exe'
  if (-not (Test-Path -LiteralPath $Script:PY)) { $Script:PY = 'python' }
}

# === 統一 $root / $data（內文若使用） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標補齊（舊內文可能用到） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }

# === 統一 $root / $data（舊內文有時用 $PSScriptRoot） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
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

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else {
    throw "UniverseFile not found and no fallback available."
  }
}
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
if (-not (Test-Path $Rules -PathType Leaf))        { throw "File missing: $Rules" }
if (-not (Test-Path $UniverseFile -PathType Leaf)) { throw "File missing: $UniverseFile" }
New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
Write-Host "[paths] root=$Root rules=$Rules universe=$UniverseFile data=$DataRoot"

$Py = Join-Path $Root '.venv\Scripts\python.exe'; $PY_ARGS=@()
if (-not (Test-Path $Py)) { if (Get-Command py -ErrorAction SilentlyContinue) { $Py='py'; $PY_ARGS=@('-3.11') } elseif (Get-Command python -ErrorAction SilentlyContinue) { $Py=(Get-Command python).Path } else { throw 'No Python found' } }

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

RunPyRetry @('scripts/build_universe.py','--rules',$Rules)
RunPyRetry @('scripts/preflight_check.py','--rules',$Rules,'--export','reports')
RunPyRetry @('scripts/wf_runner.py','--summary','--export',(Join-Path $Reports 'gate_summary.json'))
Write-Host '[Backfill-RatePlan] Completed.'

 -notmatch '^\s*#' } |
    ForEach-Object { #requires -Version 7
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

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標與節流預設（舊內文相容） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }
if (-not (Test-Path variable:Qps))        { $Script:Qps        = 1.50  }
if (-not (Test-Path variable:HourlyCap))  { $Script:HourlyCap  = 6000  }

# === Python 執行路徑預設（舊內文可能依賴 $PY） ===
if (-not (Test-Path variable:PY)) {
  $Script:PY = Join-Path $RepoRoot '.venv\Scripts\python.exe'
  if (-not (Test-Path -LiteralPath $Script:PY)) { $Script:PY = 'python' }
}

# === 統一 $root / $data（內文若使用） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標補齊（舊內文可能用到） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }

# === 統一 $root / $data（舊內文有時用 $PSScriptRoot） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
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

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else {
    throw "UniverseFile not found and no fallback available."
  }
}
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
if (-not (Test-Path $Rules -PathType Leaf))        { throw "File missing: $Rules" }
if (-not (Test-Path $UniverseFile -PathType Leaf)) { throw "File missing: $UniverseFile" }
New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
Write-Host "[paths] root=$Root rules=$Rules universe=$UniverseFile data=$DataRoot"

$Py = Join-Path $Root '.venv\Scripts\python.exe'; $PY_ARGS=@()
if (-not (Test-Path $Py)) { if (Get-Command py -ErrorAction SilentlyContinue) { $Py='py'; $PY_ARGS=@('-3.11') } elseif (Get-Command python -ErrorAction SilentlyContinue) { $Py=(Get-Command python).Path } else { throw 'No Python found' } }

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

RunPyRetry @('scripts/build_universe.py','--rules',$Rules)
RunPyRetry @('scripts/preflight_check.py','--rules',$Rules,'--export','reports')
RunPyRetry @('scripts/wf_runner.py','--summary','--export',(Join-Path $Reports 'gate_summary.json'))
Write-Host '[Backfill-RatePlan] Completed.'

.Trim() } | Where-Object { #requires -Version 7
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

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標與節流預設（舊內文相容） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }
if (-not (Test-Path variable:Qps))        { $Script:Qps        = 1.50  }
if (-not (Test-Path variable:HourlyCap))  { $Script:HourlyCap  = 6000  }

# === Python 執行路徑預設（舊內文可能依賴 $PY） ===
if (-not (Test-Path variable:PY)) {
  $Script:PY = Join-Path $RepoRoot '.venv\Scripts\python.exe'
  if (-not (Test-Path -LiteralPath $Script:PY)) { $Script:PY = 'python' }
}

# === 統一 $root / $data（內文若使用） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標補齊（舊內文可能用到） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }

# === 統一 $root / $data（舊內文有時用 $PSScriptRoot） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
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

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else {
    throw "UniverseFile not found and no fallback available."
  }
}
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
if (-not (Test-Path $Rules -PathType Leaf))        { throw "File missing: $Rules" }
if (-not (Test-Path $UniverseFile -PathType Leaf)) { throw "File missing: $UniverseFile" }
New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
Write-Host "[paths] root=$Root rules=$Rules universe=$UniverseFile data=$DataRoot"

$Py = Join-Path $Root '.venv\Scripts\python.exe'; $PY_ARGS=@()
if (-not (Test-Path $Py)) { if (Get-Command py -ErrorAction SilentlyContinue) { $Py='py'; $PY_ARGS=@('-3.11') } elseif (Get-Command python -ErrorAction SilentlyContinue) { $Py=(Get-Command python).Path } else { throw 'No Python found' } }

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

RunPyRetry @('scripts/build_universe.py','--rules',$Rules)
RunPyRetry @('scripts/preflight_check.py','--rules',$Rules,'--export','reports')
RunPyRetry @('scripts/wf_runner.py','--summary','--export',(Join-Path $Reports 'gate_summary.json'))
Write-Host '[Backfill-RatePlan] Completed.'

 -ne '' }
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
# 旗標/節流（供舊內文取用）
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }
if (-not (Test-Path variable:Qps))        { $Script:Qps        = 1.50   }   # 記錄用途（不再傳給 finmind_backfill）
if (-not (Test-Path variable:HourlyCap))  { $Script:HourlyCap  = 6000   }   # 記錄用途

# Python 路徑（舊內文若用 $PY）
if (-not (Test-Path variable:PY)) {
  $Script:PY = Join-Path $RepoRoot '.venv\Scripts\python.exe'
  if (-not (Test-Path -LiteralPath $Script:PY)) { $Script:PY = 'python' }
}

# $root/$data 統一（舊內文可能會用）
$Script:root = $RepoRoot
if(-not $DataRoot -or $DataRoot -eq ''){ $DataRoot = 'datahub' }
$Script:data = $DataRoot

# 以 Universe 產生預設批次（供內文使用的 $BatchSymbols）
if (-not (Test-Path variable:BatchSize))    { $Script:BatchSize = 80 }
if (-not (Test-Path variable:BatchSymbols)) {
  $sym = Get-SymbolsFromFile $UniverseFile
  $Script:BatchSymbols = New-Batches $sym $Script:BatchSize
}
# --- end of hotfix ---$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
function _Res([string]$p){
  if([string]::IsNullOrWhiteSpace($p)){ return $p }
  if(Test-Path -LiteralPath $p){ return (Resolve-Path -LiteralPath $p).Path }
  $q = Join-Path $RepoRoot $p
  if(Test-Path -LiteralPath $q){ return (Resolve-Path -LiteralPath $q).Path }
  return $p
}
if(-not $DataRoot){ $DataRoot = 'datahub' }
$Rules        = _Res $Rules
$UniverseFile = _Res $UniverseFile
$DataRoot     = _Res $DataRoot

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else { throw "UniverseFile not found and no fallback available." }
}

# === 缺失旗標補齊（舊內文可能用到） ===
if (-not (Test-Path variable:DoPrices))   { $Script:DoPrices   = $true  }
if (-not (Test-Path variable:DoChip))     { $Script:DoChip     = $true  }
if (-not (Test-Path variable:DoDividend)) { $Script:DoDividend = $false }
if (-not (Test-Path variable:DoPER))      { $Script:DoPER      = $false }

# === 統一 $root / $data（舊內文有時用 $PSScriptRoot） ===
$Script:root = $RepoRoot
$Script:data = $DataRoot
$ErrorActionPreference = 'Stop'

# repo-root = tools\daily\..\.. = 專案根
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

# UniverseFile 容錯：investable → mini → tw_all
if(-not (Test-Path -LiteralPath $UniverseFile)){
  $cand = @('.\configs\investable_universe.txt','.\configs\mini_universe.txt','.\configs\universe.tw_all.txt') |
          ForEach-Object { _Res $_ } | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1
  if($cand){
    Write-Warning "UniverseFile not found; fallback => $cand"
    $UniverseFile = $cand
  } else {
    throw "UniverseFile not found and no fallback available."
  }
}
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
if (-not (Test-Path $Rules -PathType Leaf))        { throw "File missing: $Rules" }
if (-not (Test-Path $UniverseFile -PathType Leaf)) { throw "File missing: $UniverseFile" }
New-Item -ItemType Directory -Force -Path $DataRoot | Out-Null
Write-Host "[paths] root=$Root rules=$Rules universe=$UniverseFile data=$DataRoot"

$Py = Join-Path $Root '.venv\Scripts\python.exe'; $PY_ARGS=@()
if (-not (Test-Path $Py)) { if (Get-Command py -ErrorAction SilentlyContinue) { $Py='py'; $PY_ARGS=@('-3.11') } elseif (Get-Command python -ErrorAction SilentlyContinue) { $Py=(Get-Command python).Path } else { throw 'No Python found' } }

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

RunPyRetry @('scripts/build_universe.py','--rules',$Rules)
RunPyRetry @('scripts/preflight_check.py','--rules',$Rules,'--export','reports')
RunPyRetry @('scripts/wf_runner.py','--summary','--export',(Join-Path $Reports 'gate_summary.json'))
Write-Host '[Backfill-RatePlan] Completed.'

