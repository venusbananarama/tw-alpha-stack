#requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\configs\universe.tw_all.txt', '.\configs\universe.tw_all',
    '.\configs\groups\ALL', '.\configs\groups\ALL.txt',
    '.\universe\universe.tw_all.txt', '.\universe\tw_all.txt', '.\universe\all.txt'
  )
  foreach($p in $candidates){
    if(Test-Path $p){
      $ids = Get-Content -LiteralPath $p | ForEach-Object {
        $x = #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  # 候選（由高到低）：investable_universe → configs\universe.tw_all(.txt) → universe\*.txt
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\configs\universe.tw_all.txt',
    '.\configs\universe.tw_all',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs/universe 皆無），請先建立或放置池檔。" }
  return Get-Content -LiteralPath $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

.Trim().Replace('.TW','') } |
         Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 -match '^\d{4}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 } | Sort-Object -Unique
}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"


.Trim()
        if ($x -match '^\s*(\d{4})(?:\.TW)?\b') { $matches[1] }  # 接受 2330 / 2330.TW / 2330,xxx
      } | Sort-Object -Unique
      if($ids.Count -gt 0){ return $ids }
    }
  }
  throw "找不到有效池表或內容為空；候選：" + ($candidates -join ', ')
}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs/universe 皆無），請先建立或放置池檔。" }
  return Get-Content -LiteralPath $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\configs\universe.tw_all.txt', '.\configs\universe.tw_all',
    '.\configs\groups\ALL', '.\configs\groups\ALL.txt',
    '.\universe\universe.tw_all.txt', '.\universe\tw_all.txt', '.\universe\all.txt'
  )
  foreach($p in $candidates){
    if(Test-Path $p){
      $ids = Get-Content -LiteralPath $p | ForEach-Object {
        $x = #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  # 候選（由高到低）：investable_universe → configs\universe.tw_all(.txt) → universe\*.txt
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\configs\universe.tw_all.txt',
    '.\configs\universe.tw_all',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs/universe 皆無），請先建立或放置池檔。" }
  return Get-Content -LiteralPath $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

.Trim().Replace('.TW','') } |
         Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 -match '^\d{4}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 } | Sort-Object -Unique
}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"


.Trim()
        if ($x -match '^\s*(\d{4})(?:\.TW)?\b') { $matches[1] }  # 接受 2330 / 2330.TW / 2330,xxx
      } | Sort-Object -Unique
      if($ids.Count -gt 0){ return $ids }
    }
  }
  throw "找不到有效池表或內容為空；候選：" + ($candidates -join ', ')
}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

.Trim().Replace('.TW','') } |
         Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\configs\universe.tw_all.txt', '.\configs\universe.tw_all',
    '.\configs\groups\ALL', '.\configs\groups\ALL.txt',
    '.\universe\universe.tw_all.txt', '.\universe\tw_all.txt', '.\universe\all.txt'
  )
  foreach($p in $candidates){
    if(Test-Path $p){
      $ids = Get-Content -LiteralPath $p | ForEach-Object {
        $x = #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  # 候選（由高到低）：investable_universe → configs\universe.tw_all(.txt) → universe\*.txt
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\configs\universe.tw_all.txt',
    '.\configs\universe.tw_all',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs/universe 皆無），請先建立或放置池檔。" }
  return Get-Content -LiteralPath $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

.Trim().Replace('.TW','') } |
         Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-PoolIDs {
  $candidates = @(
    '.\configs\investable_universe.txt',
    '.\universe\universe.tw_all.txt',
    '.\universe\tw_all.txt',
    '.\universe\all.txt'
  )
  $p = $candidates | Where-Object { Test-Path #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 -match '^\d{4}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 } | Sort-Object -Unique
}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"


.Trim()
        if ($x -match '^\s*(\d{4})(?:\.TW)?\b') { $matches[1] }  # 接受 2330 / 2330.TW / 2330,xxx
      } | Sort-Object -Unique
      if($ids.Count -gt 0){ return $ids }
    }
  }
  throw "找不到有效池表或內容為空；候選：" + ($candidates -join ', ')
}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 -match '^\d{4}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"

 } | Sort-Object -Unique
}
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Select-Object -First 1
  if(-not $p){ throw "找不到池表（configs\investable_universe.txt / universe\universe.tw_all.txt / tw_all.txt / all.txt）" }
  return Get-Content $p | ForEach-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
.Trim().Replace('.TW','') } | Where-Object { #requires -Version 7
<#
  Run-Max-Recent.ps1
  近 N 日｜Date-ID 特攻 orchestrator（單線 + 節流 + checkpoint）
  - 用於「必收 8 表」等 extras，落地 extra/<Dataset>；不動四表主線。
#>

[CmdletBinding(PositionalBinding=$false)]
param(
  [ValidateRange(1,120)][int]$Days = 7,
  [string[]]$IDs,   # 空值則讀池表（全市場）
  [string]$Datasets = 'TaiwanStockShareholding,TaiwanStockKBar,TaiwanStockMarketValue,TaiwanStockMarketValueWeight,TaiwanStockSplitPrice,TaiwanStockParValueChange,TaiwanStockCapitalReductionReferencePrice,TaiwanStockDelisting',
  [ValidateRange(6,120)][int]$RPM = 48,
  [ValidateRange(1,2000)][int]$Batch = 300,
  [ValidateRange(0,600000)][int]$SleepMsBetweenBatches = 2000,
  [string]$Checkpoint = '.\reports\dateid_maxrecent_checkpoint.json',
  [switch]$Strict
)

Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 -match '^\d{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 } | Sort-Object -Unique
}

function Convert-TokenToLike4([string]$token){
  $t = ($token -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
  if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
) { return '????' }
  $like = ''
  foreach($ch in $t.ToCharArray()){
    if ($like.Length -ge 4) { break }
    switch -Regex ($ch) {
      '^\d
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { $like += $ch; break }
      '^[Xx\?]
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
 { $like += '?'; break }
      '^\*
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
     { while($like.Length -lt 4){ $like += '?' }; break }
      default    { break }
    }
  }
  while($like.Length -lt 4){ $like += '?' }
  return $like
}

function Expand-IDPatterns([string[]]$IDs){
  $pool = Get-PoolIDs
  $set  = New-Object System.Collections.Generic.HashSet[string]
  foreach($tok in @($IDs)){
    foreach($raw in ( ($tok -is [string] -and $tok -match ',') ? $tok.Split(',') : @($tok) )){
      $t = ($raw -replace '\.TW
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
,'').Trim()
      if (-not $t) { continue }
      if ($t -match '^(ALL|TSE)
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ foreach($id in $pool){ [void]$set.Add($id) }; continue }
      if ($t -match '^[0-9]{4}
# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"
){ [void]$set.Add($t); continue }
      $like = Convert-TokenToLike4 $t
      foreach($id in $pool){ if ($id -like $like) { [void]$set.Add($id) } }
    }
  }
  return @($set | Sort-Object)
}

# 專案根
$tryRoots = @(
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..\..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..') -ErrorAction SilentlyContinue),
  (Resolve-Path -LiteralPath '.' -ErrorAction SilentlyContinue)
) | Where-Object { $_ -ne $null }
$root = ($tryRoots | Where-Object { Test-Path (Join-Path $_.Path 'cal\trading_days.csv') } | Select-Object -First 1).Path
if(-not $root){ throw "無法定位專案根（缺 cal\trading_days.csv）" }
Set-Location $root
if(-not $env:ALPHACITY_ALLOW){ $env:ALPHACITY_ALLOW='1' }
Remove-Item Env:PYTHONSTARTUP -EA SilentlyContinue | Out-Null

# 找 Date-ID 主腳本（外掛）
$RUNS = @(
  '.\tools\Run-DateID-Extras.ps1',
  '.\tools\Run-DateID-Extras-Fixed.ps1',
  '.\tools\dateid\Run-DateID-Extras.ps1'
)
$RUN = ($RUNS | Where-Object { Test-Path $_ } | Select-Object -First 1)
if(-not $RUN){ throw "找不到 Run-DateID-Extras.ps1" }

# 讀交易日曆 → 近 N 個活交易日（台北 <= 今天）
$cal = Import-Csv .\cal\trading_days.csv | ForEach-Object { [datetime]::ParseExact($_.date,'yyyy-MM-dd',$null) } | Sort-Object
$todayTpe = (Get-Date).AddHours(8).Date
$liveDays = @($cal | Where-Object { $_ -le $todayTpe })
if($liveDays.Count -eq 0){ throw "交易日曆為空或全都大於今天(台北)" }
$take = [math]::Min($Days, $liveDays.Count)
$days = $liveDays[-$take..-1]  # 近 N 日；由舊到新

# 讀全市場池或樣板展開
if(-not $IDs -or $IDs.Count -eq 0){ $IDs = Get-PoolIDs }
$expandedIDs = Expand-IDPatterns @($IDs)
if(-not $expandedIDs){ throw "IDs 展開後為空（請檢查樣板或投資池）" }
$syms = foreach($id in $expandedIDs){ "$id.TW" }
$syms = foreach($x in $IDs){ $y=$x.Trim(); if($y -notmatch '\.'){"$y.TW"} else {$y} }

# checkpoint
New-Item -ItemType Directory -Force -Path '.\reports' | Out-Null
$state = @{}
if(Test-Path $Checkpoint){ try{ $state = Get-Content $Checkpoint -Raw | ConvertFrom-Json }catch{} }
$log = 'reports\dateid_maxrecent_{0}.log' -f (Get-Date -Format 'yyyyMMdd_HHmmss')
"MAX-RECENT days=$Days ids=$($syms.Count) rpm=$RPM batch=$Batch datasets=$Datasets" | Tee-Object -FilePath $log

# 節流（單線；遇 402/429 自動降到 >=6）
$env:FINMIND_THROTTLE_RPM = [string]$RPM
function Invoke-Chunk([datetime]$d, [string[]]$chunk){
  $ds = $d.ToString('yyyy-MM-dd')
  $idsArg = ($chunk -join ',')
  $ok=$false; $tryRPM=[int]$env:FINMIND_THROTTLE_RPM
  while(-not $ok){
    try{
      # Run-DateID-Extras.ps1 的介面以 -Date/-IDs 為主；Datasets 依預設(必收 8)或你有擴充就加上 -Datasets
      pwsh -NoProfile -ExecutionPolicy Bypass -File $RUN -Date $ds -IDs $idsArg *>> $log
      $ok=$true
    }catch{
      if($tryRPM -gt 6){
        $tryRPM = [Math]::Max(6, [int]([double]$tryRPM * 0.7))
        $env:FINMIND_THROTTLE_RPM=[string]$tryRPM
        Start-Sleep -Seconds 5
      } else {
        if($Strict){ throw } else { "[WARN][$ds][$($chunk.Count)] $($_.Exception.Message)" | Tee-Object -FilePath $log -Append; break }
      }
    }
  }
}

# 主迴圈：逐日 × 分批
foreach($d in $days){
  $key = $d.ToString('yyyy-MM-dd')
  if($state.ContainsKey($key) -and $state[$key] -eq 'ok'){ Write-Host "[SKIP] $key"; continue }
  Write-Host "== [$key] =="
  for($i=0; $i -lt $syms.Count; $i+=$Batch){
    $j = [Math]::Min($i+$Batch-1, $syms.Count-1)
    Invoke-Chunk -d $d -chunk $syms[$i..$j]
    if($SleepMsBetweenBatches -gt 0){ Start-Sleep -Milliseconds $SleepMsBetweenBatches }
  }
  $state[$key]='ok'
  ($state | ConvertTo-Json -Depth 5) | Set-Content -LiteralPath $Checkpoint -Encoding utf8BOM
  Write-Host "[OK] $key"
}
Write-Host "Done. Log: $log"



