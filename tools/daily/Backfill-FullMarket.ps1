<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)# FAST extras passthrough（僅當呼叫端有傳入時）
$extras = @{}; foreach($k in 'Qps','BatchSize','MaxConcurrency','MaxRetries','RetryDelaySec'){ if($PSBoundParameters.ContainsKey($k)){ $extras[$k] = $PSBoundParameters[$k] } }][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce"; try{ & $DivForce -From $s.ToString("yyyy-MM-dd") -To $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[dividend] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce"; try{ & $DivForce -From $s.ToString("yyyy-MM-dd") -To $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[dividend] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce"; try{ & $DivForce -From $s.ToString("yyyy-MM-dd") -To $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[dividend] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce"; try{ & $DivForce -From $s.ToString("yyyy-MM-dd") -To $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[dividend] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce"; try{ & $DivForce -From $s.ToString("yyyy-MM-dd") -To $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[dividend] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce"; try{ & $DivForce -From $s.ToString("yyyy-MM-dd") -To $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[dividend] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip";   try{ & $Chip   -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[chip] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices"; try{ & $Prices -Start $s.ToString("yyyy-MM-dd") -End $e.ToString("yyyy-MM-dd"); $didAny=$true } catch { Write-Warning "[prices] wrapper call failed: $(<#
  Full-market historical backfill for prices/chip/dividend/per (End exclusive).
  Uses existing daily scripts; retries per task; no external deps.
#>
[CmdletBinding()]
param(
  [string]$Start = '2000-01-01',
  [Parameter(Mandatory)][string]$End,
  [string[]]$Tables = @('prices','chip','dividend','per'),
  [int]$MaxRetries = 3,
  [int]$RetryDelaySec = 10,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# normalize tables
$Tables = @($Tables | ForEach-Object { $_ -split '[,\s]+' } | Where-Object { $_ }) |
          ForEach-Object { $_.ToLower() } | Select-Object -Unique

# dates (End exclusive)
try { $s = [datetime]::Parse($Start).Date } catch { throw "Invalid -Start: $Start" }
try { $e = [datetime]::Parse($End).Date   } catch { throw "Invalid -End: $End" }
if($e -le $s){ throw "End ($End) must be greater than Start ($Start). End is exclusive." }

# resolve scripts (based on tools\daily)
function TryPath([string]$p){ if(Test-Path $p){ (Resolve-Path $p).Path } else { $null } }
$dailyRoot = Split-Path -Parent $PSCommandPath
$Prices    = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Prices.ps1')
$Chip      = TryPath (Join-Path $dailyRoot 'Daily-Backfill-Chip.ps1')
$DivForce  = TryPath (Join-Path $dailyRoot 'Backfill-Dividend-Force.ps1')
$RateFast  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.fast.ps1')
$RatePlan  = TryPath (Join-Path $dailyRoot 'Backfill-RatePlan.ps1')

function Get-ParamNames([string]$path){
  $tok=$null;$err=$null
  $ast=[System.Management.Automation.Language.Parser]::ParseFile($path,[ref]$tok,[ref]$err)
  if($ast -and $ast.ParamBlock){ $ast.ParamBlock.Parameters.Name.VariablePath.UserPath } else { @() }
}

function Invoke-WithRetry([string]$scriptPath,[hashtable]$args,[string]$label){
  if(-not $scriptPath){ Write-Warning "[$label] missing script, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label $scriptPath $($args.Keys -join ',')"; return $true }
      & $scriptPath @args
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

function Invoke-RatePlanWithRetry([string]$rp,[hashtable]$args,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[$label] missing RatePlan, skip"; return $false }
  $ok=$false; $lastErr=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN] $label . $rp $($args.Keys -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend = $doDividend; $DoPER = $doPer   # flags consumed by RatePlan scripts
      . $rp @args                                # dot-source
      $ok=$true; break
    } catch { $lastErr=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[$label] failed after $MaxRetries tries: $lastErr" }
  return $ok
}

$didAny=$false

# prices
if('prices' -in $Tables){
  if($Prices){
    $p = Get-ParamNames $Prices
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Prices @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'prices') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Prices @{Date=$d.ToString('yyyy-MM-dd')} 'prices' | Out-Null; $didAny=$true }
    } else { Write-Warning "[prices] unsupported parameters in $Prices" }
  } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "prices: Daily-Backfill-Prices.ps1 not found" }
}

# chip
if('chip' -in $Tables){
  if($Chip){
    $p = Get-ParamNames $Chip
    if(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $Chip @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'chip') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $Chip @{Date=$d.ToString('yyyy-MM-dd')} 'chip' | Out-Null; $didAny=$true }
    } else { Write-Warning "[chip] unsupported parameters in $Chip" }
  } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } else { Write-Warning "chip: Daily-Backfill-Chip.ps1 not found" }
}

# dividend（優先 Dividend-Force；缺時由 RatePlan 代填）
if('dividend' -in $Tables){
  if($DivForce){
    $p = Get-ParamNames $DivForce
    if(('From' -in $p) -and ('To' -in $p)){
      if(-not $env:FINMIND_TOKEN){ Write-Warning "FINMIND_TOKEN not set; Dividend may be throttled." }
      $didAny = (Invoke-WithRetry $DivForce @{From=$s.ToString('yyyy-MM-dd'); To=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif(('Start' -in $p) -and ('End' -in $p)){
      $didAny = (Invoke-WithRetry $DivForce @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} 'dividend') -or $didAny
    } elseif('Date' -in $p){
      for($d=$s; $d -lt $e; $d=$d.AddDays(1)){ Invoke-WithRetry $DivForce @{Date=$d.ToString('yyyy-MM-dd')} 'dividend' | Out-Null; $didAny=$true }
    } else { Write-Warning "[dividend] unsupported parameters in $DivForce" }
  } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}.Exception.Message)" } } elseif($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $true $false 'dividend via RatePlan') -or $didAny
  } else { Write-Warning "dividend: no available script" }
}

# per（靠 RatePlan）
if('per' -in $Tables){
  if($RateFast -or $RatePlan){
    $rp = $RateFast ? $RateFast : $RatePlan
    $didAny = (Invoke-RatePlanWithRetry2 $rp @{Start=$s.ToString('yyyy-MM-dd'); End=$e.ToString('yyyy-MM-dd')} $false $true 'per via RatePlan') -or $didAny
  } else { Write-Warning "per: RatePlan scripts not found" }
}

if($didAny){ Write-Host "[DONE] FullMarket backfill completed."; exit 0 }
Write-Warning "[WARN] Nothing executed."; exit 0

# safe RatePlan invoker (avoid $args binding)
function Invoke-RatePlanWithRetry2([string]$rp,[hashtable]$ParamMap,[bool]$doDividend,[bool]$doPer,[string]$label){
  if(-not $rp){ Write-Warning "[] missing RatePlan, skip"; return $false }
  $ok=$false; $last=$null
  for($i=1;$i -le $MaxRetries;$i++){
    try{
      if($DryRun){ Write-Host "[DRYRUN]  . $rp $(($ParamMap.Keys) -join ',') flags: Dividend=$doDividend PER=$doPer"; return $true }
      $DoDividend=$doDividend; $DoPER=$doPer
      . $rp @ParamMap
      $ok=$true; break
    } catch { $last=$_.Exception.Message; Start-Sleep -Seconds $RetryDelaySec }
  }
  if(-not $ok){ Write-Warning "[] failed after $MaxRetries tries: $last" }
  return $ok
}
