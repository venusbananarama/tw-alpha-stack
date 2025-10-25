#requires -Version 7

param(
  [string]$UniversePath = ".\configs\derived\universe_ids_only.txt",
  [string]$Start        = "2004-01-01",
  [string]$End          = ([datetime]::Now.ToString("yyyy-MM-dd")),
  [int]   $BatchSize    = 400,
  [int]   $WindowDays   = 1,
  [int]   $Workers      = 4,
  [switch]$Resume,
  [switch]$Strict
)

Import-Module ThreadJob -ErrorAction SilentlyContinue
$ErrorActionPreference = "Stop"
Set-Location (Resolve-Path "$PSScriptRoot\..")

if(-not $env:FINMIND_THROTTLE_RPM){ $env:FINMIND_THROTTLE_RPM='6' }
if(-not $env:FINMIND_KBAR_INTERVAL){ $env:FINMIND_KBAR_INTERVAL='5' }

# 載入 IDs（四碼），切批，過濾空批
$all = Get-Content $UniversePath | Where-Object { $_ -match '^\d{4}$' }
if(!$all){ throw "Universe empty: $UniversePath" }
$batches = for($i=0;$i -lt $all.Count;$i+=$BatchSize){ ($all[$i..([math]::Min($i+$BatchSize-1,$all.Count-1))] -join ',') }
$batches = $batches | Where-Object { $_ -and $_.Length -gt 0 }

function Invoke-Batches {
  param([string[]]$Batches,[string]$S,[string]$E,[int]$Throttle,[switch]$Strict)
  $jobs = @()

  foreach($ids in $Batches){
    # Throttle：到上限就等任一完成，再收取已完成結果
    while( ($jobs | Where-Object { $_.State -eq 'Running' }).Count -ge $Throttle ){
      $running = $jobs | Where-Object { $_.State -eq 'Running' }
      if($running){ Wait-Job -Job $running -Any -Timeout 1 | Out-Null }
      $done = $jobs | Where-Object { $_.State -in 'Completed','Failed' }
      if($done){ Receive-Job -Job $done -Keep -ErrorAction SilentlyContinue | Out-Null }
      Start-Sleep -Milliseconds 100
    }

    $args = @('-Start',$S,'-End',$E,'-IDs',$ids,'-Group','A')
    if($Strict){ $args += '-FailOnError' }

    $jobs += Start-ThreadJob -ScriptBlock {
      param($a)
      pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Run-DateID-Extras.ps1 @a
      [int]$LASTEXITCODE
    } -ArgumentList (,$args)
  }

  # 收尾：等全部完成並安全接收
  while( ($jobs | Where-Object { $_.State -in 'Running','NotStarted' }).Count -gt 0 ){
    $running = $jobs | Where-Object { $_.State -eq 'Running' }
    if($running){ Wait-Job -Job $running -Any -Timeout 1 | Out-Null }
    $done = $jobs | Where-Object { $_.State -in 'Completed','Failed' }
    if($done){ Receive-Job -Job $done -Keep -ErrorAction SilentlyContinue | Out-Null }
  }

  # 統計 rc 並清理
  $rcs = @()
  foreach($j in $jobs){
    try { $rcs += [int](Receive-Job -Job $j -Keep | Select-Object -Last 1) } catch { $rcs += 1 }
    Remove-Job $j -Force -ErrorAction SilentlyContinue
  }
  return $rcs
}

# 視窗迴圈（End 不含）
for($d=[datetime]$Start; $d -lt [datetime]$End; $d=$d.AddDays($WindowDays)){
  $s = $d.ToString('yyyy-MM-dd')
  $E = ([datetime]$s).AddDays($WindowDays); if($E -gt [datetime]$End){ $E = [datetime]$End }
  $e = $E.ToString('yyyy-MM-dd')
  "=== $s → $e ==="
  $rcs = Invoke-Batches -Batches $batches -S $s -E $e -Throttle $Workers -Strict:$Strict
  if($Strict -and ($rcs | Where-Object { $_ -ne 0 }).Count -gt 0){
    throw "Strict stop: some batches failed (rc != 0)"
  }
}
"✅ MaxRate (ThreadJob v0.2) done."

