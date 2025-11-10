param(
  [string[]] $Datasets = @('prices','chip','per','dividend'),
  [string]   $StateRoot = ".\_state\mainline",
  [string]   $LedgerPath = ".\metrics\ingest_ledger.jsonl",
  [string]   $From,                  # yyyy-MM-dd（可省略，預設用各自最早 .ok 或基準日）
  [string]   $To,                    # yyyy-MM-dd（可省略，預設用今天）
  [ValidateSet("live","backfill")]
  [string]   $RunTypeFilter,
  [switch]   $ShowHoles,             # 顯示缺洞區間
  [int]      $Recent = 5,            # 每個 dataset 顯示 ledger 最近幾筆
  [string]   $OutputCsv              # 匯出 Summary 成 CSV（可省略）
)

Set-StrictMode -Version Latest

# 基準日（無 .ok 時回退）
$BaseStart = @{
  prices   = '2015-04-18'
  chip     = '2015-04-04'
  per      = '2015-04-15'
  dividend = '2004-01-01'
}

function Parse-Date([string]$s){
  if([string]::IsNullOrWhiteSpace($s)){ return $null }
  try {
    return [datetime]::ParseExact($s,'yyyy-MM-dd',
      [System.Globalization.CultureInfo]::InvariantCulture,
      [System.Globalization.DateTimeStyles]::None)
  } catch { return $null }
}

$fromDt = Parse-Date $From
$toDt   = Parse-Date $To
if(-not $toDt){ $toDt = (Get-Date).Date }  # 到今天（含），下方做半開處理

function Get-OkDates([string]$dir){
  if(!(Test-Path $dir)){ return @() }
  $names = Get-ChildItem $dir -Filter *.ok -Recurse -File -ErrorAction SilentlyContinue |
           Where-Object { $_.BaseName -match '^\d{4}-\d{2}-\d{2}$' } |
           Select-Object -ExpandProperty BaseName
  $dates = New-Object System.Collections.Generic.List[datetime]
  foreach($n in $names){
    $d = Parse-Date $n
    if($d){ [void]$dates.Add($d) }
  }
  return ($dates | Sort-Object)
}

function Get-ContiguousRanges([datetime[]]$missing){
  if(-not $missing -or $missing.Count -eq 0){ return @() }
  $ranges = @()
  $start = $missing[0]; $prev = $missing[0]
  for($i=1;$i -lt $missing.Count;$i++){
    $cur = $missing[$i]
    if($cur -ne $prev.AddDays(1)){
      $ranges += [pscustomobject]@{
        Start = $start.ToString('yyyy-MM-dd')
        End   = $prev.ToString('yyyy-MM-dd')
        Days  = (New-TimeSpan -Start $start -End $prev).Days + 1
      }
      $start = $cur
    }
    $prev = $cur
  }
  $ranges += [pscustomobject]@{
    Start = $start.ToString('yyyy-MM-dd')
    End   = $prev.ToString('yyyy-MM-dd')
    Days  = (New-TimeSpan -Start $start -End $prev).Days + 1
  }
  return $ranges
}

$summary = @()
$holesAll = @()

foreach($ds in $Datasets){
  $dir = Join-Path $StateRoot $ds
  $ok = Get-OkDates $dir
  $okCount = $ok.Count
  $okMin = if($okCount){ $ok[0] } else { Parse-Date $BaseStart[$ds] }
  $okMax = if($okCount){ $ok[-1] } else { $null }

  $fromThis = if($fromDt){ $fromDt } else { $okMin }
  $toThis   = $toDt  # 統一使用

  # 建立日期集合（半開：from .. toThis 含 from、不含 toThis+1；實際列舉到 toThis）
  $days = @()
  $d = $fromThis.Date
  while($d -le $toThis.Date){
    $days += $d
    $d = $d.AddDays(1)
  }

  $okSet = [System.Collections.Generic.HashSet[datetime]]::new()
  foreach($d2 in $ok){ [void]$okSet.Add($d2.Date) }

  $missing = @()
  foreach($d3 in $days){ if(-not $okSet.Contains($d3)){ $missing += $d3 } }

  $ranges = @()
  if($ShowHoles -and $missing.Count -gt 0){
    $ranges = Get-ContiguousRanges($missing)
    foreach($r in $ranges){
      $holesAll += [pscustomobject]@{
        Dataset = $ds; Start=$r.Start; End=$r.End; Days=$r.Days
      }
    }
  }

  $latestOk = if($okMax){ $okMax.ToString('yyyy-MM-dd') } else { $null }
  $nextStart = if($okMax){ $okMax.AddDays(1).ToString('yyyy-MM-dd') } else { $BaseStart[$ds] }

  $covered = $okSet.Count
  $periodDays = $days.Count
  $pct = if($periodDays -gt 0){ [math]::Round(100.0 * $covered / $periodDays, 2) } else { 0 }

  $summary += [pscustomobject]@{
    Dataset   = $ds
    From      = $fromThis.ToString('yyyy-MM-dd')
    To        = $toThis.ToString('yyyy-MM-dd')
    PeriodDays= $periodDays
    OkDays    = $covered
    Coverage  = ("{0}%" -f $pct)
    OkCount   = $okCount
    LatestOk  = $latestOk
    NextStart = $nextStart
  }
}

# 顯示 Summary
Write-Host "`n=== Mainline Coverage Summary ===" -ForegroundColor Cyan
$summary | Sort-Object Dataset | Format-Table -Auto

if($ShowHoles){
  Write-Host "`n=== Missing Ranges (Holes) ===" -ForegroundColor Yellow
  if($holesAll.Count){ $holesAll | Sort-Object Dataset, Start | Format-Table -Auto }
  else{ Write-Host "No holes in selected interval." -ForegroundColor Green }
}

# 讀取 ledger 做 run_type 統計與最近紀錄
if(Test-Path $LedgerPath){
  $rows = @()
  Select-String -Path $LedgerPath -SimpleMatch '"dataset":"' -ErrorAction SilentlyContinue |
    ForEach-Object{
      try{ $_.Line | ConvertFrom-Json }catch{}
    } | ForEach-Object{
      if($_ -and ($Datasets -contains $_.dataset)){
        if([string]::IsNullOrWhiteSpace($RunTypeFilter) -or $_.run_type -eq $RunTypeFilter){ $rows += $_ }
      }
    }

  if($rows.Count){
    Write-Host "`n=== Ledger Counts by dataset x run_type ===" -ForegroundColor Cyan
    $rows | Group-Object dataset, run_type | ForEach-Object {
      [pscustomobject]@{
        Dataset = $_.Group[0].dataset
        RunType = if([string]::IsNullOrWhiteSpace($_.Group[0].run_type)){'(null)'} else {$_.Group[0].run_type}
        Count   = $_.Count
      }
    } | Sort-Object Dataset, RunType | Format-Table -Auto

    Write-Host "`n=== Recent Ledger Entries per dataset (Top $Recent) ===" -ForegroundColor Cyan
    foreach($ds in $Datasets){
      $last = $rows | Where-Object { $_.dataset -eq $ds } |
              Sort-Object { try{ [datetime]$_.date }catch{ Get-Date '1900-01-01' } } |
              Select-Object -Last $Recent
      if($last){
        Write-Host ("-- {0} --" -f $ds) -ForegroundColor DarkCyan
        $last | ForEach-Object {
          [pscustomobject]@{
            date     = $_.date
            run_type = if([string]::IsNullOrWhiteSpace($_.run_type)){'(null)'} else {$_.run_type}
            status   = $_.status
            note     = $_.note
          }
        } | Format-Table -Auto
      }
    }
  } else {
    Write-Host "`n(no ledger rows matched filters)" -ForegroundColor DarkGray
  }
} else {
  Write-Host "`nLedger file not found: $LedgerPath" -ForegroundColor DarkGray
}

# 匯出 CSV（若指定）
if($OutputCsv){
  try{
    $summary | Export-Csv -NoTypeInformation -Encoding UTF8 $OutputCsv
    Write-Host ("`nSaved summary CSV -> {0}" -f (Resolve-Path $OutputCsv).Path) -ForegroundColor Green
  }catch{
    Write-Host ("`nFailed to save CSV: {0}" -f $_.Exception.Message) -ForegroundColor Red
  }
}
