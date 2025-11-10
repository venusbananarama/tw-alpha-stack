param()
$ErrorActionPreference='Stop'
Set-Location C:\AI\tw-alpha-stack
$env:ALPHACITY_ALLOW='1'

function Get-LatestOkDate([string]$ds){
  $p = Join-Path ".\_state\ingest\$ds" '*.ok'
  $last = Get-ChildItem $p -ErrorAction SilentlyContinue | Select-Object -Expand Name | Sort-Object | Select-Object -Last 1
  if(!$last){ return $null }
  return [datetime]::ParseExact(($last -replace '\.ok$',''),'yyyy-MM-dd',$null)
}

# 共同最新日 → 鎖定 EXPECT_DATE_FIXED（避免預期未來）
$dates = @()
'prices','chip','dividend','per' | ForEach-Object {
  $d = Get-LatestOkDate $_
  if($d){ $dates += $d }
}
if($dates.Count){
  $common = ($dates | Sort-Object | Select-Object -First 1).ToString('yyyy-MM-dd')
  $env:EXPECT_DATE_FIXED = $common
  Write-Host ("EXPECT_DATE_FIXED = {0}" -f $common) -ForegroundColor Cyan
}

# 1) Gate（唯一入口）
.\tools\gate\Run-WFGate.ps1

# 2) Gate 總結
$gpath = ".\reports\gate_summary.json"
if(!(Test-Path $gpath)){ throw "gate_summary.json not found: $gpath" }
$g = Get-Content $gpath -Raw | ConvertFrom-Json
"== Gate Summary =="
"wf.windows     : " + (($g.wf.windows ?? @()) -join ', ')
"wf.pass_rate   : $($g.wf.pass_rate)"

# 列 FAIL（若檔案有提供）
$failItems = @()
$failItems += ($g.checks   | Where-Object { $_ }) 2>$null
$failItems += ($g.failures | Where-Object { $_ }) 2>$null
$failItems += ($g.fails    | Where-Object { $_ }) 2>$null
$failItems = $failItems | Where-Object { $_.status -match 'FAIL' -or $_.result -match 'FAIL' }
"`n-- FAIL checks --"
if($failItems -and $failItems.Count){
  $failItems | Select-Object window,name,metric,value,threshold,reason | Format-Table -AutoSize
} else {
  "no explicit FAIL items listed in gate_summary.json"
}

# 3) WF 細節（若存在）
$wfPath = @('.\reports\wf_gate.json','.\reports\wf_summary.json','.\wf_summary.json') |
          Where-Object { Test-Path $_ } | Select-Object -First 1
if($wfPath){
  "`n== WF Detail =="
  $wf = Get-Content $wfPath -Raw | ConvertFrom-Json
  $rows = @()
  if($wf.windows){ $rows += $wf.windows }
  if($wf.results){ $rows += $wf.results }
  if(-not $rows -and $wf.window){ $rows += $wf }   # 某些版本直接在根層
  if($rows){
    $rows | Select-Object `
      @{n='window';e={ $_.window }},
      @{n='status';e={ $_.status ?? $_.result }},
      @{n='pass_rate';e={ $_.pass_rate }},
      @{n='fails';e={ ($_.fails ?? $_.failures ?? $_.reasons) -join '; ' }} |
      Format-Table -AutoSize
  } else {
    "wf file loaded but contains no enumerable windows/results."
  }
} else {
  "`nwf detail file not found."
}

# 依代號六：不呼叫任何 orchestrator/* （禁 wrapper／直路徑）
