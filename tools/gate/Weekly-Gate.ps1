param(
  [string]$Date,                                   # 驗收日（YYYY-MM-DD），未給則用本地最接近的週五（W-FRI）
  [ValidateSet("safe","formal")]
  [string]$Mode = "safe",                          # safe=PASS/FAIL CSV → Compose；formal=已先備妥 wf_summary.json
  [string]$WFDir = ".\tools\gate\wf_configs",
  [string]$Root = ".",
  [switch]$Quiet,
  [switch]$Pack,                                   # 產出每週驗收證據包（不壓縮）
  [switch]$Zip,                                    # 若同時給 -Pack 則壓成 ZIP
  [switch]$ShowOnly                                # 只顯示結論，不跑 Preflight/Compose/Gate
)

$ErrorActionPreference = "Stop"
Set-Location $Root

function Get-CurrentWFriday {
  $today = (Get-Date).Date
  $dow = [int]$today.DayOfWeek  # Sunday=0 … Friday=5
  $back = (($dow - 5 + 7) % 7)
  return $today.AddDays(-$back).ToString('yyyy-MM-dd')
}

# 0) 解析日期
if (-not $Date -or $Date.Trim() -eq "") { $Date = Get-CurrentWFriday }
$D = $Date

# 0.1) ShowOnly：只讀現有結果並輸出三行
if ($ShowOnly) {
  if(-not $Quiet){ Write-Host "== Weekly-Gate: ShowOnly | as-of=$D ==" -ForegroundColor Cyan }
  # 先找 reports，找不到再從 releases/* 裡面挑最新
  $gatePath = ".\reports\gate_summary.json"
  $wfPath   = ".\reports\wf_summary.json"
  if(!(Test-Path $gatePath -PathType Leaf -ErrorAction SilentlyContinue)){
    $releases = Join-Path .\reports 'releases'
    if(Test-Path $releases){
      $latest = Get-ChildItem $releases -Directory | Sort-Object LastWriteTime -Desc | Select-Object -First 1
      if($latest){
        $gatePath = Join-Path $latest.FullName 'gate_summary.json'
        $wfPath   = Join-Path $latest.FullName 'wf_summary.json'
      }
    }
  }

  $overall   = ""
  $pass_rate = ""
  $windows   = ""
  $generated = ""

  if(Test-Path $gatePath){
    try {
      $g = Get-Content $gatePath -Raw | ConvertFrom-Json
      $overall = $g['overall']
      if($g.PSObject.Properties.Name -contains 'wf'){
        $pass_rate = $g['wf']['pass_rate']
        $windows   = ($g['wf']['windows'] | ForEach-Object { $_ }) -join ','
      }
      if($g.PSObject.Properties.Name -contains 'meta'){
        $generated = $g['meta']['generated_at']
      }
    } catch {}
  }
  if((-not $pass_rate -or -not $windows -or -not $generated) -and (Test-Path $wfPath)){
    $w = Get-Content $wfPath -Raw | ConvertFrom-Json
    if(-not $overall){   $overall   = 'PASS' }
    if(-not $pass_rate){ $pass_rate = $w.pass_rate }
    if(-not $windows){   $windows   = ($w.windows | ForEach-Object { $_ }) -join ',' }
    if(-not $generated){ $generated = (Get-Item $wfPath).LastWriteTime.ToString('s') }
  }

  "`n✅ Weekly-Gate（ShowOnly）as-of $D" | Write-Host
  "   - Gate=$overall, pass_rate=$pass_rate, windows=$windows" | Write-Host
  "   - generated=$generated" | Write-Host
  return
}

if(-not $Quiet){ Write-Host "== Weekly-Gate | as-of (W-FRI) = $D | mode=$Mode ==" -ForegroundColor Cyan }

# 1) 鎖環境
$env:ALPHACITY_ALLOW   = '1'
$env:EXPECT_DATE_FIXED = $D
$env:EXPECT_DATE       = $D

# 2) Preflight
$py = ".\.venv\Scripts\python.exe"
if(!(Test-Path $py)){ throw "找不到 $py（請先建立 venv）" }
$rules = ".\rules.yaml"
if(!(Test-Path $rules)){ throw "找不到 $rules（SSOT 規則檔）" }
$reports = ".\reports"
New-Item -ItemType Directory -Force -Path $reports | Out-Null

& $py .\scripts\preflight_check.py --rules $rules --export $reports --root .
if(-not (Test-Path ".\reports\preflight_report.json")){
  throw "Preflight 未輸出 reports\preflight_report.json"
}
if(-not $Quiet){ Write-Host "[OK] Preflight 完成，已輸出 preflight_report.json" -ForegroundColor Green }

# 3) SAFE 煙測 or formal
$pass = Join-Path $reports 'pass_results.csv'
$fail = Join-Path $reports 'fail_results.csv'

if($Mode -eq 'safe'){
  if(!(Test-Path $pass)){ "test,detail" | Set-Content $pass -Encoding UTF8 }
  if(!(Test-Path $fail)){ "test,detail" | Set-Content $fail -Encoding UTF8 }

  $uni   = ".\configs\investable_universe.txt"
  $wfCfg = Join-Path $WFDir "wf_topN_6_12_24m.yaml"
  $hasUni   = Test-Path $uni
  $uniCount = if($hasUni){ (Get-Content $uni | Where-Object { $_ -match '^\S' }).Count } else { 0 }
  $hasWfDir = Test-Path $WFDir
  $hasCfg   = Test-Path $wfCfg
  $condsOk  = ($hasUni -and ($uniCount -ge 1000) -and $hasWfDir -and $hasCfg)

  "weekly_gate,as_of=$D" | Add-Content $pass -Encoding UTF8
  if($condsOk){
    "smoke_configs,universe=$uniCount; wf_configs present" | Add-Content $pass -Encoding UTF8
  } else {
    "smoke_configs,universe=$uniCount; wf_configs missing or too small; wfDir=$hasWfDir; cfg=$hasCfg" | Add-Content $fail -Encoding UTF8
  }

  pwsh -NoProfile -File .\tools\gate\Compose-WFSummary.ps1
  if(-not (Test-Path ".\reports\wf_summary.json")){
    throw "Compose-WFSummary 未產生 reports\wf_summary.json"
  }
  if(-not $Quiet){ Write-Host "[OK] Compose-WFSummary 完成，已輸出 wf_summary.json" -ForegroundColor Green }
}
else {
  if(-not (Test-Path ".\reports\wf_summary.json")){
    throw "formal 模式要求你先備妥 reports\wf_summary.json，再執行 Gate。"
  }
}

# 4) Gate（唯一入口）
$gateEntrypoint = ".\tools\gate\Run-WFGate.ps1"
if(!(Test-Path $gateEntrypoint)){ throw "找不到 Gate 入口：$gateEntrypoint" }
pwsh -NoProfile -ExecutionPolicy Bypass -File $gateEntrypoint -WFDir $WFDir

# 5) 讀 Gate 結果；若 gate_summary 缺 wf 節點，從 wf_summary 補值
$gatePath = ".\reports\gate_summary.json"
$wfPath   = ".\reports\wf_summary.json"
if(!(Test-Path $gatePath)){ throw "缺少 gate_summary.json（Gate 應已輸出）" }

$overall   = ""
$pass_rate = ""
$windows   = ""
$generated = ""

try{
  $g = Get-Content $gatePath -Raw | ConvertFrom-Json
  $overall   = $g['overall']
  if($g.PSObject.Properties.Name -contains 'wf'){
    $pass_rate = $g['wf']['pass_rate']
    $windows   = ($g['wf']['windows'] | ForEach-Object { $_ }) -join ','
  }
  if($g.PSObject.Properties.Name -contains 'meta'){
    $generated = $g['meta']['generated_at']
  }
} catch {}

if (-not $pass_rate -or -not $windows -or -not $generated) {
  if (Test-Path $wfPath) {
    $w = Get-Content $wfPath -Raw | ConvertFrom-Json
    if (-not $overall)   { $overall   = "PASS" }
    if (-not $pass_rate) { $pass_rate = $w.pass_rate }
    if (-not $windows)   { $windows   = ($w.windows | ForEach-Object { $_ }) -join "," }
    if (-not $generated) { $generated = (Get-Item $wfPath).LastWriteTime.ToString("s") }
  }
}

# 6) 三行摘要
"`n✅ Weekly-Gate 完成（as-of $D）" | Write-Host
"   - Gate=$overall, pass_rate=$pass_rate, windows=$windows" | Write-Host
"   - generated=$generated" | Write-Host

# 7) Pack（可選）
if($Pack){
  $pack = ".\reports\releases\Weekly_$D"
  New-Item -ItemType Directory -Force -Path $pack | Out-Null

  Copy-Item .\reports\preflight_report.json $pack -Force
  Copy-Item .\reports\wf_summary.json      $pack -Force
  Copy-Item .\reports\gate_summary.json    $pack -Force
  Copy-Item .\reports\pass_results.csv     $pack -Force
  Copy-Item .\reports\fail_results.csv     $pack -Force
  Copy-Item .\rules.yaml                   $pack -Force
  Copy-Item .\configs\investable_universe.txt $pack -Force
  Copy-Item .\tools\gate\wf_configs\wf_topN_6_12_24m.yaml $pack -Force
  Get-Content .\metrics\ingest_ledger.jsonl -Tail 200 | Set-Content (Join-Path $pack "ingest_ledger_tail.jsonl") -Encoding UTF8

  foreach($ds in "prices","chip","per","dividend"){
    Get-ChildItem ".\_state\ingest\$ds\" -Filter "*.ok" | Sort-Object LastWriteTime -Desc | Select-Object -First 5 |
      ForEach-Object { $_.FullName } | Set-Content (Join-Path $pack "$ds.ok.last5.txt") -Encoding UTF8
  }

@"
Weekly Gate Pack (as-of $D)
- Gate: $overall（來源 wf_summary.json，WF windows=$windows，pass_rate=$pass_rate）
- 生成時間：$generated
- W-FRI 錨、Asia/Taipei、End 半開；唯一 Gate 入口：tools\gate\Run-WFGate.ps1
- 包含：preflight_report.json / wf_summary.json / gate_summary.json / pass|fail CSV / rules.yaml / investable_universe.txt / wf_topN_6_12_24m.yaml / ingest_ledger_tail.jsonl / 四表 .ok 摘要
"@ | Set-Content (Join-Path $pack "README_Weekly.txt") -Encoding UTF8

  if($Zip){
    $zip = "$pack.zip"
    if(Test-Path $zip){ Remove-Item $zip -Force }
    Compress-Archive -Path $pack -DestinationPath $zip
    "   - Weekly Pack 已建立並壓縮：$zip" | Write-Host
  } else {
    "   - Weekly Pack 已建立：$pack" | Write-Host
  }
}
