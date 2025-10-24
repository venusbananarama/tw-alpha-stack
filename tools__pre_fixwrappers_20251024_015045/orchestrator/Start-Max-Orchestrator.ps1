param(
  [int]$TotalTargetRPM = 100,
  [double]$S1Share = 1.0,
  [int]$TestMinutes = 6,
  [datetime]$S1Start = "2018-01-01",
  [datetime]$S4Start = "2004-01-01",
  [int]$S1Workers = 4,
  [int]$S1RPM = 20,
  [int]$S1MaxWorkers = 8,
  [int]$S1MaxRPM = 80,
  [int]$S4RPM = 0,              # 預設關閉 S4
  [int]$S4MaxIDs = 0,           # 預設關閉 S4
  [int]$WindowDays = 14
)
$ErrorActionPreference='Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

# 日誌與狀態
$orchLog = ".\reports\orchestrator_{0}.log" -f (Get-Date -Format "yyyyMMdd_HHmmss")
$stateFile = ".\state\orchestrator_status.json"
function W($m){ "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $m | Tee-Object -FilePath $orchLog -Append | Out-Host }
function Write-State([hashtable]$h){ if(-not (Test-Path .\state)){ New-Item -ItemType Directory -Force -Path .\state | Out-Null }; $h.ts=(Get-Date).ToString("yyyy-MM-dd HH:mm:ss"); $h | ConvertTo-Json -Depth 5 | Set-Content $stateFile -Encoding UTF8 }

# 偵測佔位（提醒、不阻擋）
$maxBody = (Get-Content .\tools\Run-FullMarket-DateID-MaxRate.ps1 -Raw -ea SilentlyContinue)
$extBody = (Get-Content .\tools\Run-DateID-Extras.ps1        -Raw -ea SilentlyContinue)
if($maxBody -match '佔位' -or $extBody -match '佔位'){ W "⚠️ 核心腳本為【佔位版】；只會印字不會打 API。" }

# 子任務 PID 管控（單實例）
$script:S1Pid = $null
$script:S4Pid = $null
function IsAlive([Nullable[int]]$pid){ if(-not $pid){ return $false } try{ Get-Process -Id $pid -ErrorAction Stop | Out-Null; $true }catch{ $false } }

function Stop-S1 { if(IsAlive $script:S1Pid){ try{ Stop-Process -Id $script:S1Pid -Force }catch{}; W "停止 S1 PID=$script:S1Pid" } $script:S1Pid=$null
  Get-CimInstance Win32_Process -Filter "Name='pwsh.exe'" | Where-Object { $_.CommandLine -match 'Run-Max-Recent|Run-FullMarket-DateID-MaxRate' } | ForEach-Object { try{ Stop-Process -Id $_.ProcessId -Force }catch{} } }

function Stop-S4 { if(IsAlive $script:S4Pid){ try{ Stop-Process -Id $script:S4Pid -Force }catch{}; W "停止 S4 PID=$script:S4Pid" } $script:S4Pid=$null
  Get-CimInstance Win32_Process -Filter "Name='pwsh.exe'" | Where-Object { $_.CommandLine -match 'Run-Max-SmartBackfill' } | ForEach-Object { try{ Stop-Process -Id $_.ProcessId -Force }catch{} } }

function Start-S1([int]$rpm,[int]$workers){
  $arg = "-NoProfile -File .\tools\Run-Max-Recent.ps1 -Start {0} -WindowDays {1} -Workers {2} -ThrottleRPM {3} -Tag S1_final" -f ($S1Start.ToString('yyyy-MM-dd')),$WindowDays,$workers,$rpm
  $p = Start-Process -FilePath (Get-Command pwsh).Source -ArgumentList $arg -WindowStyle Hidden -PassThru
  $script:S1Pid = $p.Id; W "啟動 S1：PID=$($p.Id) RPM=$rpm Workers=$workers"
}
function Start-S4([int]$rpm,[int]$maxIDs){
  if($rpm -le 0 -or $maxIDs -le 0){ W "跳過 S4：RPM=$rpm MaxIDs=$maxIDs"; $script:S4Pid=$null; return }
  $arg = "-NoProfile -File .\tools\Run-Max-SmartBackfill.ps1 -Start {0} -Group A -MaxIDs {1} -WindowDays {2} -M 8 -JumpYears 3 -ThrottleRPM {3} -Tag S4_final" -f ($S4Start.ToString('yyyy-MM-dd')),$maxIDs,$WindowDays,$rpm
  $p = Start-Process -FilePath (Get-Command pwsh).Source -ArgumentList $arg -WindowStyle Hidden -PassThru
  $script:S4Pid = $p.Id; W "啟動 S4：PID=$($p.Id) RPM=$rpm MaxIDs=$maxIDs"
}
function Ensure-S1([int]$rpm,[int]$workers){ if(-not (IsAlive $script:S1Pid)){ Start-S1 -rpm $rpm -workers $workers } }
function Ensure-S4([int]$rpm,[int]$maxIDs){ if(-not (IsAlive $script:S4Pid)){ Start-S4 -rpm $rpm -maxIDs $maxIDs } }
function Restart-S1([int]$rpm,[int]$workers){ Stop-S1; Start-S1 -rpm $rpm -workers $workers }

# 健壯 Measure-RPM（有/無時間戳都可、失敗回退 LastWriteTime）
function Measure-RPM([int]$minutes){
  $since = (Get-Date).AddMinutes(-$minutes)
  $logs  = Get-ChildItem .\reports -Filter *.log -File -ea SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 15
  $pat   = '^(?:\[(?<ts>[\d-]+\s[\d:]+)\]\s*)?(?:=== |DONE |FAIL |HTTP|429|Rate)'
  $hits=0; $r429=0
  foreach($f in $logs){
    $lw = $f.LastWriteTime
    foreach($m in (Select-String $f.FullName -Pattern $pat)){
      $tsV = $m.Matches.Groups['ts'].Value
      $ts  = if([string]::IsNullOrEmpty($tsV)){ $lw } else { try{ [datetime]$tsV }catch{ $lw } }
      if($ts -ge $since){ $hits++; if($m.Line -match '429|Too Many|Rate limit|quota'){ $r429++ } }
    }
  }
  $rpm = [math]::Round($hits / [double]$minutes, 1)
  [pscustomobject]@{ rpm=$rpm; perHour=[int]($rpm*60); r429=$r429 }
}

# 寫入初始 state
Write-State @{ phase='boot'; rpm=0; perHour=0; r429=0; S1RPM=$S1RPM; S1Workers=$S1Workers; S4RPM=$S4RPM; S4MaxIDs=$S4MaxIDs }
W "已寫入初始狀態（phase=boot，rpm=0）"

# 啟動與校正
Stop-S1; Stop-S4
$target   = $TotalTargetRPM
$targetS1 = [int]($target * $S1Share)
$targetS4 = $target - $targetS1
W ("目標合併：{0}/min（S1={1}, S4≈{2}），測試 {3} 分鐘開始" -f $target,$targetS1,$targetS4,$TestMinutes)
Ensure-S1 -rpm $S1RPM -workers $S1Workers
Ensure-S4 -rpm $S4RPM -maxIDs $S4MaxIDs

$maxErr=0.15; $stepRPM=5; $stepW=1
for($i=1; $i -le $TestMinutes; $i++){
  Start-Sleep -Seconds 60
  Ensure-S1 -rpm $S1RPM -workers $S1Workers
  Ensure-S4 -rpm $S4RPM -maxIDs $S4MaxIDs
  $m = Measure-RPM -minutes 1
  if($m.rpm -lt ($target*(1-$maxErr)) -and $m.r429 -eq 0){
    $old=$S1RPM; $S1RPM=[Math]::Min($S1RPM+$stepRPM,$S1MaxRPM)
    if($S1RPM -ne $old){ Restart-S1 -rpm $S1RPM -workers $S1Workers; W ("↑ S1 RPM {0}→{1}" -f $old,$S1RPM) }
    elseif($S1Workers -lt $S1MaxWorkers){ $S1Workers++; Restart-S1 -rpm $S1RPM -workers $S1Workers; W ("↑ S1 Workers → {0}" -f $S1Workers) }
    else{ W "S1 已達上限（RPM=$S1RPM,Workers=$S1Workers）" }
  } elseif(($m.rpm -gt ($target*(1+$maxErr))) -or $m.r429 -gt 10){
    $old=$S1RPM; $S1RPM=[Math]::Max(5,$S1RPM-$stepRPM)
    Restart-S1 -rpm $S1RPM -workers $S1Workers; W ("↓ S1 RPM {0}→{1}（429={2}）" -f $old,$S1RPM,$m.r429)
  } else {
    W ("穩定：合併≈{0}/min（≈{1}/h），429={2}" -f $m.rpm,$m.perHour,$m.r429)
  }
  Write-State @{ phase='tuning'; minute=$i; rpm=$m.rpm; perHour=$m.perHour; r429=$m.r429; S1RPM=$S1RPM; S1Workers=$S1Workers; S4RPM=$S4RPM; S4MaxIDs=$S4MaxIDs }
}

# 測試完成摘要
$final = Measure-RPM -minutes 3
W ("測試完成：近3分鐘平均 ≈ {0}/min（≈{1}/h），429={2}；S1=RPM{3}/W{4}，S4=RPM{5}/IDs{6}" -f $final.rpm,$final.perHour,$final.r429,$S1RPM,$S1Workers,$S4RPM,$S4MaxIDs)
Write-State @{ phase='run'; rpm=$final.rpm; perHour=$final.perHour; r429=$final.r429; S1RPM=$S1RPM; S1Workers=$S1Workers; S4RPM=$S4RPM; S4MaxIDs=$S4MaxIDs }

# 守護：不中斷長跑
while($true){
  Start-Sleep -Seconds 60
  Ensure-S1 -rpm $S1RPM -workers $S1Workers
  Ensure-S4 -rpm $S4RPM -maxIDs $S4MaxIDs
  $m = Measure-RPM -minutes 1
  W ("守護：合併≈{0}/min（≈{1}/h），429={2}" -f $m.rpm,$m.perHour,$m.r429)
  Write-State @{ phase='run'; rpm=$m.rpm; perHour=$m.perHour; r429=$m.r429; S1RPM=$S1RPM; S1Workers=$S1Workers; S4RPM=$S4RPM; S4MaxIDs=$S4MaxIDs }
}
