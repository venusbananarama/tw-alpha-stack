param(
  [int]$TargetRPM=100,[double]$OkBand=0.10,[int]$High429=10,
  [int]$StepRPM=5,[int]$MaxS1RPM=80,[int]$MaxS1Workers=8,[switch]$Apply
)
$ErrorActionPreference='Stop'
$root = if(-not [string]::IsNullOrEmpty($PSScriptRoot)){ Split-Path -Parent $PSScriptRoot } else { (Get-Location).Path }
Set-Location $root

function Read-Orch {  # 從 orchestrator log 近3分鐘估算 rpm/429
  $since=(Get-Date).AddMinutes(-3)
  $log = Get-ChildItem .\reports -Filter 'orchestrator_*.log' -File -ea SilentlyContinue | Sort LastWriteTime -desc | Select -First 1
  if(-not $log){ return $null }
  $pat='^\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] (=== |DONE |FAIL |429|Rate|HTTP)'
  $hits=0;$r429=0
  (Select-String $log.FullName -Pattern $pat) | ForEach-Object{
    if($_.Line -match '^\[(?<ts>[\d-]+\s[\d:]+)\]'){ $ts=[datetime]$matches['ts']; if($ts -ge $since){
      $hits++; if($_.Line -match '429|Too Many|Rate limit|quota'){ $r429++ }
    }}
  }
  if($hits -eq 0){ return $null }
  [pscustomobject]@{ rpm=[math]::Round($hits/3.0,1); perHour=[int]($hits/3.0*60); r429=$r429; ts=(Get-Date) }
}

# 1) 讀 state；若沒有，就退回 orchestrator log
$statePath = '.\state\orchestrator_status.json'
if(Test-Path $statePath){
  $s = Get-Content $statePath -Raw | ConvertFrom-Json
  $rpm=[double]$s.rpm; $perHour=[int]$s.perHour; $r429=[int]$s.r429
  $S1RPM=[int]$s.S1RPM; $S1Workers=[int]$s.S1Workers; $ts=$s.ts
}else{
  $o = Read-Orch
  if(-not $o){ throw "找不到 $statePath，且 orchestrator log 尚未產生有效樣本；請再過 1~2 分鐘重試。" }
  # 無法從 state 取得 S1 檔位，保守假設
  $rpm=$o.rpm; $perHour=$o.perHour; $r429=$o.r429; $ts=$o.ts
  $S1RPM=20; $S1Workers=4
}

# 2) 佔位核心檢查
$maxBody = (Get-Content .\tools\Run-FullMarket-DateID-MaxRate.ps1 -Raw -ea SilentlyContinue)
$extBody = (Get-Content .\tools\Run-DateID-Extras.ps1        -Raw -ea SilentlyContinue)
$warnCore = ($maxBody -match '佔位' -or $extBody -match '佔位')

# 3) 閾值與決策
$low=$TargetRPM*(1-$OkBand); $high=$TargetRPM*(1+$OkBand)
$action='keep'; $reason=''
if($warnCore){ $action='block'; $reason='核心腳本為佔位版' }
elseif($r429 -gt $High429){ $action='down'; $reason="429 偏高（$r429>$High429）" }
elseif($rpm -lt $low){ $action='up'; $reason="吞吐不足（$rpm < $([math]::Round($low,1))）" }
elseif($rpm -gt $high){ $action='down'; $reason="吞吐超標（$rpm > $([math]::Round($high,1))）" }
else{ $action='keep'; $reason="吞吐在容忍帶內（$([math]::Round(100*($rpm/$TargetRPM),1))%）" }

# 4) 建議檔位（僅動 S1）
$prop=[ordered]@{ S1RPM=$S1RPM; S1Workers=$S1Workers }
switch($action){
  'up'   { if($S1RPM -lt $MaxS1RPM){ $prop.S1RPM=[Math]::Min($S1RPM+$StepRPM,$MaxS1RPM) }
           elseif($S1Workers -lt $MaxS1Workers){ $prop.S1Workers=$S1Workers+1 } }
  'down' { $prop.S1RPM=[Math]::Max(5,$S1RPM-$StepRPM) }
}

"=== 決策報告（$ts） ==="
"合併 RPM ≈ $rpm（≈$perHour/h），429=$r429；S1=RPM $S1RPM / W $S1Workers"
"判斷：$action  │ 原因：$reason"

$restart = "pwsh -NoProfile -File .\tools\Start-Max-Orchestrator.ps1 -TotalTargetRPM $TargetRPM -S1Share 0.9 -TestMinutes 6 -S1Start 2018-01-01 -S1Workers {0} -S1RPM {1} -S1MaxWorkers $MaxS1Workers -S1MaxRPM $MaxS1RPM -S4Start 2004-01-01 -S4RPM 10 -S4MaxIDs 20 -WindowDays 14" -f $prop.S1Workers,$prop.S1RPM

switch($action){
  'keep'  { "下一步：維持現狀" }
  'block' { "下一步：替換核心腳本為正式實作，再執行：`n$restart" }
  default { "下一步：先停 → 再用建議檔位重啟`n1) pwsh -NoProfile -File .\tools\Stop-Alpha.ps1 -All`n2) $restart" }
}

if($Apply -and $action -ne 'keep' -and $action -ne 'block'){
  & pwsh -NoProfile -File .\tools\Stop-Alpha.ps1 -All
  Invoke-Expression $restart
  "✅ 已依建議重啟（自動套用）"
}
