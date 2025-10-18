param(
  [datetime]$Start = "2018-01-01",
  [int]$WindowDays = 14,
  [int]$Workers = 4,
  [int]$MinRPM = 4,
  [int]$MaxRPM = 30,
  [int]$Step = 2,               # 每窗調整幅度（±2/min）
  [int]$InitRPM = 8,
  [string]$Group = "ALL",
  [string]$Tag = "S1_all"
)
$ErrorActionPreference='Stop'
Set-Location (Split-Path -Parent $PSScriptRoot)

# 檢查群組 ALL 是否存在且非空
$all = ".\configs\groups\$Group.txt"
if(!(Test-Path $all)){ throw "找不到 $all；請先建立全市場清單。" }
$cnt = (Get-Content $all | ? { $_.Trim() } | Measure-Object).Count
if($cnt -lt 10){ Write-Warning "$Group.txt 只有 $cnt 行，吞吐會很低。"; Start-Sleep 1 }

# 過夜調速：每窗跑完看 429 再微調
$rpm = [int]$InitRPM
$d = [datetime]$Start
$today = [datetime]::Today

while($d -lt $today){
  $env:FINMIND_THROTTLE_RPM = "$rpm"
  pwsh -NoProfile -File .\tools\Run-Max-Recent.ps1 `
    -Start $d.ToString('yyyy-MM-dd') -WindowDays $WindowDays `
    -Workers $Workers -ThrottleRPM $rpm -Tag $Tag -Group $Group

  # 只看最新 S1 fullmarket 日誌尾端（這一窗）
  $lf = Get-ChildItem .\reports -Filter ("fullmarket_maxrate_*_{0}.log" -f $Tag) -File |
        Sort LastWriteTime -desc | Select -First 1
  $has429 = $false
  if($lf){
    $has429 = (Get-Content $lf.FullName -Tail 400 | Select-String '429|Too Many|Rate limit') -ne $null
  }

  # 自動微調：有 429 → 降；無 429 → 升
  if($has429){
    $rpm = [math]::Max($MinRPM, $rpm - $Step)
  } else {
    $rpm = [math]::Min($MaxRPM, $rpm + $Step)
  }

  # 下一窗
  $d = $d.AddDays($WindowDays)
}
