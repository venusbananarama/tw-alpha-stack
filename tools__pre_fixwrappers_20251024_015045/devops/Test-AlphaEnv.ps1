# 代號六：Test-AlphaEnv.ps1（PSScriptRoot 回退、-join 修正）
$ErrorActionPreference = "Stop"
$base = if(-not [string]::IsNullOrEmpty($PSScriptRoot)){ Split-Path -Parent $PSScriptRoot } else { (Get-Location).Path }
Set-Location $base
function Mark([bool]$ok,[string]$msg){ if($ok){ Write-Host "OK   $msg" -ForegroundColor Green } else { Write-Host "WARN $msg" -ForegroundColor Yellow } }
function Fail([string]$msg){ Write-Host "FAIL $msg" -ForegroundColor Red }

$need = @('tools\Run-FullMarket-DateID-MaxRate.ps1','tools\Run-DateID-Extras.ps1')
$miss = $need | Where-Object { -not (Test-Path (Join-Path $base $_)) }
if($miss){ $miss | ForEach-Object { Fail "缺檔: $_" } } else { Mark $true "核心腳本存在" }

@('reports','state') | ForEach-Object {
  if(Test-Path (Join-Path $base $_)){ Mark $true "資料夾存在: $_" } else { Fail "缺資料夾: $_" }
}

$grp='configs\groups\A.txt'; $uni='configs\investable_universe.txt'
$ids=@()
if(Test-Path (Join-Path $base $grp)){ $ids = Get-Content (Join-Path $base $grp) | Where-Object { $_.Trim() } }
elseif(Test-Path (Join-Path $base $uni)){ $ids = Get-Content (Join-Path $base $uni) | Where-Object { $_.Trim() } }
if($ids.Count -gt 0){ $sample = ($ids | Select-Object -First 5) -join ','; Mark $true "IDs 可用（樣本）：$sample" } else { Fail "IDs 無法解析（檢查 $grp / $uni）" }

$toolsDir = Join-Path $base 'tools'
$has = Get-ChildItem $toolsDir -ea SilentlyContinue | Where-Object Name -like 'Run-FullMarket-DateID-MaxRate.ps1'
if(-not $has){ Fail "找不到 Run-FullMarket-DateID-MaxRate.ps1（注意連字號）" } else { Mark $true "MaxRate 檔名正確" }

Mark $true ("ALPHACITY_ALLOW=" + $env:ALPHACITY_ALLOW)
Mark $true ("FINMIND_THROTTLE_RPM=" + $env:FINMIND_THROTTLE_RPM)

$p = Get-CimInstance Win32_Process -Filter "Name='pwsh.exe'" |
     Where-Object { $_.CommandLine -match 'Run-Max-Recent|Run-Max-SmartBackfill|Run-FullMarket-DateID-MaxRate|Run-DateID-Extras' }
if($p){ $list = $p | Select-Object ProcessId, CreationDate, @{n='Cmd';e={$_.CommandLine}} | Out-String; Mark $true ("偵測到進程：`n" + $list) }
else  { Mark $false "目前沒有 S1/S4 在跑（可用 Start-Max-Orchestrator 啟動）" }
