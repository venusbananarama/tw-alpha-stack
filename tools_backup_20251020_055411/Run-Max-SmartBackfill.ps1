param(
  [datetime]$Start = "2004-01-01", [string]$Group = "A", [Nullable[int]]$MaxIDs = 20,
  [int]$WindowDays = 14, [int]$M = 8, [int]$JumpYears = 3,
  [switch]$Strict, [int]$ThrottleRPM = 3, [string]$Tag = "S4_final"
)
$ErrorActionPreference = "Stop"
$root  = Split-Path -Parent $PSScriptRoot
$extras = Join-Path $PSScriptRoot "Run-DateID-Extras.ps1"
if(-not (Test-Path $extras)){ throw "找不到 $extras" }
Set-Location $root; $env:ALPHACITY_ALLOW="1"
if($ThrottleRPM){ $env:FINMIND_THROTTLE_RPM="$ThrottleRPM" }
function Resolve-AlphaIDs([string]$Group,[Nullable[int]]$TakeN){
  $grpFile = Join-Path (Join-Path $root "configs\groups") ("{0}.txt" -f $Group)
  $list = @(); if(Test-Path $grpFile){ $list=Get-Content $grpFile } else {
    $uni = Join-Path (Join-Path $root "configs") "investable_universe.txt"
    if(Test-Path $uni){ $list=Get-Content $uni }
  }
  $ids = $list | ForEach-Object { $_.Trim() } | Where-Object { $_ } |
         ForEach-Object { ($_ -replace "[^0-9A-Za-z_]", "") } |
         Select-Object -Unique
  if(-not $ids -or $ids.Count -eq 0){ throw "找不到 IDs（Group=$Group）" }
  if($TakeN){ $ids = $ids | Select-Object -First $TakeN }
  return ($ids -join ",")
}
$logDir = Join-Path $root "reports"; $null=New-Item -ItemType Directory -Force -Path $logDir
$log = Join-Path $logDir ("smartbackfill_{0}_{1}.log" -f $Tag,(Get-Date -Format "yyyyMMdd_HHmmss"))
function Run-Window([datetime]$s){
  $e=$s.AddDays($WindowDays); $idsCsv=Resolve-AlphaIDs -Group $Group -TakeN $MaxIDs
  ("=== {0} -> {1} === Group={2} IDs={3}" -f $s.ToString("yyyy-MM-dd"),$e.ToString("yyyy-MM-dd"),$Group,$idsCsv.Substring(0,[Math]::Min(30,$idsCsv.Length))+"...") |
    Tee-Object -FilePath $log -Append | Out-Host
  $extra=@("-Start",$s.ToString("yyyy-MM-dd"),"-End",$e.ToString("yyyy-MM-dd"),"-Group",$Group,"-IDs",$idsCsv)
  if($Strict){ $extra+="-FailOnError" }
  $out=& pwsh -NoProfile -ExecutionPolicy Bypass -File $extras @extra 2>&1
  foreach($line in $out){ if($line -match "^FAIL "){ [console]::Beep(1000,120); Write-Host $line -ForegroundColor Red }
    elseif($line -match "^DONE .*total_rows=0"){ Write-Host $line -ForegroundColor Yellow } else { Write-Host $line } }
  $out | ForEach-Object { "[{0}] {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $_ } | Add-Content -Path $log -Encoding UTF8
  $any=$false; foreach($line in $out){ if($line -like "*total_rows=*"){ $n=(($line -split "total_rows=")[1] -split "[^0-9]")[0]; if($n -and [int]$n -gt 0){ $any=$true; break } } }
  [pscustomobject]@{ start=$s; end=$e; hasRows=$any }
}
$empty=0; while($true){
  $win=Run-Window $Start; if($win.hasRows){ $empty=0 } else { $empty++ }
  if($empty -ge $M){
    ("⚠️ 連續 {0} 窗全 0 → 快轉 {1} 年：{2} → {3}" -f $M,$JumpYears,$Start.ToString("yyyy"),$Start.AddYears($JumpYears).ToString("yyyy")) |
      Tee-Object -FilePath $log -Append | Out-Host
    $Start=$Start.AddYears($JumpYears); $empty=0
  } else { $Start=$win.end }
}
