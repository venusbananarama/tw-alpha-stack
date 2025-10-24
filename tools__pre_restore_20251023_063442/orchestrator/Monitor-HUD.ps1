param([int]$Interval=20,[int]$TailLines=8)
$ErrorActionPreference='SilentlyContinue'
[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding($false)
$OutputEncoding = [Console]::OutputEncoding

function Get-Procs {
  Get-CimInstance Win32_Process -Filter "name='pwsh.exe'" |
    ?{ $_.CommandLine -match 'Run-Max-Recent\.ps1|Run-FullMarket-DateID-?MaxRate\.ps1|Run-WFGate\.ps1|wf_runner\.py' } |
    Select ProcessId,@{n='Cmd';e={$_.CommandLine}}
}
function Show-Checkpoint {
  $ck='.\\state\\dateid_checkpoint.json'
  if(Test-Path $ck){ try{ $j=Get-Content $ck -Raw|ConvertFrom-Json; "Checkpoint: last_window=$($j.latest_window) last_processed=$($j.last_processed_date)" }catch{"Checkpoint: invalid json"} } else {"Checkpoint: not found"}
}
function Show-Preflight {
  $pf='.\\reports\\preflight_report.json'
  if(Test-Path $pf){ try{ $j=Get-Content $pf -Raw|ConvertFrom-Json; "Preflight: prices=$($j.freshness.prices.max_date) chip=$($j.freshness.chip.max_date) per=$($j.freshness.per.max_date) dividend=$($j.freshness.dividend.max_date)" }catch{"Preflight: invalid json"} } else {"Preflight: no report"}
}
function Show-Gate {
  $g='.\\reports\\gate_summary.json'
  if(Test-Path $g){
    try{
      $raw=Get-Content $g -Raw|ConvertFrom-Json
      if($raw -is [System.Array]){ "Gate: runs={0}" -f $raw.Count }
      else{ "Gate: overall={0} wf.pass_rate={1}" -f $raw.overall,$raw.wf.pass_rate }
    }catch{"Gate: invalid json"}
  } else {"Gate: no report"}
}
function Tail-Latest([int]$TailLines=8){
  $lf=Get-ChildItem .\reports -File -Filter '*.log' -ErrorAction SilentlyContinue|Sort LastWriteTime -Desc|Select -First 1
  if($lf){ "=== Tail: $($lf.Name) ==="; Get-Content $lf.FullName -Tail $TailLines } else {"No log files in .\reports"}
}
while($true){
  Clear-Host
  Write-Host ("AlphaCity HUD  —  {0}" -f (Get-Date)) -ForegroundColor Cyan
  "Processes:"; Get-Procs|Format-Table -AutoSize
  (Show-Checkpoint)
  (Show-Preflight)
  (Show-Gate)
  ""; Tail-Latest -TailLines $TailLines
  "`n(Press Ctrl+C to exit) — Refresh every $Interval sec"
  Start-Sleep -Seconds $Interval
}
