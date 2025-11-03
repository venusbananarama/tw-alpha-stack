Set-StrictMode -Version Latest
$ErrorActionPreference='Stop'

function Get-CheckpointPath([string]$Dataset,[datetime]$Date,[string]$Root='datahub\_state\ingest'){
  $d = $Date.ToString('yyyy-MM-dd'); Join-Path $Root (Join-Path $Dataset "$d.ok")
}
function Test-Checkpoint([string]$Dataset,[datetime]$Date,[string]$Root='datahub\_state\ingest'){
  Test-Path (Get-CheckpointPath -Dataset $Dataset -Date $Date -Root $Root)
}
function New-Checkpoint([string]$Dataset,[datetime]$Date,[string]$Root='datahub\_state\ingest'){
  $p = Get-CheckpointPath -Dataset $Dataset -Date $Date -Root $Root
  $dir = Split-Path -Parent $p; if(!(Test-Path $dir)){ New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  "ok $(Get-Date -Format o)" | Set-Content -LiteralPath $p -Encoding UTF8
  $p
}
function Add-IngestLedger([string]$Dataset,[datetime]$Date,[int]$Symbols,[int]$Rows,[double]$Qps,[int]$Exit=0){
  $o=[pscustomobject]@{ ts=(Get-Date).ToString('o'); dataset=$Dataset; date=$Date.ToString('yyyy-MM-dd');
    symbols=$Symbols; rows=$Rows; qps=$Qps; exit_code=$Exit }
  $lf='metrics\ingest_ledger.jsonl'; $dir=Split-Path -Parent $lf; if(!(Test-Path $dir)){ New-Item -ItemType Directory -Force -Path $dir | Out-Null }
  ($o | ConvertTo-Json -Compress) + "`n" | Add-Content -LiteralPath $lf -Encoding UTF8
}
Export-ModuleMember -Function Get-CheckpointPath,Test-Checkpoint,New-Checkpoint,Add-IngestLedger
