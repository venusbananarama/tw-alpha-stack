param(
  [ValidateSet("auto","fullmarket","dateid","smart")]
  [string]$Kind="auto",
  [int]$Tail=120,
  [switch]$NoBeep,
  [string]$Group="A"
)
$root = Split-Path -Parent $PSScriptRoot
$dir  = Join-Path $root "reports"
if(-not (Test-Path $dir)){ throw "找不到 $dir" }

function Pick-Latest([string]$kind){
  $all = Get-ChildItem $dir -File -ErrorAction SilentlyContinue
  switch($kind){
    "fullmarket" { return $all | Where-Object Name -like "fullmarket_maxrate_*.log" | Sort-Object LastWriteTime -Descending | Select-Object -First 1 }
    "smart"      { return $all | Where-Object Name -like "smartbackfill_*.log"      | Sort-Object LastWriteTime -Descending | Select-Object -First 1 }
    "dateid"     { return $all | Where-Object Name -like ("dateid_extras_*_{0}.log" -f $Group) | Sort-Object LastWriteTime -Descending | Select-Object -First 1 }
    default {
      $cand = @()
      $cand += $all | Where-Object Name -like "fullmarket_maxrate_*.log"
      $cand += $all | Where-Object Name -like "smartbackfill_*.log"
      $cand += $all | Where-Object Name -like ("dateid_extras_*_{0}.log" -f $Group)
      return $cand | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    }
  }
}
$f = Pick-Latest -kind $Kind
if(-not $f){ Write-Host "No logs found in $dir" -ForegroundColor Yellow; exit 0 }
Write-Host ("Tail: {0}" -f $f.FullName) -ForegroundColor Cyan

Get-Content $f.FullName -Wait -Tail $Tail | ForEach-Object {
  $line = $_
  if($line -match "^FAIL "){
    if(-not $NoBeep){ [console]::Beep(1000,120) }
    Write-Host $line -ForegroundColor Red
  } elseif($line -match "^DONE .*total_rows=0"){
    Write-Host $line -ForegroundColor Yellow
  } else {
    Write-Host $line
  }
}
