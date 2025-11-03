param([ValidateSet("auto","orchestrator","fullmarket","smart","dateid")] [string]$Kind="auto",
      [int]$Tail=160,[switch]$NoBeep,[string]$Group="A")
$root = Split-Path -Parent $PSScriptRoot; $dir=Join-Path $root "reports"
if(-not (Test-Path $dir)){ throw "找不到 $dir" }
function Pick([string]$k){ $all=Get-ChildItem $dir -File -ea SilentlyContinue
  switch($k){
    "orchestrator"{return $all|? Name -like "orchestrator_*.log"           | sort LastWriteTime -desc | select -f 1}
    "fullmarket"  {return $all|? Name -like "fullmarket_maxrate_*.log"     | sort LastWriteTime -desc | select -f 1}
    "smart"       {return $all|? Name -like "smartbackfill_*.log"          | sort LastWriteTime -desc | select -f 1}
    "dateid"      {return $all|? Name -like ("dateid_extras_*_{0}.log" -f $Group) | sort LastWriteTime -desc | select -f 1}
    default       {$cand=@();$cand+=$all|? Name -like "orchestrator_*.log"
                           ;$cand+=$all|? Name -like "fullmarket_maxrate_*.log"
                           ;$cand+=$all|? Name -like "smartbackfill_*.log"
                           ;$cand+=$all|? Name -like ("dateid_extras_*_{0}.log" -f $Group)
                   return $cand|sort LastWriteTime -desc|select -f 1} } }
$f=Pick $Kind; if(-not $f){ "No logs found in $dir"; exit 0 }
"Tail: $($f.FullName)"; Get-Content $f.FullName -Wait -Tail $Tail | %{
  if($_ -match "^\[.*\]\sFAIL "){ if(-not $NoBeep){ [console]::Beep(1000,120) }; Write-Host $_ -ForegroundColor Red }
  elseif($_ -match "total_rows=0"){ Write-Host $_ -ForegroundColor Yellow }
  else { Write-Host $_ }
}
