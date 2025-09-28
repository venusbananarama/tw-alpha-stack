param(
  [string]$OutPath = ".\metrics\verify_summary_latest_from_csv.json",
  [int]$LandingWindowMins = 30
)
$repoRoot = Split-Path -Parent (Split-Path -Parent (Split-Path -Parent $PSCommandPath))
$metricsDir= Join-Path $repoRoot "metrics"
$datahub   = Join-Path $repoRoot "datahub"
function FindCol { param($df,[string[]]$cands) $n=($df|Select-Object -First 1).PSObject.Properties.Name; foreach($c in $cands){ if($n -contains $c){ return $c } }; foreach($c in $cands){ $hit=$n|Where-Object{ $_.ToLower() -eq $c.ToLower() }|Select-Object -First 1; if($hit){ return $hit } }; $null }
function SumCol { param($df,[string]$col) if(-not $col){return $null}; $s=0.0; foreach($r in $df){ $v=$r.$col; if($null -ne $v -and $v -ne ""){ try{$s+=[double]$v}catch{}} }; [int][Math]::Round($s,0) }
$csv = Get-ChildItem $metricsDir -Filter *.csv -File | Sort-Object LastWriteTime -Desc | Select-Object -First 1
if(-not $csv){ throw "No metrics CSV in $metricsDir" }
$df = Import-Csv $csv
$callsCol=FindCol $df @("calls","api_calls","requests","request_count","n_calls")
$errsCol =FindCol $df @("errors","error","error_count","n_errors","failures")
$rowsCol =FindCol $df @("rows","row_count","rows_written","n_rows","count_rows","records","records_written","nrecords","lines")
$calls=SumCol $df $callsCol
$errs =$(if((SumCol $df $errsCol) -ne $null){ [int](SumCol $df $errsCol) } else { 0 })
$rows =$(if((SumCol $df $rowsCol) -ne $null){ [int](SumCol $df $rowsCol) } else { 0 })
$since=$csv.LastWriteTime; $until=(Get-Date).AddMinutes($LandingWindowMins)
$land=0; if(Test-Path $datahub){ $land=(Get-ChildItem $datahub -Recurse -Filter *.parquet | Where-Object{ $_.LastWriteTime -ge $since -and $_.LastWriteTime -le $until } | Measure-Object).Count }
$summary=[pscustomobject]@{ generatedAt=(Get-Date).ToString('s'); fromCsv=$csv.FullName; results=[pscustomobject]@{ calls=[int]$calls; errors=[int]$errs; rows=[int]$rows; landings=[int]$land; pass=( ($errs -eq 0) -and ( ($rows -gt 0) -or ($land -gt 0) ) ) } }
$dir = Split-Path -Parent $OutPath; if($dir){ New-Item -Type Directory -Force $dir > $null }
Set-Content -Path $OutPath -Value ($summary | ConvertTo-Json -Depth 4) -Encoding UTF8
Write-Host "[INFO] Summary JSON written: $((Resolve-Path $OutPath).Path)"
