param(
  [string] = (Get-ChildItem .\reports\dateid_run_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 1).FullName
)
Set-Location \..
Write-Host "[MON] Log: "
if (Test-Path .\reports\preflight_report.json) {
   = Get-Content .\reports\preflight_report.json -Raw | ConvertFrom-Json
  "
[Freshness]" 
  "prices = "
  "chip   = "
  "dividend = "
  "per      = "
}
if (Test-Path ) {
   = Get-Content  -Tail 400
   = ( | Select-String -SimpleMatch "402").Length -gt 0
   = ( | Select-String -SimpleMatch "429").Length -gt 0
  if ( -or ) {
    Write-Warning "Detected provider throttle (402/429). Suggest: set --jobs 1, reduce --batch-size, add --batch-sleep >= 2~5s."
  } else {
    Write-Host "[MON] No 402/429 found in last 400 lines."
  }
}
