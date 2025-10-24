$tracked = git ls-files -z | ForEach-Object { $_ -split "`0" } | Where-Object { $_ }
$tooBig = foreach ($p in $tracked) { $f = Get-Item $p -ErrorAction SilentlyContinue; if ($f -and $f.Length -ge 100MB) { $f } }
if ($tooBig) { $tooBig | ForEach-Object { Write-Error ("Too big: {0} {1}MB" -f $_.FullName, [math]::Round($_.Length/1MB,2)) }; exit 1 }
New-Item -Force -ItemType Directory -Path reports | Out-Null
"OK $(Get-Date -Format s)" | Set-Content reports/snapshot.txt -Encoding utf8
Get-ChildItem -Recurse -File | Select-Object FullName, Length |
  Sort-Object Length -Descending | ConvertTo-Json -Depth 3 | Set-Content reports/gate_summary.json -Encoding utf8
Write-Host "Smoke OK"
