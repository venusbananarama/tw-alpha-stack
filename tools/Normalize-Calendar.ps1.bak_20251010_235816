param([string]$Path = ".\cal\trading_days.csv")
$rows = Import-Csv $Path
$clean = $rows | % { $d=$_.date.Trim(); if ($d -match '^\d{4}-\d{2}-\d{2}$') { [pscustomobject]@{date=$d} } }
$clean | Sort-Object date -Unique | Export-Csv $Path -NoTypeInformation -Encoding UTF8
Write-Host "[OK] normalized: $Path"
