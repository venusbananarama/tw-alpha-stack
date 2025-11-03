param(
  [string]$Preflight = ".\reports\preflight_report.json",
  [string]$Map       = ".\preflight_files.json"
)
$pf  = Get-Content $Preflight -Raw | ConvertFrom-Json
$cfg = Get-Content $Map -Raw | ConvertFrom-Json
$viol = @()

function Fail([string]$msg){ $script:viol += $msg }

# 檢日曆錨點與時區
if ($pf.calendar.anchor -ne $cfg.calendar.anchor) { Fail("calendar.anchor=$($pf.calendar.anchor) != $($cfg.calendar.anchor)") }
if ($pf.tz -ne $cfg.calendar.tz)                 { Fail("tz=$($pf.tz) != $($cfg.calendar.tz)") }

# schema 與 as-of/lag
if ($cfg.fail_on_schema -and $pf.schema_failures.Count -gt 0) { Fail("schema_failures>0") }

foreach($src in @('prices','chip','dividend','per')){
  $maxd = $pf.freshness.$src.max_date
  if (-not $maxd){ Fail("$src freshness missing"); continue }
  $lag = ((Get-Date).Date - ([datetime]$maxd).Date).Days
  $lim = [int]$cfg.freshness.$src.max_lag_days
  if ($lag -gt $lim){ Fail("$src freshness lag=$lag > $lim days") }
}

$pass = ($viol.Count -eq 0)
$result = [ordered]@{
  date       = (Get-Date).ToString('s')
  pass       = $pass
  violations = $viol
}
$result | ConvertTo-Json -Depth 4 | Set-Content .\reports\preflight_assert.json -Encoding utf8 -Force
if (-not $pass){ Write-Error "Preflight Assert FAIL"; exit 1 } else { "Preflight Assert PASS" }
