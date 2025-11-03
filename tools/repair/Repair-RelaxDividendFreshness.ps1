param([int]$TDLag = 120)
$ErrorActionPreference = "Stop"
if (-not (Get-Command ConvertFrom-Yaml -ErrorAction SilentlyContinue)) { throw "缺少 powershell-yaml 模組：Install-Module powershell-yaml -Scope CurrentUser" }
$Utf8 = New-Object System.Text.UTF8Encoding($false)
function WriteUtf8([string]$Path,[string]$Text){ [System.IO.File]::WriteAllText($Path,$Text,$Utf8) }
$RULES = ".\rules.yaml"; $MANI = ".\run_manifest.json"; $PFF = ".\preflight_files.json"; $PY = ".\.venv\Scripts\python.exe"
$bk = "rules.yaml.bak_{0}" -f (Get-Date -Format yyyyMMdd_HHmmss); Copy-Item $RULES $bk -Force; Write-Host ("Backup => {0}" -f $bk)
$doc = Get-Content $RULES -Raw | ConvertFrom-Yaml
if (-not $doc.validation) { $doc.validation = [ordered]@{} }
if (-not $doc.validation.freshness) { $doc.validation.freshness = [ordered]@{} }
if (-not $doc.validation.freshness.relaxed_event_datasets) { $doc.validation.freshness.relaxed_event_datasets = @() }
if ($doc.validation.freshness.relaxed_event_datasets -isnot [System.Collections.ArrayList]) { $doc.validation.freshness.relaxed_event_datasets = [System.Collections.ArrayList]$doc.validation.freshness.relaxed_event_datasets }
if (-not ($doc.validation.freshness.relaxed_event_datasets -contains "dividend")) { [void]$doc.validation.freshness.relaxed_event_datasets.Add("dividend") }
if (-not $doc.validation.freshness.trading_days_lag_max) { $doc.validation.freshness.trading_days_lag_max = [ordered]@{ default = 2 } }
$doc.validation.freshness.trading_days_lag_max.dividend = $TDLag
WriteUtf8 $RULES ($doc | ConvertTo-Yaml); Write-Host ("rules.yaml patched (dividend relaxed; TD cap={0})" -f $TDLag)
if (Test-Path $MANI) { $mani = Get-Content $MANI -Raw | ConvertFrom-Json; $mani.ssot_hash = (Get-FileHash $RULES -Algorithm SHA256).Hash; WriteUtf8 $MANI ($mani | ConvertTo-Json -Depth 10); Write-Host "run_manifest.json ssot_hash updated." }
if (Test-Path $PFF) { $map = Get-Content $PFF -Raw | ConvertFrom-Json } else {
  $map = [pscustomobject]@{
    calendar = @{ anchor = "W-FRI"; tz = "Asia/Taipei" }
    freshness = @{
      prices   = @{ max_lag_days = 2 }
      chip     = @{ max_lag_days = 2 }
      dividend = @{ max_lag_days = $TDLag }
      per      = @{ max_lag_days = 365 }
    }
    fail_on_schema = $true
  }
}
if (-not $map.freshness) { $map | Add-Member -Name freshness -Value (@{}) -MemberType NoteProperty }
if (-not $map.freshness.dividend) { $map.freshness | Add-Member -Name dividend -Value (@{ max_lag_days = $TDLag }) -MemberType NoteProperty }
$map.freshness.dividend.max_lag_days = $TDLag
WriteUtf8 $PFF ($map | ConvertTo-Json -Depth 10); Write-Host ("preflight_files.json updated (dividend.max_lag_days={0})" -f $TDLag)
& $PY .\scripts\preflight_check.py --rules .\rules.yaml --export .\reports --root .
$pf = Get-Content .\reports\preflight_report.json -Raw | ConvertFrom-Json
$summary = "Freshness: prices={0} chip={1} dividend={2} per={3}" -f $pf.freshness.prices.max_date, $pf.freshness.chip.max_date, $pf.freshness.dividend.max_date, $pf.freshness.per.max_date
WriteUtf8 ".\reports\repair_dividend_relax_summary.txt" $summary
$result = [ordered]@{ tool="Repair-RelaxDividendFreshness"; td_lag=$TDLag; rules_bak=$bk; ssot_hash=(Get-FileHash $RULES -Algorithm SHA256).Hash; freshness=$pf.freshness; summary=$summary; generated_at=(Get-Date).ToString("s") }
WriteUtf8 ".\reports\repair_dividend_relax_result.json" ($result | ConvertTo-Json -Depth 10)
Write-Host "✅ 完成：dividend 事件型寬鬆已套用並重跑 preflight。"; Write-Host ("➡ 摘要：{0}" -f $summary)
