Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
param([string[]]$Datasets,[string]$Root="C:\AI\tw-alpha-stack")
Import-Module powershell-yaml -ErrorAction Stop
$path = ".\configs\datasets.registry.yaml"
if (-not (Test-Path $path)) { throw "Missing $path" }
$reg  = ConvertFrom-Yaml (Get-Content $path -Raw -Encoding UTF8)

# 取 keys（同時支援屬性與索引器），並做小寫化去空白
$known = @()
if ($reg.datasets)    { $known += $reg.datasets.PSObject.Properties.Name }
if ($reg['datasets']) { $known += $reg['datasets'].Keys }
$known = $known | Where-Object { $_ } | ForEach-Object { ($_ -as [string]).Trim().ToLowerInvariant() } | Select-Object -Unique

$miss = @(); foreach($d in $Datasets){ if ($known -notcontains ($d.Trim().ToLowerInvariant())) { $miss += $d } }
if ($miss.Count) { throw "Unknown datasets: $($miss -join ', ')" }

$py = (Get-Command python -ErrorAction SilentlyContinue).Source
if (-not $py) { $py = "C:\AI\.venv\ac311-alpha\Scripts\python.exe" }

$out = & $py 'scripts\finmind_backfill.py' `
  '--datahub-root' "$Root\datahub" '--start' '2022-09-28' '--end' '2025-09-28' `
  '--symbols' '2330' '--datasets' $Datasets '--plan-only' 2>&1 | Out-String

if ($out -notmatch 'EstCalls=\d+') { throw "Preflight cannot read EstCalls. Raw:`n$out" }
$est = [int]([regex]::Match($out,'EstCalls=(\d+)').Groups[1].Value)
if ($est -le 0) { throw "Preflight EstCalls=$est (no work planned)." }
"Preflight OK (EstCalls=$est)"

