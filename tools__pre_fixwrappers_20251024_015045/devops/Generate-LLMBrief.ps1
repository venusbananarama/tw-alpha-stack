param([string]$OutDir="handovers")
$ErrorActionPreference = "Stop"
$tag  = (git describe --tags --abbrev=0 2>$null)
$head = (git rev-parse --short HEAD)
$dt   = Get-Date -Format "yyyy-MM"
$pf = if(Test-Path .\reports\preflight_report.json){ Get-Content .\reports\preflight_report.json -Raw | ConvertFrom-Json } else { $null }
$gs = if(Test-Path .\reports\gate_summary.json){ Get-Content .\reports\gate_summary.json -Raw | ConvertFrom-Json } else { $null }
$uni = if(Test-Path .\configs\investable_universe.txt){ (Get-Content .\configs\investable_universe.txt | Measure-Object -Line).Lines } else { 0 }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
$body = @"
LLM_Brief ($dt)

Repo: venusbananarama/tw-alpha-stack
Tag: $tag @ $head

現況
- Universe 行數: $uni
- Freshness: prices=$($pf?.freshness?.prices?.max_date) chip=$($pf?.freshness?.chip?.max_date) dividend=$($pf?.freshness?.dividend?.max_date) per=$($pf?.freshness?.per?.max_date)
- WF/Gate: pass_rate=$($gs?.wf?.pass_rate) overall=$($gs?.overall)

本月重點
<填 3–5 條>

下一步（給新帳號）
.\tools\Switch-GitHubAccount.ps1 -UserName "<>" -Email "<>"
.\tools\Test-RepoHealth.ps1
依 RUNBOOK：Preflight → Build → 回測 → WF → Gate
"@
$path = Join-Path $OutDir ("LLM_Brief_{0}.md" -f $dt)
$body | Set-Content $path -Encoding utf8
Write-Host "Updated $path"
