[CmdletBinding()]
param(
  [string]$Root = (Split-Path -Parent $PSScriptRoot),
  [string]$PythonExe = "",
  [string]$RulesBase = "",
  [string]$RulesOverride = "",
  [ValidateSet("base","override")] [string]$RulesMode = "base",
  [string]$WfDir = "",
  [string]$ReportsDir = "",
  [switch]$SkipGate
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function _Echo([string]$m){ Write-Host ("[{0}] {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $m) }

# 1) helper
$helper = Join-Path $PSScriptRoot 'Check-SSOT-And-Paths.ps1'
if (Test-Path -LiteralPath $helper) {
  . $helper
} else {
  _Echo "Helper not found ($helper). Using minimal built-ins."
  function Resolve-ProjectRoot { param($Candidate) if (Test-Path -LiteralPath $Candidate){ (Resolve-Path -LiteralPath $Candidate).Path } else { throw "Root missing: $Candidate" } }
  function Ensure-Dir { param($Path) if (-not (Test-Path -LiteralPath $Path)) { New-Item -ItemType Directory -Force -Path $Path | Out-Null } (Resolve-Path -LiteralPath $Path).Path }
  function Assert-File { param($Path) if (-not (Test-Path -LiteralPath $Path)) { throw "Required file not found: $Path" } (Resolve-Path -LiteralPath $Path).Path }
  function Get-FileSha256 { param($Path) (Get-FileHash -Algorithm SHA256 -LiteralPath $Path).Hash.ToLowerInvariant() }
  function Read-RulesSchemaVer { param($RulesFile)
    $m = Select-String -Path $RulesFile -Pattern '^\s*schema_ver:\s*([^\s#]+)' -Encoding UTF8 | Select-Object -First 1
    if ($m){ $m.Matches[0].Groups[1].Value.Trim() } else { $null }
  }
  function Write-RunManifest { param($ManifestPath,$SsotHash,[string]$SchemaVer=$null)
    $obj = if (Test-Path -LiteralPath $ManifestPath) { try { Get-Content -LiteralPath $ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json } catch { [ordered]@{} } } else { [ordered]@{} }
    $obj.ssot_hash = $SsotHash; if ($SchemaVer){ $obj.schema_ver = $SchemaVer }
    ($obj | ConvertTo-Json -Depth 8) | Set-Content -LiteralPath $ManifestPath -Encoding UTF8
  }
}

# 2) paths
$Root       = Resolve-ProjectRoot $Root
$ReportsDir = if ($ReportsDir) { $ReportsDir } else { Join-Path $Root 'reports' }
$ReportsDir = Ensure-Dir $ReportsDir
$WfDir      = if ($WfDir) { $WfDir } else { Join-Path $Root 'runs\wf_configs' }
$WfDir      = Resolve-ProjectRoot $WfDir
$RunManifest = Join-Path $Root 'run_manifest.json'

if (-not $PythonExe)  { $PythonExe  = Join-Path $Root '.venv\Scripts\python.exe' }
$PythonExe = Assert-File $PythonExe

if (-not $RulesBase)  { $RulesBase  = Join-Path $Root 'rules.yaml' }
$RulesBase = Assert-File $RulesBase
if ($RulesMode -eq 'override') {
  if (-not $RulesOverride) { throw "RulesMode=override 需要提供 -RulesOverride <path to rules.yaml>" }
  $RulesOverride = Assert-File $RulesOverride
}

# 3) transcript
$datestamp = Get-Date -Format 'yyyy-MM-dd'
$logPath   = Join-Path $ReportsDir ("nightly_{0}.log" -f $datestamp)
try { Start-Transcript -Path $logPath -Append | Out-Null } catch { _Echo "Transcript 啟動失敗：$($_.Exception.Message)（忽略並續跑）" }

try {
  # 4) unlock + env
  $env:ALPHACITY_ALLOW = '1'
  Remove-Item Env:PYTHONSTARTUP -ErrorAction SilentlyContinue | Out-Null
  & $PythonExe -V
  & $PythonExe -c "import pandas, pyarrow; print('OK pandas/pyarrow:', pandas.__version__, pyarrow.__version__)"

  # 5) effective rules & SSOT hash
  $EffectiveRules = Join-Path $ReportsDir 'rules.effective.yaml'
  switch ($RulesMode) {
    'base'     { Copy-Item -LiteralPath $RulesBase     -Destination $EffectiveRules -Force }
    'override' { Copy-Item -LiteralPath $RulesOverride -Destination $EffectiveRules -Force }
  }
  $EffectiveRules = Assert-File $EffectiveRules
  $ssotHash   = Get-FileSha256 $EffectiveRules
  $schemaVer  = Read-RulesSchemaVer $EffectiveRules
  _Echo ("SSOT rules: {0}" -f $EffectiveRules)
  _Echo ("SSOT sha256: {0}" -f $ssotHash)
  if ($schemaVer) { _Echo ("SSOT schema_ver: {0}" -f $schemaVer) }
  Write-RunManifest -ManifestPath $RunManifest -SsotHash $ssotHash -SchemaVer $schemaVer | Out-Null

  # 6) Preflight
  _Echo "Preflight → preflight_check.py"
  & $PythonExe (Join-Path $Root 'scripts\preflight_check.py') --rules $EffectiveRules --export $ReportsDir --root $Root

  # 7) Build Universe（若舊版 argparse 不支援，偵測後自動移除重跑）
  _Echo "Build Universe → build_universe.py"
  $argBuild = @(
    (Join-Path $Root 'scripts\build_universe.py'),
    '--config', (Join-Path $Root 'configs\universe.yaml'),
    '--rules',  $EffectiveRules,
    '--out',    (Join-Path $Root 'configs\investable_universe.txt')
  )
  & $PythonExe @argBuild; $rc=$LASTEXITCODE
  if ($rc -ne 0) {
    Write-Warning ("build_universe.py exit={0}，嘗試移除 後重跑（相容舊版）。" -f $rc)
    $argBuild = $argBuild | Where-Object { $_ -ne 0}
    & $PythonExe @argBuild
  }

  # 8) WF
  _Echo "WF → wf_runner.py"
  & $PythonExe (Join-Path $Root 'scripts\wf_runner.py') --dir $WfDir --export $ReportsDir

  # 9) Gate（存在才跑）
  if (-not $SkipGate) {
    $gateEntrypoint = Join-Path $Root 'tools\Run-WFGate.ps1'
    if (Test-Path -LiteralPath $gateEntrypoint) {
      _Echo "Gate → Run-WFGate.ps1"
      & pwsh -NoProfile -ExecutionPolicy Bypass -File $gateEntrypoint -PythonExe $PythonExe -Rules $EffectiveRules -Reports $ReportsDir
    } else {
      Write-Warning "找不到 tools\Run-WFGate.ps1，略過 Gate（WF 仍已完成）。"
    }
  }

  _Echo ("Nightly 完成。輸出在 {0}" -f $ReportsDir)
}
finally {
  try { Stop-Transcript | Out-Null } catch { }
}







