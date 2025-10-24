<# tools/gate/Run-WFGate.ps1 — FINAL: supports v4X rules paths + robust numerics #>
[CmdletBinding()]
param(
  [string]$Rules = ".\rules.yaml",
  [string]$Runner = ".\scripts\wf_runner.py",
  [string]$Python = ".\.venv\Scripts\python.exe",
  [string]$InputsDir = ".\runs\wf_inputs_gate",
  [string]$ReportsDir = ".\reports",
  [switch]$SmokeOK = $true,
  [switch]$SkipRunner = $false
)

$ErrorActionPreference = 'Stop'
if(!(Test-Path $ReportsDir)){ New-Item -ItemType Directory -Force -Path $ReportsDir | Out-Null }
$GateRawPath  = Join-Path $ReportsDir 'gate_summary.json'
$DecisionPath = Join-Path $ReportsDir 'gate_decision.json'

# ---------- Helpers ----------
function Get-SSOT {
  param([Parameter(Mandatory)][string]$Path)
  if(([System.IO.Path]::GetExtension($Path)).ToLower() -eq '.json'){
    return (Get-Content $Path -Raw | ConvertFrom-Json)
  }
  $py = $Python
  $shim = @"
import sys, json
try:
    import yaml
except Exception:
    print("PY_YAML_MISSING", file=sys.stderr); sys.exit(3)
p = sys.argv[1]
with open(p, "r", encoding="utf-8") as f:
    text = f.read()
data = yaml.safe_load(text)  # duplicate key -> last wins
print(json.dumps(data, ensure_ascii=False))
"@
  $tmp = Join-Path $env:TEMP "yaml2json_gate.py"
  Set-Content -Path $tmp -Encoding UTF8 -Value $shim
  $json = & $py $tmp $Path
  if($LASTEXITCODE -ne 0){ throw "YAML 解析失敗（請確認 .venv 的 PyYAML 就緒）" }
  return $json | ConvertFrom-Json
}

function Get-Prop { param($obj,[string]$name)
  if($null -eq $obj){ return $null }
  $p = $obj.PSObject.Properties[$name]
  if($null -eq $p){
    $p = $obj.PSObject.Properties | Where-Object { $_.Name -ieq $name } | Select-Object -First 1
  }
  if($null -eq $p){ return $null } else { return $p.Value }
}

function Get-FromPaths {
  param($root,[string[]]$paths,$default=$null)
  foreach($p in $paths){
    $cur=$root; $ok=$true
    foreach($seg in ($p -split '\.')){
      if($null -eq $cur){ $ok=$false; break }
      $prop = $cur.PSObject.Properties[$seg]
      if($null -eq $prop){
        $prop = $cur.PSObject.Properties | ?{ $_.Name -ieq $seg } | Select -First 1
      }
      if($null -eq $prop){ $ok=$false; break }
      $cur = $prop.Value
    }
    if($ok){ return $cur }
  }
  return $default
}

function To-Double { param($v)
  if($null -eq $v){ return $null }
  if($v -is [double]){ return $v }
  $s = ($v.ToString()).Trim() -replace '^\[double\]\s*',''
  $style=[System.Globalization.NumberStyles]::Float
  $culture=[System.Globalization.CultureInfo]::InvariantCulture
  [double]$out=0
  if([double]::TryParse($s,$style,$culture,[ref]$out)){ return $out }
  throw "無法轉 double：'$v'"
}

function Normalize-DD { param($x)
  $val = To-Double $x
  if($null -eq $val){ return $null }
  if($val -gt 1){ return [math]::Round($val/100.0, 6) } else { return [math]::Round($val, 6) }
}

function Get-MissingKeys { param($obj,[string[]]$keys)
  $missing=@()
  foreach($k in $keys){ if($null -eq (Get-Prop $obj $k)){ $missing+=$k } }
  return $missing
}

function Build-Rules {
  param($cfg)
  # 同時支援：gate.* 與 v4X 規格 (validation.*, risk.*, walk_forward.*, costs_exec.*)
  $R = [ordered]@{}
  $R.sharpe_min = To-Double (Get-FromPaths $cfg @('gate.sharpe_min')) ; if($null -eq $R.sharpe_min){ $R.sharpe_min = 1.0 }
  $R.max_drawdown_pct = Normalize-DD (Get-FromPaths $cfg @(
      'gate.max_drawdown_pct', 'validation.risk.max_drawdown_pct', 'risk.max_drawdown_pct'
    )); if($null -eq $R.max_drawdown_pct){ $R.max_drawdown_pct = 0.20 }
  $R.wf_pass_min = To-Double (Get-FromPaths $cfg @(
      'gate.wf_pass_min', 'validation.walk_forward.wf_pass_min', 'walk_forward.wf_pass_min'
    )); if($null -eq $R.wf_pass_min){ $R.wf_pass_min = 0.80 }
  $R.dsr_min_after_costs = To-Double (Get-FromPaths $cfg @(
      'gate.dsr_min_after_costs', 'validation.alpha_quality.dsr_min_after_costs'
    )); if($null -eq $R.dsr_min_after_costs){ $R.dsr_min_after_costs = 0.0 }
  $R.psr_min = To-Double (Get-FromPaths $cfg @(
      'gate.psr_min', 'validation.alpha_quality.psr_min'
    )); if($null -eq $R.psr_min){ $R.psr_min = 0.9 }
  $R.t_min = To-Double (Get-FromPaths $cfg @(
      'gate.t_min', 'validation.alpha_quality.t_stat_min'
    )); if($null -eq $R.t_min){ $R.t_min = 2.0 }
  $R.replay_mae_bps_max = To-Double (Get-FromPaths $cfg @(
      'gate.replay_mae_bps_max', 'validation.costs_exec.replay_mae_bps_max', 'costs_exec.replay_mae_bps_max'
    )); if($null -eq $R.replay_mae_bps_max){ $R.replay_mae_bps_max = 2.0 }
  $req = Get-FromPaths $cfg @('gate.capacity_ok_required')
  $R.capacity_ok_required = if($null -ne $req){ [bool]$req } else { $true }
  return [pscustomobject]$R
}

# ---------- Decision ----------
function Decide-Strict {
  param($m, $r)
  $need = @('sharpe','wf_pass_rate','dsr_after_costs','psr','t','execution_replay_mae_bps','max_drawdown_pct','capacity_ok')
  $miss = Get-MissingKeys -obj $m -keys $need
  if($miss.Count -gt 0){ return @{ ok=$false; missing=$miss; failed=@() } }

  $sharpe = To-Double (Get-Prop $m 'sharpe')
  $wf     = To-Double (Get-Prop $m 'wf_pass_rate')
  $dsr    = To-Double (Get-Prop $m 'dsr_after_costs')
  $psr    = To-Double (Get-Prop $m 'psr')
  $t      = To-Double (Get-Prop $m 't')
  $mae    = To-Double (Get-Prop $m 'execution_replay_mae_bps')
  $dd     = Normalize-DD (Get-Prop $m 'max_drawdown_pct')
  $capVal = [bool](Get-Prop $m 'capacity_ok')

  $okSharpe = ($sharpe -ge $r.sharpe_min)
  $okDD     = ($dd -le $r.max_drawdown_pct)
  $okWF     = ($wf -ge $r.wf_pass_min)
  $okDSR    = ($dsr -gt $r.dsr_min_after_costs)
  $okPSR    = ($psr -ge $r.psr_min)
  $okT      = ($t   -ge $r.t_min)
  $okCap    = ($r.capacity_ok_required ? $capVal : $true)
  $okMAE    = ($mae -le $r.replay_mae_bps_max)

  $failed = @()
  if(-not $okSharpe){ $failed += 'sharpe' }
  if(-not $okDD){     $failed += 'max_drawdown_pct' }
  if(-not $okWF){     $failed += 'wf_pass_rate' }
  if(-not $okDSR){    $failed += 'dsr_after_costs' }
  if(-not $okPSR){    $failed += 'psr' }
  if(-not $okT){      $failed += 't' }
  if(-not $okCap){    $failed += 'capacity_ok' }
  if(-not $okMAE){    $failed += 'execution_replay_mae_bps' }

  return @{ ok=($failed.Count -eq 0); failed=$failed; missing=@() }
}

function Decide-Smoke {
  param($m, $r)
  $need = @('sharpe','wf_pass_rate','dsr_after_costs','max_drawdown_pct')
  $miss = Get-MissingKeys -obj $m -keys $need
  if($miss.Count -gt 0){ return @{ ok=$false; missing=$miss; failed=@() } }

  $sharpe = To-Double (Get-Prop $m 'sharpe')
  $wf     = To-Double (Get-Prop $m 'wf_pass_rate')
  $dsr    = To-Double (Get-Prop $m 'dsr_after_costs')
  $dd     = Normalize-DD (Get-Prop $m 'max_drawdown_pct')

  $okSharpe = ($sharpe -ge $r.sharpe_min)
  $okDD     = ($dd -le $r.max_drawdown_pct)
  $okWF     = ($wf -ge $r.wf_pass_min)
  $okDSR    = ($dsr -gt $r.dsr_min_after_costs)

  $failed = @()
  if(-not $okSharpe){ $failed += 'sharpe' }
  if(-not $okDD){     $failed += 'max_drawdown_pct' }
  if(-not $okWF){     $failed += 'wf_pass_rate' }
  if(-not $okDSR){    $failed += 'dsr_after_costs' }

  return @{ ok=($failed.Count -eq 0); failed=$failed; missing=@() }
}

# ---------- Orchestration ----------
if(-not $SkipRunner){
  & $Python $Runner --dir $InputsDir --export $GateRawPath | Write-Verbose
  if($LASTEXITCODE -ne 0){ throw "wf_runner 執行失敗（$LASTEXITCODE）" }
  if(Test-Path $GateRawPath -PathType Container){
    throw "wf_runner 的 --export 指向資料夾（$GateRawPath）。請改為檔案，例如 reports\\gate_summary.json"
  }
}

$cfg = Get-SSOT -Path $Rules
$R = Build-Rules $cfg

if(!(Test-Path $GateRawPath)){ throw "找不到 $GateRawPath（請先產生 gate_summary.json）" }
if(Test-Path $GateRawPath -PathType Container){ throw "期望檔案，卻是資料夾：$GateRawPath" }
$raw = Get-Content $GateRawPath -Raw | ConvertFrom-Json
$items = @(); if($null -eq $raw){ throw "gate_summary.json 為 null" } elseif($raw -is [System.Collections.IEnumerable]){ $items = @($raw) } else { $items = @($raw) }

$records = @()
foreach($m in $items){
  $strict = Decide-Strict -m $m -r $R
  $smoke  = Decide-Smoke  -m $m -r $R
  $ok_final = $SmokeOK ? ($strict.ok -or $smoke.ok) : $strict.ok
  $records += [pscustomobject]@{
    run_id    = (Get-Prop $m 'run_id')
    metrics   = $m
    ok_strict = $strict.ok
    ok_smoke  = $smoke.ok
    ok_final  = $ok_final
    reasons   = @{
      strict_failed  = $strict.failed
      strict_missing = $strict.missing
      smoke_failed   = $smoke.failed
      smoke_missing  = $smoke.missing
      notes = @()
    }
  }
}

$strictPass = ($records | ?{$_.ok_strict}).Count
$smokeAdded = ($records | ?{ -not $_.ok_strict -and $_.ok_smoke }).Count
$finalPass  = ($records | ?{$_.ok_final}).Count
$decision = [ordered]@{
  mode  = ($SmokeOK ? 'strict+smoke' : 'strict')
  ok    = ($finalPass -gt 0 -and $finalPass -eq $records.Count)
  counts = @{
    total       = $records.Count
    strict_pass = $strictPass
    smoke_added = $smokeAdded
    final_pass  = $finalPass
    pass_rate   = if($records.Count){ [math]::Round($finalPass / $records.Count, 4) } else { 0 }
  }
  rules        = $R
  items        = $records
  generated_at = (Get-Date).ToString("s")
}

# === Output & Exit (revised semantics) ===
# 狀態：strict_all | smoke_present | fail
$hasFail   = ($records | Where-Object { -not $_.ok_final }).Count -gt 0
$hasSmoke  = ($records | Where-Object { -not $_.ok_strict -and $_.ok_smoke }).Count -gt 0
$allStrict = ($records | Where-Object { $_.ok_strict }).Count -eq $records.Count

$decision.status = if($hasFail){ 'fail' } elseif($allStrict){ 'strict_all' } else { 'smoke_present' }
$decision | ConvertTo-Json -Depth 10 | Set-Content -Path $DecisionPath -Encoding UTF8

# Exit code：0=全嚴格；2=含任一 smoke（且無 fail）；1=有 fail
$exit = if($hasFail){ 1 } elseif($hasSmoke){ 2 } else { 0 }

Write-Host ("Gate mode={0} status={1} ok={2} strict_pass={3}/{4} smoke_added={5} final_pass={6}/{4}" -f `
  $decision.mode,$decision.status,$decision.ok,$strictPass,$records.Count,$smokeAdded,$finalPass)
exit $exit
