param(
    [Parameter(Mandatory = $true)]
    [string]$Date,                                        # as-of date (YYYY-MM-DD)

    [ValidateSet('dryrun', 'evalonly', 'commit')]
    [string]$Mode,                                        # 若省略，會依 Profile 推導

    [ValidateSet('dev', 'test', 'live')]
    [string]$Profile = 'live',                            # dev/test/live → 推導 Mode 與 WF 視窗

    [ValidateSet('classic', 'ai', 'all')]
    [string]$Engine = 'classic',                          # 因子引擎：classic / ai / all

    [string]$Root = '.',                                  # repo root
    [string]$RulesPath = '.\rules_factors.yaml',          # 因子規則 SSOT
    [int]$MaxFactorsPerBatch = 20,                        # 每批最多因子數

    [switch]$ComposeToWF,                                 # 是否在 B 段執行 compose_factors_to_wf
    [switch]$AutoGate,                                    # 是否在 B 段執行 Run-WFGate.ps1（ShowOnly）

    [switch]$DumpPlan,                                    # 是否輸出 factor_plan.<Date>.json
    [string]$PythonExe = '.\.venv\Scripts\python.exe'     # Python 執行檔
)

# ---------------------------------------------------------------------------
# Global settings
# ---------------------------------------------------------------------------
$ErrorActionPreference = 'Stop'

# 將 root 正規化，避免相對路徑造成混亂
$Root = [System.IO.Path]::GetFullPath($Root)

# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------
function Write-Phase2Info {
    param([string]$Message)
    Write-Host "[Phase2] $Message" -ForegroundColor Cyan
}

function Write-Phase2Warn {
    param([string]$Message)
    Write-Warning "[Phase2] $Message"
}

function Write-Phase2Error {
    param([string]$Message)
    Write-Error "[Phase2] $Message"
}

# ---------------------------------------------------------------------------
# Utility: validate / parse date
# ---------------------------------------------------------------------------
function Get-AsOfDate {
    param([string]$DateText)

    try {
        return [datetime]::ParseExact($DateText, 'yyyy-MM-dd', $null)
    } catch {
        throw "Invalid -Date '$DateText'. Expected format: yyyy-MM-dd."
    }
}

# ---------------------------------------------------------------------------
# Utility: invoke Python tools
# ---------------------------------------------------------------------------
function Invoke-PythonTool {
    param(
        [Parameter(Mandatory = $true)][string]$PythonExePath,
        [Parameter(Mandatory = $true)][string]$ScriptPath,
        [string[]]$Arguments
    )

    if (-not (Test-Path -LiteralPath $PythonExePath)) {
        throw "Python executable not found: $PythonExePath"
    }
    if (-not (Test-Path -LiteralPath $ScriptPath)) {
        throw "Python script not found: $ScriptPath"
    }

    $argList = @($ScriptPath) + ($Arguments | Where-Object { $_ -ne $null })
    Write-Phase2Info "Running: $PythonExePath $($argList -join ' ')"

    $output = & $PythonExePath @argList
    $exitCode = $LASTEXITCODE

    if ($exitCode -ne 0) {
        $text = ($output -join "`n")
        throw "Python script failed: $ScriptPath (exit=$exitCode)`n$text"
    }

    return $output
}

function Invoke-PythonJson {
    param(
        [Parameter(Mandatory = $true)][string]$PythonExePath,
        [Parameter(Mandatory = $true)][string]$ScriptPath,
        [string[]]$Arguments
    )

    $output = Invoke-PythonTool -PythonExePath $PythonExePath -ScriptPath $ScriptPath -Arguments $Arguments
    $jsonText = ($output -join "`n").Trim()
    if ([string]::IsNullOrWhiteSpace($jsonText)) {
        throw "Python script $ScriptPath did not produce JSON on stdout."
    }
    return $jsonText | ConvertFrom-Json -Depth 10
}

# ---------------------------------------------------------------------------
# Utility: profile / mode / WF windows 決策
# ---------------------------------------------------------------------------
function Resolve-ProfileAndMode {
    param(
        [string]$Profile,
        [string]$Mode
    )

    $effective = [ordered]@{
        Profile   = $Profile
        Mode      = $Mode
        WfWindows = @()
    }

    switch ($Profile) {
        'dev'  { $effective.WfWindows = @(6);         if (-not $Mode) { $effective.Mode = 'dryrun'  } }
        'test' { $effective.WfWindows = @(6, 12);     if (-not $Mode) { $effective.Mode = 'evalonly'} }
        'live' { $effective.WfWindows = @(6, 12, 24); if (-not $Mode) { $effective.Mode = 'commit' } }
        default {
            $effective.WfWindows = @(6, 12, 24)
            if (-not $Mode) { $effective.Mode = 'commit' }
        }
    }

    if (-not $effective.Mode) {
        throw "Effective Mode is empty. Profile=$Profile, Mode=$Mode"
    }

    return $effective
}

function New-FactorPlan {
    param(
        [Parameter(Mandatory = $true)]$Registry,
        [Parameter(Mandatory = $true)]$StatusRecords,
        [Parameter(Mandatory = $true)][string]$Mode,
        [Parameter(Mandatory = $true)][string]$Profile,
        [Parameter(Mandatory = $true)][string]$Engine,
        [int]$MaxFactorsPerBatch = 20,
        [int[]]$WfWindows = @(6, 12, 24)
    )

    # 建立 registry 索引：factor_id → config
    $registryIndex = @{}
    foreach ($f in $Registry.factors) {
        if (-not $f.factor_id) { continue }
        $registryIndex[$f.factor_id] = $f
    }

    $planItems = @()

    foreach ($s in $StatusRecords) {
        $fid = $s.factor_id
        if (-not $fid) { continue }

        $cfg = $null
        if ($registryIndex.ContainsKey($fid)) {
            $cfg = $registryIndex[$fid]
        }

        # -------------------------
        # engine 決策：
        # - 若 registry 裡沒填 engine 或是空字串 → 視為 'classic'
        # - 若有填 → 用原本的值（'classic' / 'ai'）
        # -------------------------
        $engineTag = $null
        if ($cfg -and $cfg.engine) {
            $engineTag = [string]$cfg.engine
        } else {
            $engineTag = 'classic'
        }

        # 依 CLI 參數 Engine 過濾（classic / ai / all）
        if ($Engine -eq 'classic' -and $engineTag -ne 'classic') { continue }
        if ($Engine -eq 'ai'      -and $engineTag -ne 'ai')      { continue }
        # Engine='all' → 不過濾

        $requiredAction = if ($s.required_action) { $s.required_action } else { 'unknown' }

        # Mode × required_action 決策表
        $decidedAction = switch ($Mode) {
            'dryrun' {
                switch ($requiredAction) {
                    'missing'      { 'compute+eval' }
                    'rebuild'      { 'compute+eval' }
                    'ok'           { 'skip' }
                    'orphan_data'  { 'orphan' }
                    default        { 'unknown' }
                }
            }
            'evalonly' {
                switch ($requiredAction) {
                    'missing'      { 'compute+eval' }  # 只標計畫，B 段會依 Mode 決定是否實跑
                    'rebuild'      { 'eval_only' }
                    'ok'           { 'skip' }
                    'orphan_data'  { 'orphan' }
                    default        { 'unknown' }
                }
            }
            'commit' {
                switch ($requiredAction) {
                    'missing'      { 'compute+eval' }
                    'rebuild'      { 'compute+eval' }
                    'ok'           { 'skip' }
                    'orphan_data'  { 'orphan' }
                    default        { 'unknown' }
                }
            }
            default {
                throw "Unsupported Mode: $Mode"
            }
        }

        $planItems += [pscustomobject]@{
            factor_id        = $fid
            category         = if ($cfg) { $cfg.category } else { $null }
            engine           = $engineTag
            enabled          = if ($cfg) { $cfg.enabled } else { $null }
            required_action  = $requiredAction
            decided_action   = $decidedAction
            profile          = $Profile
            mode             = $Mode
            windows          = $WfWindows
            batch_index      = $null  # 後面分批再填
        }
    }

    # 依 decided_action 分配 batch_index（只對 compute+eval / eval_only）
    $active = $planItems | Where-Object {
        $_.decided_action -in @('compute+eval', 'eval_only')
    }

    $batchIndex = 0
    $counter    = 0

    foreach ($item in $active) {
        if ($counter -ge $MaxFactorsPerBatch) {
            $batchIndex++
            $counter = 0
        }
        $item.batch_index = $batchIndex
        $counter++
    }

    return $planItems
}

# ---------------------------------------------------------------------------
# A 段：計畫（Plan）
# ---------------------------------------------------------------------------
Set-Location -LiteralPath $Root
Write-Phase2Info "Run-Phase2-OneClick (A-segment: plan)"
Write-Phase2Info "Root=$Root Date=$Date Profile=$Profile Mode(raw)=$Mode Engine=$Engine"

$asOfDate = Get-AsOfDate -DateText $Date
$resolved = Resolve-ProfileAndMode -Profile $Profile -Mode $Mode
$effectiveProfile = $resolved.Profile
$effectiveMode    = $resolved.Mode
$wfWindows        = $resolved.WfWindows

Write-Phase2Info "Effective profile=$effectiveProfile mode=$effectiveMode WF_windows=$($wfWindows -join ',')"

# 準備 reports 目錄
$reportsDir = Join-Path $Root 'reports'
if (-not (Test-Path -LiteralPath $reportsDir)) {
    New-Item -ItemType Directory -Path $reportsDir | Out-Null
}

# ---------------------------------------------------------------------------
# 呼叫 factor_registry.py 取得 registry JSON
# ---------------------------------------------------------------------------
$registryScript = Join-Path $Root 'scripts\factor_registry.py'
$registryJson = Invoke-PythonJson -PythonExePath $PythonExe `
                                  -ScriptPath $registryScript `
                                  -Arguments @(
                                      '--root',  $Root,
                                      '--rules', $RulesPath,
                                      '--json',
                                      '--log-level', 'WARNING'
                                  )

if (-not $registryJson) {
    throw "Failed to load factor registry (no JSON returned)."
}

# ---------------------------------------------------------------------------
# 呼叫 factor_status.py 取得 status JSON
# ---------------------------------------------------------------------------
$statusScript   = Join-Path $Root 'scripts\factor_status.py'
$statusJsonPath = Join-Path $reportsDir ("factor_status.{0}.json" -f $Date)

$null = Invoke-PythonTool -PythonExePath $PythonExe `
                          -ScriptPath $statusScript `
                          -Arguments @(
                              '--root',         $Root,
                              '--expect-date',  $Date,
                              '--window-months','24',
                              '--rules',        $RulesPath,
                              '--output-json',  $statusJsonPath,
                              '--log-level',    'WARNING'
                          )

if (-not (Test-Path -LiteralPath $statusJsonPath)) {
    throw "factor_status.py did not produce expected JSON file: $statusJsonPath"
}

$statusRecords = Get-Content -LiteralPath $statusJsonPath -Raw | ConvertFrom-Json -Depth 10
if (-not $statusRecords) {
    Write-Phase2Warn "No factor status records found in $statusJsonPath."
    $statusRecords = @()
}

# ---------------------------------------------------------------------------
# 建立 plan
# ---------------------------------------------------------------------------
$planItems = New-FactorPlan -Registry $registryJson `
                            -StatusRecords $statusRecords `
                            -Mode $effectiveMode `
                            -Profile $effectiveProfile `
                            -Engine $Engine `
                            -MaxFactorsPerBatch $MaxFactorsPerBatch `
                            -WfWindows $wfWindows

$Global:Phase2Plan = $planItems

# 統計摘要
$byAction = $planItems | Group-Object -Property decided_action | Sort-Object -Property Name
foreach ($g in $byAction) {
    Write-Phase2Info ("Plan action {0,-12}: {1,4} factors" -f $g.Name, $g.Count)
}

$activeCount = ($planItems | Where-Object { $_.decided_action -in @('compute+eval','eval_only') }).Count
Write-Phase2Info "Active factors (compute+eval / eval_only): $activeCount"

# 可選：輸出 factor_plan.<Date>.json
if ($DumpPlan.IsPresent) {
    $planPath = Join-Path $reportsDir ("factor_plan.{0}.json" -f $Date)
    ($planItems | ConvertTo-Json -Depth 10) | Set-Content -LiteralPath $planPath -Encoding UTF8
    Write-Phase2Info "Wrote factor plan JSON to $planPath"
}

# ---------------------------------------------------------------------------
# B 段：Execution segment
# 依照 A 段產生的 $Global:Phase2Plan，實際執行
# 1) engine (compute parquet)
# 2) eval (factor_eval)
# 3) compose_factors_to_wf + factor_slo
# 4) AutoGate（可選）
# ---------------------------------------------------------------------------

function Invoke-FactorEngineBatches {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExePath,
        [Parameter(Mandatory = $true)]
        [string]$RootPath,
        [Parameter(Mandatory = $true)]
        [string]$RulesPath,
        [Parameter(Mandatory = $true)]
        [int[]]$WfWindows,
        [Parameter(Mandatory = $true)]
        [object[]]$PlanItems
    )

    $engineScript = Join-Path $RootPath 'scripts\factor_engine.py'

    # 只處理 decided_action = compute+eval 的因子
    $active = $PlanItems | Where-Object { $_.decided_action -eq 'compute+eval' }

    if (-not $active -or $active.Count -eq 0) {
        Write-Phase2Info "No factors with decided_action=compute+eval → engine phase skipped."
        return
    }

    $byBatch = $active | Group-Object -Property batch_index | Sort-Object -Property Name

    foreach ($batch in $byBatch) {
        $batchIndex      = $batch.Name
        $factorsInBatch  = $batch.Group.factor_id
        $factorList      = ($factorsInBatch -join ',')
        $windowsArg      = ($WfWindows -join ',')

        Write-Phase2Info ("[Engine] Batch {0}: {1} factors → {2}" -f `
            $batchIndex, $factorsInBatch.Count, $factorList)

        $args = @(
            '--root',    $RootPath,
            '--rules',   $RulesPath,
            '--factors', $factorList,
            '--end',     $Date,
            '--windows', $windowsArg,
            '--run-id-prefix','factor-phase2',
            '--log-level','INFO'
        )

        # 不傳 start_date，讓 engine 依規則自行決定
        $null = Invoke-PythonTool -PythonExePath $PythonExePath `
                                  -ScriptPath  $engineScript `
                                  -Arguments   $args
    }
}

function Invoke-FactorEval {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExePath,
        [Parameter(Mandatory = $true)]
        [string]$RootPath,
        [Parameter(Mandatory = $true)]
        [int[]]$WfWindows,
        [Parameter(Mandatory = $true)]
        [string]$AsOfDate,
        [Parameter(Mandatory = $true)]
        [object[]]$PlanItems
    )

    $evalScript = Join-Path $RootPath 'scripts\factor_eval.py'

    if (-not (Test-Path -LiteralPath $evalScript)) {
        Write-Phase2Warn "factor_eval.py not found at $evalScript; eval phase skipped."
        return
    }

    # 只對 decided_action in (compute+eval, eval_only) 的因子做 eval
    $active = $PlanItems | Where-Object {
        $_.decided_action -in @('compute+eval','eval_only')
    }

    if (-not $active -or $active.Count -eq 0) {
        Write-Phase2Info "No active factors for eval phase; factor_eval will be skipped."
        return
    }

    $factorIds = $active | Select-Object -ExpandProperty factor_id -Unique

    Write-Phase2Info ("Eval phase: {0} factors → {1}" -f $factorIds.Count, ($factorIds -join ','))

    foreach ($fid in $factorIds) {
        if (-not $fid) { continue }

        $args = @(
            '--root',      $RootPath,
            '--factor-id', $fid,
            '--wf-windows'
        ) + ($WfWindows | ForEach-Object { "$_" }) + @(
            '--as-of',     $AsOfDate
        )

        $null = Invoke-PythonTool -PythonExePath $PythonExePath `
                                  -ScriptPath  $evalScript `
                                  -Arguments   $args
    }
}

function Invoke-ComposeFactorsToWF {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExePath,
        [Parameter(Mandatory = $true)]
        [string]$RootPath,
        [Parameter(Mandatory = $true)]
        [string]$RulesPath,
        [Parameter(Mandatory = $true)]
        [int[]]$WfWindows,
        [Parameter(Mandatory = $true)]
        [string]$SloProfile,
        [Parameter(Mandatory = $true)]
        [string]$SloEngine
    )

    $composeScript = Join-Path $RootPath 'scripts\compose_factors_to_wf.py'
    $wfSummaryPath = Join-Path $RootPath 'reports\wf_summary.json'
    $factorEvalDir = Join-Path $RootPath 'reports\factor_eval'

    $args = @(
        '--root',           $RootPath,
        '--rules-file',     $RulesPath,
        '--wf-summary',     $wfSummaryPath,
        '--factor-eval-dir',$factorEvalDir,
        '--wf-windows'
    ) + ($WfWindows | ForEach-Object { "$_" }) + @(
        '--mode',          'all',
        '--slo-profile',   $SloProfile,
        '--slo-engine',    $SloEngine,
        '--log-level',     'INFO'
    )

    $null = Invoke-PythonTool -PythonExePath $PythonExePath `
                              -ScriptPath  $composeScript `
                              -Arguments   $args

    return $wfSummaryPath
}

function Show-FactorSLOFromWfSummary {
    param(
        [Parameter(Mandatory = $true)]
        [string]$WfSummaryPath
    )

    if (-not (Test-Path -LiteralPath $WfSummaryPath)) {
        Write-Phase2Warn "wf_summary.json not found at $WfSummaryPath; skip SLO display."
        return
    }

    try {
        $wf = Get-Content -LiteralPath $WfSummaryPath -Raw | ConvertFrom-Json -Depth 20
    } catch {
        Write-Phase2Warn ("Failed to parse wf_summary.json at {0}: {1}" -f $WfSummaryPath, $_)
        return
    }

    if (-not $wf.factor_slo) {
        Write-Phase2Info "wf_summary.json has no factor_slo section; skip SLO display."
        return
    }

    $slo = $wf.factor_slo

    $profile   = $slo.profile
    $engine    = $slo.engine
    $windows   = if ($slo.windows) { ($slo.windows -join ',') } else { 'n/a' }
    $counts    = if ($slo.per_window_counts) {
        ($slo.per_window_counts.GetEnumerator() |
            Sort-Object -Property Name |
            ForEach-Object { "{0}:{1}" -f $_.Name, $_.Value }) -join ' '
    } else {
        'n/a'
    }
    $missing   = if ($slo.missing_required_factors) {
        ($slo.missing_required_factors -join ',')
    } else {
        ''
    }
    $satisfied = if ($slo.satisfied -eq $true) { 'YES' } else { 'NO' }

    Write-Phase2Info ("[SLO] profile={0} engine={1} windows={2}" -f $profile, $engine, $windows)
    Write-Phase2Info ("[SLO] per_window_counts={0}" -f $counts)
    if ($missing) {
        Write-Phase2Warn ("[SLO] missing_required_factors={0}" -f $missing)
    }
    Write-Phase2Info ("[SLO] satisfied={0}" -f $satisfied)
}

function Invoke-AutoGate {
    param(
        [Parameter(Mandatory = $true)]
        [string]$RootPath,
        [Parameter(Mandatory = $true)]
        [string]$Date
    )

    $gateScript = Join-Path $RootPath 'tools\gate\Run-WFGate.ps1'
    if (-not (Test-Path -LiteralPath $gateScript)) {
        Write-Phase2Warn "AutoGate requested but Run-WFGate.ps1 not found at $gateScript."
        return
    }

    Write-Phase2Info "[Gate] AutoGate enabled → invoking Run-WFGate.ps1 (ShowOnly)."

    & $gateScript `
        -Date  $Date `
        -Mode  'safe' `
        -WFDir '.\tools\gate\wf_configs' `
        -Root  '.' `
        -ShowOnly

    if ($LASTEXITCODE -ne 0) {
        throw "AutoGate (Run-WFGate.ps1) failed with exit code $LASTEXITCODE."
    }
}

# ---------------------------------------------------------------------------
# B 段主流程
# ---------------------------------------------------------------------------

# Mode=dryrun：只做 A 段規劃，不跑 engine/eval/compose
if ($effectiveMode -eq 'dryrun') {
    Write-Phase2Info "Mode=dryrun → planning only. No engine/eval/compose will be executed."
    Write-Phase2Info "Run-Phase2-OneClick (A-segment only) completed."
    exit 0
}

if (-not $Global:Phase2Plan) {
    Write-Phase2Warn "Global Phase2Plan is empty; nothing to execute."
    Write-Phase2Info "Run-Phase2-OneClick finished (no work)."
    exit 0
}

$planItems   = $Global:Phase2Plan
$activeItems = $planItems | Where-Object { $_.decided_action -in @('compute+eval','eval_only') }

if (-not $activeItems -or $activeItems.Count -eq 0) {
    Write-Phase2Info "No active factors (compute+eval / eval_only) in plan; nothing to do."
    Write-Phase2Info "Run-Phase2-OneClick finished (A-segment only effective)."
    exit 0
}

Write-Phase2Info ("Execution segment starting for {0} active factors." -f $activeItems.Count)

# ----------------------
# 1) Engine phase
# ----------------------
if ($effectiveMode -eq 'evalonly') {
    Write-Phase2Info "Mode=evalonly → engine phase skipped (reuse existing factor parquet)."
} else {
    Invoke-FactorEngineBatches -PythonExePath $PythonExe `
                               -RootPath     $Root `
                               -RulesPath    $RulesPath `
                               -WfWindows    $wfWindows `
                               -PlanItems    $planItems
}

# ----------------------
# 2) Eval phase
# ----------------------
Write-Phase2Info "Starting factor_eval phase."
Invoke-FactorEval -PythonExePath $PythonExe `
                  -RootPath     $Root `
                  -WfWindows    $wfWindows `
                  -AsOfDate     $Date `
                  -PlanItems    $planItems

# ----------------------
# 3) Compose to wf_summary + factor_slo
# ----------------------
if ($ComposeToWF.IsPresent) {
    Write-Phase2Info "ComposeToWF enabled → composing factor_eval into wf_summary.json + factor_slo."
    $wfSummaryPath = Invoke-ComposeFactorsToWF -PythonExePath $PythonExe `
                                               -RootPath     $Root `
                                               -RulesPath    $RulesPath `
                                               -WfWindows    $wfWindows `
                                               -SloProfile   $effectiveProfile `
                                               -SloEngine    $Engine

    Show-FactorSLOFromWfSummary -WfSummaryPath $wfSummaryPath
} else {
    Write-Phase2Info "ComposeToWF not specified → skip compose_factors_to_wf."
}

# ----------------------
# 4) AutoGate (optional)
# ----------------------
if ($AutoGate.IsPresent -and $ComposeToWF.IsPresent -and $effectiveMode -eq 'commit') {
    Invoke-AutoGate -RootPath $Root -Date $Date
} else {
    if ($AutoGate.IsPresent -and -not $ComposeToWF.IsPresent) {
        Write-Phase2Warn "AutoGate requested but ComposeToWF is disabled; Gate will see old wf_summary.json."
    }
}

Write-Phase2Info "Run-Phase2-OneClick (A+B segments) completed successfully."
exit 0
