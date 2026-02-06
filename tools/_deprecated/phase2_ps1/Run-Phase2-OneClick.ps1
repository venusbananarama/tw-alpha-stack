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

    # corr 控制：auto = dev 多半關、test/live 開；on = 強制跑；off = 強制不跑
    [ValidateSet('auto','on','off')]
    [string]$CorrMode = 'auto',

    [switch]$ComposeToWF,                                 # 是否在 B 段執行 compose_factors_to_wf
    [switch]$AutoGate,                                    # 是否在 B 段執行 Run-WFGate.ps1（ShowOnly）

    [switch]$DumpPlan,                                    # 是否提示 factor_plan JSON 位置
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

# ---------------------------------------------------------------------------
# Utility: 透過 factor_plan_lib.py 建立 plan（Python 為 SSOT）
# ---------------------------------------------------------------------------
function New-FactorPlan {
    param(
        [Parameter(Mandatory = $true)][string]$Date,
        [Parameter(Mandatory = $true)][string]$Profile,
        [Parameter(Mandatory = $true)][string]$Engine,
        [Parameter(Mandatory = $true)][string]$Root,
        [Parameter(Mandatory = $true)][string]$RulesPath,
        [Parameter(Mandatory = $true)][string]$StatusJsonPath,
        [Parameter(Mandatory = $true)][string]$PythonExe,
        [int]$MaxFactorsPerBatch = 20,
        [int[]]$WfWindows = @(6, 12, 24)
    )

    # 決定要規劃哪種 engine_kind（classic / ai / all）
    $engineKinds = @()
    switch ($Engine) {
        'classic' { $engineKinds = @('classic') }
        'ai'      { $engineKinds = @('ai') }
        'all'     { $engineKinds = @('classic','ai') }
        default   { throw "Unsupported Engine: $Engine" }
    }

    $reportsDir = Join-Path $Root 'reports'
    if (-not (Test-Path -LiteralPath $reportsDir)) {
        New-Item -ItemType Directory -Path $reportsDir | Out-Null
    }

    $planScript = Join-Path $Root 'scripts\p2\factor_plan_lib.py'
    if (-not (Test-Path -LiteralPath $planScript)) {
        throw "factor_plan_lib.py not found at $planScript"
    }

    $allItems = @()

    foreach ($engKind in $engineKinds) {
        $planPath = Join-Path $reportsDir ("factor_plan.{0}.{1}.json" -f $Date, $engKind)

        # 準備 wf-window 參數
        $wfWindowArgs = @()
        foreach ($w in $WfWindows) {
            $wfWindowArgs += @('--wf-window', "$w")
        }

        $args = @(
            '--root',        $Root,
            '--date',        $Date,
            '--profile',     $Profile,
            '--engine',      $engKind,
            '--rules-path',  $RulesPath,
            '--status-path', $StatusJsonPath,
            '--output',      $planPath
        ) + $wfWindowArgs

        Write-Phase2Info ("Building factor plan via factor_plan_lib.py for engine={0}" -f $engKind)
        $null = Invoke-PythonTool -PythonExePath $PythonExe `
                                  -ScriptPath  $planScript `
                                  -Arguments   $args

        if (-not (Test-Path -LiteralPath $planPath)) {
            throw "factor_plan_lib.py did not produce expected JSON file: $planPath"
        }

        $planJson = Get-Content -LiteralPath $planPath -Raw | ConvertFrom-Json -Depth 30
        if ($planJson -and $planJson.items) {
            foreach ($item in $planJson.items) {
                # 標記 engine_kind / profile / windows（純資訊，無邏輯）
                if (-not $item.PSObject.Properties['engine_kind']) {
                    $item | Add-Member -NotePropertyName 'engine_kind' -NotePropertyValue $planJson.engine_kind -Force
                }
                if (-not $item.PSObject.Properties['profile']) {
                    $item | Add-Member -NotePropertyName 'profile' -NotePropertyValue $planJson.profile -Force
                }
                if (-not $item.PSObject.Properties['windows']) {
                    $item | Add-Member -NotePropertyName 'windows' -NotePropertyValue $planJson.wf_windows -Force
                }
                $allItems += $item
            }
        }
    }

    # 分配 batch_index：只對 active 因子 (compute+eval / eval_only)
    $active = $allItems | Where-Object { $_.decided_action -in @('compute+eval','eval_only') }

    $batchIndex = 0
    $counter    = 0

    foreach ($item in $active) {
        if ($counter -ge $MaxFactorsPerBatch) {
            $batchIndex++
            $counter = 0
        }
        if (-not $item.PSObject.Properties['batch_index']) {
            $item | Add-Member -NotePropertyName 'batch_index' -NotePropertyValue $batchIndex -Force
        } else {
            $item.batch_index = $batchIndex
        }
        $counter++
    }

    return $allItems
}

# ---------------------------------------------------------------------------
# Utility: corr phase（呼叫 scripts\p2\factor_corr.py）
# ---------------------------------------------------------------------------
function Invoke-FactorCorr {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PythonExePath,
        [Parameter(Mandatory = $true)]
        [string]$RootPath,
        [Parameter(Mandatory = $true)]
        [string]$RulesPath,
        [Parameter(Mandatory = $true)]
        [string]$AsOfDate,
        [Parameter(Mandatory = $true)]
        [string]$Profile,
        [Parameter(Mandatory = $true)]
        [string]$Engine,
        [Parameter(Mandatory = $true)]
        [int[]]$WfWindows
    )

    # 如果沒有 window，就沒什麼好算
    if (-not $WfWindows -or $WfWindows.Count -eq 0) {
        Write-Phase2Info "Invoke-FactorCorr: WfWindows is empty → corr phase skipped."
        return
    }

    $corrScript = Join-Path $RootPath 'scripts\p2\factor_corr.py'
    if (-not (Test-Path -LiteralPath $corrScript)) {
        Write-Phase2Warn "factor_corr.py not found at $corrScript; corr phase skipped."
        return
    }

    # Engine='all' 暫時以 classic 為 corr 視角
    $corrEngine = if ($Engine -eq 'all') { 'classic' } else { $Engine }
    $windowsArg = ($WfWindows -join ',')

    Write-Phase2Info ("Starting corr phase via factor_corr.py (engine={0}, windows={1})" -f $corrEngine, $windowsArg)

    $args = @(
        '--root',         $RootPath,
        '--rules',        $RulesPath,
        '--as-of',        $AsOfDate,
        '--windows',      $windowsArg,
        '--engine',       $corrEngine,
        '--profile',      $Profile,
        '--panel-source', 'factor_parquet',
        '--log-level',    'INFO'
    )

    $null = Invoke-PythonTool -PythonExePath $PythonExePath `
                              -ScriptPath  $corrScript `
                              -Arguments   $args
}

# ---------------------------------------------------------------------------
# A 段：計畫（Plan）
# ---------------------------------------------------------------------------
Set-Location -LiteralPath $Root
Write-Phase2Info "Run-Phase2-OneClick (A-segment: plan)"
Write-Phase2Info "Root=$Root Date=$Date Profile=$Profile Mode(raw)=$Mode Engine=$Engine CorrMode=$CorrMode"

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
# 呼叫 factor_status.py 產生狀態 JSON（提供給 factor_plan_lib）
# ---------------------------------------------------------------------------
$statusScript   = Join-Path $Root 'scripts\p2\factor_status.py'
$statusJsonPath = Join-Path $reportsDir ("factor_status.{0}.json" -f $Date)

$null = Invoke-PythonTool -PythonExePath $PythonExe `
                          -ScriptPath  $statusScript `
                          -Arguments   @(
                              '--root',         $Root,
                              '--date',         $Date,
                              '--profile',      $effectiveProfile,
                              '--engine',       $Engine,
                              '--expect-date',  $Date,
                              '--window-months','24',
                              '--rules',        $RulesPath,
                              '--output',       $statusJsonPath,
                              '--log-level',    'INFO'
                          )

if (-not (Test-Path -LiteralPath $statusJsonPath)) {
    throw "factor_status.py did not produce expected JSON file: $statusJsonPath"
}
Write-Phase2Info "factor_status JSON written to $statusJsonPath"

# ---------------------------------------------------------------------------
# 呼叫 factor_plan_lib.py 建立因子計畫（Python SSOT）
# ---------------------------------------------------------------------------
$planItems = New-FactorPlan `
    -Date               $Date `
    -Profile            $effectiveProfile `
    -Engine             $Engine `
    -Root               $Root `
    -RulesPath          $RulesPath `
    -StatusJsonPath     $statusJsonPath `
    -PythonExe          $PythonExe `
    -MaxFactorsPerBatch $MaxFactorsPerBatch `
    -WfWindows          $wfWindows

$Global:Phase2Plan = $planItems

# 統計摘要
if ($planItems -and $planItems.Count -gt 0) {
    $byAction = $planItems | Group-Object -Property decided_action | Sort-Object -Property Name
    foreach ($g in $byAction) {
        Write-Phase2Info ("Plan action {0,-12}: {1,4} factors" -f $g.Name, $g.Count)
    }
} else {
    Write-Phase2Info "Plan contains 0 factors for Engine=$Engine (all filtered or no plan items)."
}

$activeItems = @()
if ($planItems) {
    $activeItems = $planItems | Where-Object { $_.decided_action -in @('compute+eval','eval_only') }
}
$activeCount = if ($activeItems) { $activeItems.Count } else { 0 }
Write-Phase2Info "Active factors (compute+eval / eval_only): $activeCount"

if ($activeCount -eq 0) {
    Write-Phase2Info "No active factors → engine/eval phases will be skipped (但仍可執行 ComposeToWF/SLO)。"
}

# 提示 factor_plan JSON 位置（Python 已輸出）
if ($DumpPlan.IsPresent) {
    if ($Engine -eq 'all') {
        $engList = @('classic','ai')
    } else {
        $engList = @($Engine)
    }
    foreach ($e in $engList) {
        $p = Join-Path $reportsDir ("factor_plan.{0}.{1}.json" -f $Date, $e)
        if (Test-Path -LiteralPath $p) {
            Write-Phase2Info "Factor plan JSON available at $p"
        }
    }
}

# ---------------------------------------------------------------------------
# B 段：Execution segment
# 依照 A 段產生的 $Global:Phase2Plan，實際執行
# 1) engine (compute parquet)
# 2) eval (factor_eval)
# 3) corr phase（可選）
# 4) compose_factors_to_wf + factor_slo
# 5) AutoGate（可選）
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
        [object[]]$PlanItems
    )

    # 空計畫直接跳過
    if (-not $PlanItems -or $PlanItems.Count -eq 0) {
        Write-Phase2Info "Invoke-FactorEngineBatches: PlanItems is empty → engine phase skipped."
        return
    }

    $engineScript = Join-Path $RootPath 'scripts\p2\factor_engine.py'

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
        [object[]]$PlanItems
    )

    $evalScript = Join-Path $RootPath 'scripts\p2\factor_eval.py'

    if (-not (Test-Path -LiteralPath $evalScript)) {
        Write-Phase2Warn "factor_eval.py not found at $evalScript; eval phase skipped."
        return
    }

    if (-not $PlanItems -or $PlanItems.Count -eq 0) {
        Write-Phase2Info "Invoke-FactorEval: PlanItems is empty → eval phase skipped."
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

    $factorIds  = $active | Select-Object -ExpandProperty factor_id -Unique
    $windowsArg = ($WfWindows -join ',')   # 關鍵：組成單一參數 "6,12"

    Write-Phase2Info ("Eval phase: {0} factors → {1} (windows={2})" -f `
        $factorIds.Count, ($factorIds -join ','), $windowsArg)

    foreach ($fid in $factorIds) {
        if (-not $fid) { continue }

        $args = @(
            '--root',      $RootPath,
            '--factor-id', $fid,
            '--windows',   $windowsArg,
            '--as-of',     $AsOfDate,
            '--log-level', 'INFO'
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

    $profile = $slo.profile
    $engine  = $slo.engine
    $windows = if ($slo.windows) { ($slo.windows -join ',') } else { 'n/a' }

    # per_window_counts 可能是 PSCustomObject，要用 PSObject.Properties 來枚舉
    $counts = 'n/a'
    if ($slo.per_window_counts) {
        $props = $slo.per_window_counts.PSObject.Properties
        if ($props) {
            $counts = ($props |
                Sort-Object -Property Name |
                ForEach-Object { "{0}:{1}" -f $_.Name, $_.Value }) -join ' '
        }
    }

    $missing = ''
    if ($slo.missing_required_factors) {
        $missing = ($slo.missing_required_factors -join ',')
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
    Write-Phase2Info "Mode=dryrun → planning only. No engine/eval/compose/corr will be executed."
    Write-Phase2Info "Run-Phase2-OneClick (A-segment only) completed."
    exit 0
}

# 保險：若 Global Plan 為 $null，改成空陣列但不要提早 exit
if (-not $Global:Phase2Plan) {
    Write-Phase2Warn "Global Phase2Plan is null/empty; engine/eval phases will be skipped."
    $Global:Phase2Plan = @()
}

$planItems   = $Global:Phase2Plan
$activeItems = $planItems | Where-Object { $_.decided_action -in @('compute+eval','eval_only') }
$activeCount = if ($activeItems) { $activeItems.Count } else { 0 }

Write-Phase2Info ("Execution segment starting for {0} active factors." -f $activeCount)

if ($activeCount -eq 0) {
    Write-Phase2Info "No active factors → engine/eval/corr phases skipped；僅執行 ComposeToWF / AutoGate（若有指定）。"
} else {
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
    # 3) Corr phase（可選）
    # ----------------------
    $runCorr = $false
    switch ($CorrMode) {
        'on'  { $runCorr = $true }
        'off' { $runCorr = $false }
        'auto' {
            # auto 規則：有 active 因子才跑；dev/profile 可用 CorrMode=off 關閉
            if ($activeCount -gt 0 -and $effectiveProfile -ne 'dev') {
                $runCorr = $true
            } else {
                $runCorr = $false
            }
        }
    }

    if ($runCorr) {
        Invoke-FactorCorr -PythonExePath $PythonExe `
                          -RootPath     $Root `
                          -RulesPath    $RulesPath `
                          -AsOfDate     $Date `
                          -Profile      $effectiveProfile `
                          -Engine       $Engine `
                          -WfWindows    $wfWindows
    } else {
        Write-Phase2Info ("Corr phase skipped (CorrMode={0}, activeCount={1}, profile={2})." -f $CorrMode, $activeCount, $effectiveProfile)
    }
}

# ----------------------
# 4) Compose to wf_summary + factor_slo
# ----------------------
if ($ComposeToWF.IsPresent) {
    Write-Phase2Info "ComposeToWF enabled → composing factor_eval into wf_summary.json + factor_slo."

    # SLO engine 不接受 'all'，預設用 'classic' 作為合併視角
    $sloEngine = if ($Engine -eq 'all') { 'classic' } else { $Engine }

    $wfSummaryPath = Invoke-ComposeFactorsToWF -PythonExePath $PythonExe `
                                               -RootPath     $Root `
                                               -RulesPath    $RulesPath `
                                               -WfWindows    $wfWindows `
                                               -SloProfile   $effectiveProfile `
                                               -SloEngine    $sloEngine

    Show-FactorSLOFromWfSummary -WfSummaryPath $wfSummaryPath
} else {
    Write-Phase2Info "ComposeToWF not specified → skip compose_factors_to_wf."
}

# ----------------------
# 5) AutoGate (optional)
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

