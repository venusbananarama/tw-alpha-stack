param(
    [Parameter(Mandatory = $true)]
    [string]$Date,                                        # 驗收日 / as-of（YYYY-MM-DD，通常是 W-FRI）

    [ValidateSet('classic', 'ai', 'all')]
    [string]$Engine = 'classic',                          # 要看的因子家族（依 rules_factors.yaml 的 category）

    [string]$Profile,                                     # A/B 測試用：對應 rules_factors.<Profile>.yaml；空白時用 rules_factors.yaml

    [string]$Root = ".",                                  # repo 根目錄（預設目前目錄）

    [string]$PythonExe = ".\.venv\Scripts\python.exe",    # Python 執行路徑（沿用 Phase-2 pipeline）

    [int[]]$WfWindows = @(6, 12, 24),                     # 預計要用的 WF 視窗（月），只影響 plan 中的 metadata

    [switch]$FactorsOnly,                                 # true = compose 模式會用 factors_only（不寫 factor_candidates）

    [switch]$WriteJson,                                   # true 時，會輸出 reports\factor_plan.<Date>.json

    [switch]$Quiet                                        # 靜音模式：只印必要錯誤
)

$ErrorActionPreference = 'Stop'

# -----------------------------------------------------------------------------
# 小工具：集中處理 logging 與 Python 呼叫
# -----------------------------------------------------------------------------

function Write-Info {
    param([string]$Message)
    if (-not $Quiet) {
        Write-Host $Message
    }
}

function Invoke-Python {
    param(
        [string]$PythonExe,
        [string[]]$Arguments,
        [string]$StepName
    )

    Write-Info "== [$StepName] python $($Arguments -join ' ')"
    & $PythonExe @Arguments
    $code = $LASTEXITCODE
    if ($code -ne 0) {
        throw "Step '$StepName' failed with exit code $code"
    }
}

# -----------------------------------------------------------------------------
# 主流程：只做「規劃」，不呼叫 factor_engine / factor_eval
# -----------------------------------------------------------------------------

$prevLocation = Get-Location
try {
    Set-Location $Root
    $rootPath = (Get-Location).ProviderPath

    if (-not (Test-Path $PythonExe)) {
        throw "Python executable not found: $PythonExe"
    }

    # 根據 Profile 決定 rules 檔案名稱
    if ([string]::IsNullOrWhiteSpace($Profile)) {
        $rulesFileName = "rules_factors.yaml"
    } else {
        $rulesFileName = "rules_factors.$Profile.yaml"
    }
    $rulesPath = Join-Path $rootPath $rulesFileName

    if (-not (Test-Path $rulesPath)) {
        throw "rules file not found: $rulesPath"
    }

    $reportsDir       = Join-Path $rootPath "reports"
    $statusJsonPath   = Join-Path $reportsDir ("factor_status.{0}.json"   -f $Date)
    $registryJsonPath = Join-Path $reportsDir ("factor_registry.{0}.json" -f $Date)
    $planJsonPath     = Join-Path $reportsDir ("factor_plan.{0}.json"     -f $Date)

    if (-not (Test-Path $reportsDir)) {
        New-Item -ItemType Directory -Path $reportsDir | Out-Null
    }

    Write-Info "Root       : $rootPath"
    Write-Info "Date       : $Date"
    Write-Info "Engine     : $Engine"
    Write-Info "Profile    : $Profile"
    Write-Info "Rules file : $rulesPath"
    Write-Info "Python     : $PythonExe"
    Write-Info "WfWindows  : $($WfWindows -join ',')"
    if ($FactorsOnly) {
        Write-Info "Compose    : factors_only (wf_summary 只保留通過門檻因子)"
    }

    # -----------------------------------------------------------------------------
    # 1. 載入 factor registry（做 category / gate_rules 索引）
    # -----------------------------------------------------------------------------

    $registryArgs = @(
        ".\tools\factors\factor_registry.py",
        "--root", $rootPath,
        "--rules", $rulesPath,
        "--json",
        "--log-level", "WARNING"
    )

    # 將 stdout 純 JSON 寫到檔案；stderr（logging）丟掉
    Write-Info "== [registry] loading factors from $rulesFileName"
    & $PythonExe @registryArgs 2>$null | Set-Content -Path $registryJsonPath -Encoding UTF8
    if ($LASTEXITCODE -ne 0) {
        throw "factor_registry.py failed with exit code $LASTEXITCODE"
    }

    if (-not (Test-Path $registryJsonPath)) {
        throw "registry JSON not created: $registryJsonPath"
    }

    $registryRaw = Get-Content $registryJsonPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if (-not $registryRaw -or -not $registryRaw.factors) {
        throw "registry JSON has no factors: $registryJsonPath"
    }

    # 建立 factor_id -> registry 設定索引，並預先抽出 gate_rules 簡單摘要
    $registryIndex = @{}
    foreach ($f in $registryRaw.factors) {
        $fid = [string]$f.factor_id
        if ([string]::IsNullOrWhiteSpace($fid)) { continue }
        $registryIndex[$fid] = $f
    }

    # 預先決定 Engine 對應到哪些 category
    $engineCategories = @()
    switch ($Engine) {
        'classic' { $engineCategories = @('classic') }
        'ai'      { $engineCategories = @('ai') }
        'all'     { $engineCategories = @() }  # all = 不過濾
    }

    # -----------------------------------------------------------------------------
    # 2. 跑 factor_status.py，產出統一狀態 JSON（不實際執行 engine）
    # -----------------------------------------------------------------------------

    $statusArgs = @(
        ".\scripts\p2\factor_status.py",
        "--root", $rootPath,
        "--expect-date", $Date,
        "--window-months", "24",           # 以 24 個月視窗為 baseline
        "--rules", $rulesPath,
        "--output-json", $statusJsonPath,
        "--log-level", "INFO"
    )
    if ($Quiet) {
        # 安靜一點，把 log-level 調低
        $statusArgs[$statusArgs.Count - 1] = "WARNING"
    }

    Invoke-Python -PythonExe $PythonExe -Arguments $statusArgs -StepName "factor_status"

    if (-not (Test-Path $statusJsonPath)) {
        throw "status JSON not created: $statusJsonPath"
    }

    $statusRaw = Get-Content $statusJsonPath -Raw -Encoding UTF8
    if ([string]::IsNullOrWhiteSpace($statusRaw)) {
        throw "status JSON is empty: $statusJsonPath"
    }

    $statusObjects = $statusRaw | ConvertFrom-Json
    if ($statusObjects -isnot [System.Array]) {
        $statusObjects = @($statusObjects)
    }

    # -----------------------------------------------------------------------------
    # 3. 根據 required_action + Engine 篩出「計畫中的因子」
    # -----------------------------------------------------------------------------

    $planItems = @()

    foreach ($s in $statusObjects) {
        # 只考慮 registry 有列出的因子
        if (-not $s.in_registry) { continue }

        $requiredAction = [string]$s.required_action
        if ($requiredAction -notin @('missing', 'rebuild')) { continue }

        $fid = [string]$s.factor_id
        if (-not $registryIndex.ContainsKey($fid)) { continue }

        $cfg = $registryIndex[$fid]
        $cat = [string]$cfg.category

        if ($engineCategories.Count -gt 0 -and $engineCategories -notcontains $cat) {
            continue
        }

        $enabled = $true
        if ($cfg.PSObject.Properties.Name -contains 'enabled') {
            $enabled = [bool]$cfg.enabled
        }

        if (-not $enabled) { continue }

        # gate_rules 摘要（min_rank_ic / max_turnover / max_corr / min_coverage）
        $hasGateRules = $false
        $gateMinRankIc  = $null
        $gateMaxTurn    = $null
        $gateMaxCorr    = $null
        $gateMinCov     = $null

        if ($cfg.PSObject.Properties.Name -contains 'gate_rules' -and $cfg.gate_rules) {
            $gr = $cfg.gate_rules
            $props = $gr.PSObject.Properties.Name
            if ($props.Count -gt 0) {
                $hasGateRules = $true
            }
            if ($props -contains 'min_rank_ic')  { $gateMinRankIc = $gr.min_rank_ic }
            if ($props -contains 'max_turnover') { $gateMaxTurn   = $gr.max_turnover }
            if ($props -contains 'max_corr')     { $gateMaxCorr   = $gr.max_corr }
            if ($props -contains 'min_coverage') { $gateMinCov    = $gr.min_coverage }
        }

        $composeMode = if ($FactorsOnly) { 'factors_only' } else { 'all' }

        # 預備成一個計畫項目（PSObject），方便 Format-Table 或輸出 JSON
        $item = [PSCustomObject]@{
            date             = $Date
            factor_id        = $fid
            category         = $cat
            required_action  = $requiredAction
            engine_filter    = $Engine
            enabled          = $enabled
            wf_windows       = ($WfWindows -join ',')
            compose_mode     = $composeMode
            has_gate_rules   = $hasGateRules
            gate_min_rank_ic = $gateMinRankIc
            gate_max_turn    = $gateMaxTurn
            gate_max_corr    = $gateMaxCorr
            gate_min_cov     = $gateMinCov
        }

        $planItems += $item
    }

    $planItems = $planItems | Sort-Object factor_id

    if ($planItems.Count -eq 0) {
        Write-Info "No factors require action for Engine='$Engine' on Date=$Date. Plan is empty."
        return 0
    }

    Write-Info "Planned factors: $($planItems.Count) item(s)."
    if (-not $Quiet) {
        # 讓使用者直接看到重點欄位；仍然把完整物件丟到 pipeline
        $planItems | Format-Table factor_id,category,required_action,wf_windows,compose_mode,has_gate_rules -AutoSize
        Write-Host ""
    }

    # 把計畫物件輸出到 pipeline，方便使用者 | Export-Csv 或 | Out-GridView
    $planItems

    # -----------------------------------------------------------------------------
    # 4. 可選：輸出 JSON 計畫檔（給其他工具消費）
    # -----------------------------------------------------------------------------

    if ($WriteJson) {
        $json = $planItems | ConvertTo-Json -Depth 5
        $json | Set-Content -Path $planJsonPath -Encoding UTF8
        Write-Info "Plan JSON written to: $planJsonPath"
    }

    return 0
}
finally {
    # 還原原本工作目錄，避免影響呼叫者
    Set-Location $prevLocation
}

