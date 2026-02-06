param(
    [Parameter(Mandatory = $true)]
    [string]$Date,                                        # 驗收日 / as-of（YYYY-MM-DD，通常是 W-FRI）

    [ValidateSet('classic', 'ai', 'all')]
    [string]$Engine = 'classic',                          # 要跑哪個因子家族（依 rules_factors.yaml 的 category）

    [ValidateSet('dryrun', 'evalonly', 'commit')]
    [string]$Mode = 'commit',                             # 預設 commit；未明確指定時會依 Profile 自動推 effective mode

    [string]$Profile,                                     # A/B 測試用：對應 rules_factors.<Profile>.yaml；空白時用 rules_factors.yaml

    [string]$Root = ".",                                  # repo 根目錄（預設目前目錄）

    [string]$PythonExe = ".\.venv\Scripts\python.exe",    # Python 執行路徑

    [string]$ImplModule = "tools.factors.eval.factor_impl", # Python 因子實作模組，可由外部指定

    [int]$MaxFactors = 0,                                 # >0 則分批執行 factor_engine，一批最多 MaxFactors 個因子；0 表示不切批

    [int]$MaxBatches = 0,                                 # >0 則限制本次最多執行幾個 batch；0 表示不限制

    [int]$StopAfterFactors = 0,                           # >0 則只處理前 N 個因子（排序後）；0 表示全跑

    [int]$BatchSleepSeconds = 0,                          # 分批之間休息秒數，避免 CPU / I/O 尖峰；0 表示不休息

    [int[]]$WfWindows,                                    # 若未指定，依 Profile 自動決定預設視窗

    [switch]$FactorsOnly,                                 # true = compose_factors_to_wf 用 factors_only 模式（不寫 factor_candidates）

    [switch]$ComposeToWF,                                 # Mode=commit 時，是否自動把 factor_eval 結果合併進 wf_summary.json.factors

    [switch]$DumpPlan,                                    # true = 只輸出計畫（factor/batch/視窗/compose 模式與批次），不執行 engine/eval

    [switch]$OutputJson,                                  # 搭配 DumpPlan：輸出 JSON 計畫檔（預設 reports\factor_plan.<Date>.json 或 OutputPath）

    [switch]$OutputCsv,                                   # 搭配 DumpPlan：輸出 CSV 計畫檔（預設 reports\factor_plan.<Date>.csv 或 OutputPath）

    [string]$OutputPath,                                  # 搭配 DumpPlan：自訂輸出檔名（副檔名決定 JSON/CSV）

    [int]$MinGateFactors = 0,                             # gate-ready：wf_summary.factors 總數門檻；未指定時由 rules_factors.yaml 或 Profile 推預設

    [int]$MinFactorsPerWindow = 0,                        # gate-ready：每個 window 至少需要的因子數（0=關閉）

    [string[]]$RequiredFactors,                           # gate-ready：必須存在的關鍵因子清單（可空）

    [switch]$AutoGate,                                    # true = gate-ready OK + Mode=commit + ComposeToWF 時自動呼叫 Run-WFGate.ps1 -ShowOnly

    [string]$GateScriptPath = ".\tools\gate\Run-WFGate.ps1", # Gate 唯一入口

    [string]$GateWFDir = ".\tools\gate\wf_configs",       # Gate WF 設定目錄

    [ValidateSet('safe','formal')]
    [string]$GateMode = 'safe',                           # AutoGate 時 Gate 的 Mode

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

function Get-DefaultWfWindowsForProfile {
    param([string]$Profile)

    # 依 Profile 給預設視窗；之後需要調整只要改這裡即可
    if ([string]::IsNullOrWhiteSpace($Profile)) {
        return @(6, 12, 24)
    }

    $key = $Profile.ToLowerInvariant()
    switch ($key) {
        'live' { return @(6, 12, 24) }
        'prod' { return @(6, 12, 24) }
        'dev'  { return @(6, 12) }      # 開發環境：視窗略短
        'test' { return @(3, 6) }       # 測試環境：只看短窗
        default { return @(6, 12, 24) }
    }
}

function Get-DefaultMinGateFactorsForProfile {
    param([string]$Profile)

    if ([string]::IsNullOrWhiteSpace($Profile)) {
        return 0   # 預設不強制 gate-ready 門檻，由使用者明確指定
    }

    $key = $Profile.ToLowerInvariant()
    switch ($key) {
        'live' { return 20 }
        'prod' { return 20 }
        'dev'  { return 5 }
        'test' { return 1 }
        default { return 0 }
    }
}

function Get-GateReadySloFromRules {
    param(
        [string]$RulesPath,
        [string]$Profile
    )

    # 這裡只用於讀 SLO，不要求一定存在；若 ConvertFrom-Yaml 不可用則直接略過
    $yamlCmd = Get-Command -Name 'ConvertFrom-Yaml' -ErrorAction SilentlyContinue
    if (-not $yamlCmd) {
        return $null
    }

    try {
        $yamlText = Get-Content $RulesPath -Raw -Encoding UTF8
        if ([string]::IsNullOrWhiteSpace($yamlText)) {
            return $null
        }
        $doc = $yamlText | ConvertFrom-Yaml
    }
    catch {
        return $null
    }

    if (-not $doc -or -not ($doc.PSObject.Properties.Name -contains 'gate_ready')) {
        return $null
    }

    $gateNode = $doc.gate_ready
    if (-not $gateNode) { return $null }

    $profileKey = if ([string]::IsNullOrWhiteSpace($Profile)) { "" } else { $Profile.ToLowerInvariant() }

    $sloNode = $null

    # 優先 gate_ready.profiles.<profile>
    if ($profileKey -ne "" -and $gateNode.PSObject.Properties.Name -contains 'profiles') {
        $profilesNode = $gateNode.profiles
        if ($profilesNode -and $profilesNode.PSObject.Properties.Name -contains $profileKey) {
            $sloNode = $profilesNode.($profileKey)
        }
    }

    # 次選 gate_ready.<profile>
    if (-not $sloNode -and $profileKey -ne "" -and $gateNode.PSObject.Properties.Name -contains $profileKey) {
        $sloNode = $gateNode.($profileKey)
    }

    # 最後 fallback 到 gate_ready root
    if (-not $sloNode) {
        $sloNode = $gateNode
    }
    if (-not $sloNode) { return $null }

    $minF   = 0
    $minPer = 0
    [string[]]$reqFactors = @()

    if ($sloNode.PSObject.Properties.Name -contains 'min_factors') {
        $minF = [int]$sloNode.min_factors
    }
    if ($sloNode.PSObject.Properties.Name -contains 'min_factors_per_window') {
        $minPer = [int]$sloNode.min_factors_per_window
    }
    if ($sloNode.PSObject.Properties.Name -contains 'required_factors') {
        $vals = $sloNode.required_factors
        if ($vals -is [System.Collections.IEnumerable]) {
            foreach ($v in $vals) {
                if ($null -ne $v -and -not [string]::IsNullOrWhiteSpace($v)) {
                    $reqFactors += [string]$v
                }
            }
        } elseif ($vals) {
            $reqFactors += [string]$vals
        }
    }

    return [PSCustomObject]@{
        MinFactors      = $minF
        MinPerWindow    = $minPer
        RequiredFactors = $reqFactors
    }
}

function Test-GateReady {
    param(
        [string]$WfSummaryPath,
        [int]$MinFactors,
        [int]$MinPerWindow,
        [string[]]$RequiredFactors,
        [int[]]$WfWindows
    )

    if ($MinFactors -le 0 -and $MinPerWindow -le 0 -and (-not $RequiredFactors -or $RequiredFactors.Count -eq 0)) {
        # 完全沒有 gate-ready 條件，視為 PASS
        return $true
    }

    if (-not (Test-Path $WfSummaryPath)) {
        Write-Warning "[gate-ready] wf_summary.json not found at $WfSummaryPath"
        return $false
    }

    try {
        $jsonText = Get-Content $WfSummaryPath -Raw -Encoding UTF8
        if ([string]::IsNullOrWhiteSpace($jsonText)) {
            Write-Warning "[gate-ready] wf_summary.json is empty"
            return $false
        }
        $wf = $jsonText | ConvertFrom-Json
    }
    catch {
        Write-Warning "[gate-ready] failed to parse wf_summary.json: $($_.Exception.Message)"
        return $false
    }

    # 抽出 factors 區塊
    $factorsNode = $null
    if ($wf -and $wf.PSObject.Properties.Name -contains 'factors') {
        $factorsNode = $wf.factors
    }

    if (-not $factorsNode) {
        Write-Warning "[gate-ready] wf_summary has no 'factors' section."
        return $false
    }

    # 將 factorsNode 正規化成：factor_id -> object
    $factorMap = @{}
    if ($factorsNode -is [System.Collections.IDictionary]) {
        foreach ($key in $factorsNode.Keys) {
            $factorMap[[string]$key] = $factorsNode[$key]
        }
    }
    elseif ($factorsNode -is [System.Collections.IEnumerable]) {
        foreach ($item in $factorsNode) {
            if (-not $item) { continue }
            $fid = $null
            if ($item.PSObject.Properties.Name -contains 'factor_id') {
                $fid = [string]$item.factor_id
            } elseif ($item.PSObject.Properties.Name -contains 'id') {
                $fid = [string]$item.id
            }
            if ([string]::IsNullOrWhiteSpace($fid)) { continue }
            $factorMap[$fid] = $item
        }
    }

    $factorIds = $factorMap.Keys
    $totalCount = $factorIds.Count

    # 條件一：總因子數
    if ($MinFactors -gt 0 -and $totalCount -lt $MinFactors) {
        Write-Warning "[gate-ready] total factors=$totalCount < MinGateFactors=$MinFactors"
        $totalOk = $false
    } else {
        $totalOk = $true
        if ($MinFactors -gt 0) {
            Write-Info "[gate-ready] total factors=$totalCount ≥ MinGateFactors=$MinFactors"
        } else {
            Write-Info "[gate-ready] total factors=$totalCount (no MinGateFactors constraint)"
        }
    }

    # 條件二：每個 window 至少 M 個因子
    $windowOk = $true
    $perWindowCounts = @{}
    if ($MinPerWindow -gt 0 -and $WfWindows -and $WfWindows.Count -gt 0) {
        foreach ($w in $WfWindows) {
            $perWindowCounts[$w] = 0
        }

        foreach ($fid in $factorIds) {
            $fv = $factorMap[$fid]
            foreach ($w in $WfWindows) {
                $hasWindow = $false

                # 嘗試找出 per-window 結構；若格式不同則退化為「每個因子對所有 window 都算一個」
                if ($fv -and $fv.PSObject.Properties.Name -contains 'windows') {
                    $winNode = $fv.windows
                    if ($winNode -is [System.Collections.IDictionary]) {
                        if ($winNode.Contains($w.ToString())) { $hasWindow = $true }
                    } elseif ($winNode -is [System.Collections.IEnumerable]) {
                        foreach ($wn in $winNode) {
                            if ($wn -eq $w -or $wn -eq $w.ToString()) { $hasWindow = $true; break }
                        }
                    }
                }

                if (-not $hasWindow) {
                    # schema 不清楚時，保守地把因子算進所有 window，以免誤殺
                    $hasWindow = $true
                }

                if ($hasWindow) {
                    $perWindowCounts[$w]++
                }
            }
        }

        foreach ($w in $WfWindows) {
            $cnt = $perWindowCounts[$w]
            if ($cnt -lt $MinPerWindow) {
                Write-Warning "[gate-ready] window=${w}m factors=$cnt < MinFactorsPerWindow=$MinPerWindow"
                $windowOk = $false
            } else {
                Write-Info "[gate-ready] window=${w}m factors=$cnt ≥ MinFactorsPerWindow=$MinPerWindow"
            }
        }
    } else {
        Write-Info "[gate-ready] MinFactorsPerWindow=0 or no WfWindows指定，略過 per-window 檢查。"
    }

    # 條件三：RequiredFactors 都必須存在
    $requiredOk = $true
    if ($RequiredFactors -and $RequiredFactors.Count -gt 0) {
        $missing = @()
        foreach ($rf in $RequiredFactors) {
            if (-not ($factorIds -contains $rf)) {
                $missing += $rf
            }
        }

        if ($missing.Count -gt 0) {
            $requiredOk = $false
            Write-Warning "[gate-ready] missing required factors: $($missing -join ', ')"
        } else {
            Write-Info "[gate-ready] all required factors present: $($RequiredFactors -join ', ')"
        }
    }

    if ($totalOk -and $windowOk -and $requiredOk) {
        Write-Info "[gate-ready] all conditions satisfied."
        return $true
    }

    return $false
}

# -----------------------------------------------------------------------------
# 主流程
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
    $factorEvalDir    = Join-Path $reportsDir "factor_eval"
    $wfSummaryPath    = Join-Path $reportsDir "wf_summary.json"
    $planJsonPath     = Join-Path $reportsDir ("factor_plan.{0}.json"     -f $Date)
    $planCsvPath      = Join-Path $reportsDir ("factor_plan.{0}.csv"      -f $Date)

    if (-not (Test-Path $reportsDir)) {
        New-Item -ItemType Directory -Path $reportsDir | Out-Null
    }

    # 決定實際使用的 WF 視窗（若未指定則依 Profile 給預設）
    [int[]]$effectiveWfWindows = $WfWindows
    if (-not $effectiveWfWindows -or $effectiveWfWindows.Count -eq 0) {
        $effectiveWfWindows = Get-DefaultWfWindowsForProfile -Profile $Profile
    }

    $composeMode = if ($FactorsOnly) { 'factors_only' } else { 'all' }

    # 依 Profile 推出 effective Mode（若使用者沒明確指定 -Mode）
    $effectiveMode = $Mode
    if (-not $PSBoundParameters.ContainsKey('Mode')) {
        $profKey = if ([string]::IsNullOrWhiteSpace($Profile)) { "" } else { $Profile.ToLowerInvariant() }
        switch ($profKey) {
            'dev'  { $effectiveMode = 'evalonly' }
            'test' { $effectiveMode = 'dryrun' }
            default { $effectiveMode = 'commit' }
        }
    }

    # 從 rules_factors.yaml 讀 gate-ready SLO（如果有）
    $gateSloFromRules = Get-GateReadySloFromRules -RulesPath $rulesPath -Profile $Profile

    # 依 Profile + rules_factors.yaml 決定 effective MinGateFactors / MinFactorsPerWindow / RequiredFactors
    [int]$effectiveMinGateFactors = $MinGateFactors
    if (-not $PSBoundParameters.ContainsKey('MinGateFactors')) {
        if ($gateSloFromRules -and $gateSloFromRules.MinFactors -gt 0) {
            $effectiveMinGateFactors = [int]$gateSloFromRules.MinFactors
        } else {
            $effectiveMinGateFactors = Get-DefaultMinGateFactorsForProfile -Profile $Profile
        }
    }

    [int]$effectiveMinPerWindow = $MinFactorsPerWindow
    if (-not $PSBoundParameters.ContainsKey('MinFactorsPerWindow')) {
        if ($gateSloFromRules -and $gateSloFromRules.MinPerWindow -gt 0) {
            $effectiveMinPerWindow = [int]$gateSloFromRules.MinPerWindow
        }
    }

    [string[]]$effectiveRequiredFactors = $RequiredFactors
    if (-not $PSBoundParameters.ContainsKey('RequiredFactors') -and $gateSloFromRules -and $gateSloFromRules.RequiredFactors) {
        $effectiveRequiredFactors = $gateSloFromRules.RequiredFactors
    }

    Write-Info "Root          : $rootPath"
    Write-Info "Date          : $Date"
    Write-Info "Engine        : $Engine"
    Write-Info "Mode(param)   : $Mode"
    if ($effectiveMode -ne $Mode) {
        Write-Info "Mode(effective): $effectiveMode (derived from Profile '$Profile')"
    } else {
        Write-Info "Mode(effective): $effectiveMode"
    }
    Write-Info "Profile       : $Profile"
    Write-Info "Rules file    : $rulesPath"
    Write-Info "Python        : $PythonExe"
    Write-Info "ImplModule    : $ImplModule"
    Write-Info "WfWindows     : $($effectiveWfWindows -join ',')"
    Write-Info "Compose mode  : $composeMode"
    Write-Info "MinGateFact(param) : $MinGateFactors"
    Write-Info "MinGateFact(eff)   : $effectiveMinGateFactors"
    Write-Info "MinFactorsPerWindow: $effectiveMinPerWindow"
    if ($effectiveRequiredFactors -and $effectiveRequiredFactors.Count -gt 0) {
        Write-Info "RequiredFactors(eff): $($effectiveRequiredFactors -join ', ')"
    } else {
        Write-Info "RequiredFactors(eff): (none)"
    }
    if ($MaxFactors -gt 0) {
        Write-Info "MaxFactors    : $MaxFactors (factor_engine will run in batches)"
    }
    if ($MaxBatches -gt 0) {
        Write-Info "MaxBatches    : $MaxBatches (this run will execute at most this many batches)"
    }
    if ($StopAfterFactors -gt 0) {
        Write-Info "StopAfterFact.: $StopAfterFactors (only first N factors will be processed)"
    }
    if ($BatchSleepSeconds -gt 0) {
        Write-Info "BatchSleep    : $BatchSleepSeconds seconds between batches"
    }
    if ($DumpPlan) {
        Write-Info "DumpPlan      : enabled (no engine/eval will be executed)"
        if ($OutputPath) {
            Write-Info "DumpPlan OutputPath : $OutputPath"
        } else {
            if ($OutputJson) { Write-Info "DumpPlan JSON : default $planJsonPath" }
            if ($OutputCsv)  { Write-Info "DumpPlan CSV  : default $planCsvPath" }
        }
    }
    if ($AutoGate) {
        Write-Info "AutoGate      : enabled (GateScriptPath=$GateScriptPath, GateMode=$GateMode)"
    }

    # -----------------------------------------------------------------------------
    # 1. 載入 factor registry（做 category / owner 等索引）
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

    # 建立 factor_id -> factor 設定索引
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
    # 2. 跑 factor_status.py，產出統一狀態 JSON
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

    # 建立 factor_id -> status 索引（for DumpPlan / logging）
    $statusIndex = @{}
    foreach ($s in $statusObjects) {
        $fid = [string]$s.factor_id
        if ([string]::IsNullOrWhiteSpace($fid)) { continue }
        $statusIndex[$fid] = $s
    }

    # -----------------------------------------------------------------------------
    # 3. 根據 required_action + Engine 篩出要跑的因子
    # -----------------------------------------------------------------------------

    $candidates = @()
    foreach ($s in $statusObjects) {
        # 只考慮 registry 有列出的因子
        if (-not $s.in_registry) { continue }

        $action = [string]$s.required_action
        if ($action -notin @('missing', 'rebuild')) { continue }

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

        $candidates += $fid
    }

    $targetFactors = $candidates | Sort-Object -Unique

    if ($targetFactors.Count -eq 0) {
        Write-Info "No factors require action for Engine='$Engine' on Date=$Date. Nothing to do."
        return 0
    }

    # 若指定 StopAfterFactors，截斷因子清單
    if ($StopAfterFactors -gt 0 -and $targetFactors.Count -gt $StopAfterFactors) {
        $targetFactors = $targetFactors[0..($StopAfterFactors - 1)]
    }

    Write-Info "Target factors (after filters): $($targetFactors.Count)"

    if (-not $Quiet) {
        foreach ($fid in $targetFactors) {
            $cat = $registryIndex[$fid].category
            $act = if ($statusIndex.ContainsKey($fid)) { [string]$statusIndex[$fid].required_action } else { '' }
            Write-Host "  - $fid (category=$cat, action=$act)"
        }
    }

    # -----------------------------------------------------------------------------
    # 4. 準備分批（建立 $batches，但此時尚未執行 engine）
    # -----------------------------------------------------------------------------

    $batches = @()
    if ($MaxFactors -gt 0 -and $targetFactors.Count -gt $MaxFactors) {
        for ($i = 0; $i -lt $targetFactors.Count; $i += $MaxFactors) {
            $end = [Math]::Min($i + $MaxFactors - 1, $targetFactors.Count - 1)
            $slice = $targetFactors[$i..$end]
            $batches += ,$slice
        }
    } else {
        $batches = ,$targetFactors
    }

    # 若指定 MaxBatches，限制本次最多的 batch 數
    if ($MaxBatches -gt 0 -and $batches.Count -gt $MaxBatches) {
        $batches = $batches[0..($MaxBatches - 1)]
    }

    $batchCount = $batches.Count

    Write-Info "Planned batches: $batchCount"

    # 預估 Gate 建議結論（expected_gate_ready）：用「計畫中的因子數＋RequiredFactors 是否都在」做 smoke
    $expectedGateReady = $true
    if ($effectiveMinGateFactors -gt 0 -and $targetFactors.Count -lt $effectiveMinGateFactors) {
        $expectedGateReady = $false
    }
    if ($effectiveRequiredFactors -and $effectiveRequiredFactors.Count -gt 0) {
        $missingPlan = @()
        foreach ($rf in $effectiveRequiredFactors) {
            if (-not ($targetFactors -contains $rf)) {
                $missingPlan += $rf
            }
        }
        if ($missingPlan.Count -gt 0) {
            $expectedGateReady = $false
        }
    }

    # -----------------------------------------------------------------------------
    # 5. DumpPlan 模式：只輸出計畫，不執行 engine / eval / compose
    # -----------------------------------------------------------------------------

    if ($DumpPlan) {
        $planItems = @()
        $idx = 0
        foreach ($batch in $batches) {
            $idx++
            foreach ($fid in $batch) {
                $cfg = $registryIndex[$fid]
                $status = $null
                if ($statusIndex.ContainsKey($fid)) { $status = $statusIndex[$fid] }
                $requiredAction = if ($status) { [string]$status.required_action } else { '' }
                $cat = [string]$cfg.category

                $item = [PSCustomObject]@{
                    date                    = $Date
                    factor_id               = $fid
                    category                = $cat
                    required_action         = $requiredAction
                    batch_index             = $idx
                    batch_count             = $batchCount
                    engine                  = $Engine
                    profile                 = $Profile
                    wf_windows              = ($effectiveWfWindows -join ',')
                    compose_mode            = $composeMode
                    max_factors             = $MaxFactors
                    max_batches             = $MaxBatches
                    stop_after              = $StopAfterFactors
                    mode_effective          = $effectiveMode
                    min_gate_factors        = $effectiveMinGateFactors
                    min_factors_per_window  = $effectiveMinPerWindow
                    required_factors        = ($effectiveRequiredFactors -join ',')
                    expected_gate_ready     = $expectedGateReady
                }
                $planItems += $item
            }
        }

        Write-Info "DumpPlan: $($planItems.Count) factor-batch item(s) across $batchCount batch(es)."
        if (-not $Quiet) {
            $planItems | Format-Table batch_index,factor_id,category,required_action,wf_windows,compose_mode,mode_effective,expected_gate_ready -AutoSize
            Write-Host ""
        }

        # 決定輸出檔案（JSON / CSV / OutputPath）
        if ($OutputPath) {
            $target = $OutputPath
            # 相對路徑 → 視為 reports 下的檔案
            if (-not [System.IO.Path]::IsPathRooted($target)) {
                $target = Join-Path $reportsDir $target
            }
            $ext = [System.IO.Path]::GetExtension($target).ToLowerInvariant()
            if ($ext -eq ".csv") {
                $planItems | Export-Csv -Path $target -Encoding UTF8 -NoTypeInformation
                Write-Info "DumpPlan CSV written to: $target"
            } else {
                $json = $planItems | ConvertTo-Json -Depth 5
                $json | Set-Content -Path $target -Encoding UTF8
                Write-Info "DumpPlan JSON written to: $target"
            }
        } else {
            if ($OutputJson) {
                $json = $planItems | ConvertTo-Json -Depth 5
                $json | Set-Content -Path $planJsonPath -Encoding UTF8
                Write-Info "DumpPlan JSON written to: $planJsonPath"
            }
            if ($OutputCsv) {
                $planItems | Export-Csv -Path $planCsvPath -Encoding UTF8 -NoTypeInformation
                Write-Info "DumpPlan CSV written to: $planCsvPath"
            }
        }

        # 將計畫物件丟到 pipeline，方便外部 Export-Csv / ConvertTo-Json 等操作
        $planItems
        return 0
    }

    # -----------------------------------------------------------------------------
    # 6. 若 Mode=dryrun（effective）：只看 status，不執行 engine / eval
    # -----------------------------------------------------------------------------

    if ($effectiveMode -eq 'dryrun') {
        Write-Info "[Mode=dryrun] Only status inspection done. No engine / eval invoked."
        return 0
    }

    # -----------------------------------------------------------------------------
    # 7. 呼叫 factor_engine.py（支援 impl-module + MaxFactors 分批 + BatchSleep）
    # -----------------------------------------------------------------------------

    if (-not (Test-Path $factorEvalDir)) {
        New-Item -ItemType Directory -Path $factorEvalDir | Out-Null
    }

    $runIdPrefix = if ([string]::IsNullOrWhiteSpace($Profile)) { "factor" } else { "factor_$Profile" }

    $batchIndex = 0
    foreach ($batch in $batches) {
        $batchIndex++
        $factorList = [string]::Join(',', $batch)

        $engineArgs = @(
            ".\scripts\p2\factor_engine.py",
            "--root", $rootPath,
            "--impl-module", $ImplModule,
            "--rules", $rulesPath,
            "--factors", $factorList,
            "--run-id-prefix", $runIdPrefix,
            "--log-level", ($Quiet ? "WARNING" : "INFO")
        )

        if ($effectiveMode -eq 'evalonly') {
            # evalonly = 不寫 parquet，只做計算與 schema 檢查
            $engineArgs += "--dry-run"
        }

        $stepName = if ($batchCount -gt 1) {
            "factor_engine[$batchIndex/$batchCount]"
        } else {
            "factor_engine"
        }

        Invoke-Python -PythonExe $PythonExe -Arguments $engineArgs -StepName $stepName

        # 多批次之間可選擇休息幾秒，避免 CPU / I/O 尖峰
        if ($BatchSleepSeconds -gt 0 -and $batchIndex -lt $batchCount) {
            Write-Info "Sleeping $BatchSleepSeconds seconds before next batch..."
            Start-Sleep -Seconds $BatchSleepSeconds
        }
    }

    if ($effectiveMode -eq 'evalonly') {
        Write-Info "[Mode=evalonly] factor_engine dry-run completed. No parquet / ledger written."
        return 0
    }

    # -----------------------------------------------------------------------------
    # 8. Mode=commit：跑 factor_eval.py，更新因子評估骨架（帶 effectiveWfWindows）
    # -----------------------------------------------------------------------------

    if (-not (Test-Path $factorEvalDir)) {
        New-Item -ItemType Directory -Path $factorEvalDir | Out-Null
    }

    $wfWindowArgs = @()
    foreach ($w in $effectiveWfWindows) {
        $wfWindowArgs += $w.ToString()
    }

    $evalArgs = @(
        ".\scripts\p2\factor_eval.py",
        "--root", $rootPath,
        "--rules-file", $rulesPath,
        "--date", $Date,
        "--wf-windows"
    )
    $evalArgs += $wfWindowArgs
    $evalArgs += @(
        "--output", $factorEvalDir,
        "--overwrite"
    )

    Invoke-Python -PythonExe $PythonExe -Arguments $evalArgs -StepName "factor_eval"

    Write-Info "[Mode=commit] factor_engine + factor_eval completed successfully."
    Write-Info "  - factors       : $($targetFactors.Count)"
    Write-Info "  - rules         : $rulesFileName"
    Write-Info "  - status JSON   : $statusJsonPath"
    Write-Info "  - registry JSON : $registryJsonPath"
    Write-Info "  - factor_eval   : $factorEvalDir"

    # -----------------------------------------------------------------------------
    # 9. 可選：ComposeToWF → 呼叫 Python compose_factors_to_wf.py 合併因子結果 + gate-ready 檢查 + AutoGate
    # -----------------------------------------------------------------------------

    if ($ComposeToWF) {
        Write-Info "== [compose] delegating to scripts\compose_factors_to_wf.py"

        $composeArgs = @(
            ".\scripts\compose_factors_to_wf.py",
            "--root", $rootPath,
            "--rules-file", $rulesPath,
            "--wf-summary", $wfSummaryPath,
            "--factor-eval-dir", $factorEvalDir,
            "--wf-windows"
        )
        $composeArgs += $wfWindowArgs
        $composeArgs += @(
            "--mode", $composeMode,
            "--log-level", ($Quiet ? "WARNING" : "INFO")
        )

        Invoke-Python -PythonExe $PythonExe -Arguments $composeArgs -StepName "compose_factors_to_wf"

        Write-Info "[compose] factors composed into wf_summary.json (mode=$composeMode)"

        # gate-ready 檢查：只做預警，不會強制讓整體流程失敗
        $gateReady = Test-GateReady -WfSummaryPath $wfSummaryPath `
                                    -MinFactors $effectiveMinGateFactors `
                                    -MinPerWindow $effectiveMinPerWindow `
                                    -RequiredFactors $effectiveRequiredFactors `
                                    -WfWindows $effectiveWfWindows
        if (-not $gateReady) {
            Write-Warning "[compose/gate-ready] wf_summary.json not gate-ready (see warnings above)."
        }

        # AutoGate：在 Mode=commit 且 gate-ready OK 時，自動呼叫 Run-WFGate.ps1 -ShowOnly
        if ($AutoGate) {
            if ($effectiveMode -ne 'commit') {
                Write-Info "[autogate] requested but effectiveMode=$effectiveMode (only commit supports AutoGate). Skipping Gate."
            }
            elseif (-not $gateReady) {
                Write-Warning "[autogate] gate-ready check failed; AutoGate will not invoke Run-WFGate.ps1."
            }
            else {
                if (-not (Test-Path $GateScriptPath)) {
                    Write-Warning "[autogate] Gate script not found: $GateScriptPath"
                }
                else {
                    Write-Info "== [autogate] invoking Gate script: $GateScriptPath"
                    & $GateScriptPath `
                        -Date $Date `
                        -Mode $GateMode `
                        -WFDir $GateWFDir `
                        -Root $rootPath `
                        -ShowOnly

                    $gateExit = $LASTEXITCODE
                    if ($gateExit -ne 0) {
                        Write-Warning "[autogate] Gate script exited with code $gateExit"
                    } else {
                        Write-Info "[autogate] Gate completed (ShowOnly)."
                    }
                }
            }
        }
    }

    return 0
}
finally {
    # 還原原本工作目錄，避免影響呼叫者
    Set-Location $prevLocation
}

