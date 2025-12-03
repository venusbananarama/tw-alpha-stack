param(
  [string]$Date,                                   # 驗收日（YYYY-MM-DD），未給則用本地最接近的週五（W-FRI）
  [ValidateSet("safe","formal")]
  [string]$Mode = "safe",                          # safe = PASS/FAIL CSV → Compose → Gate；formal = 已先備妥 wf_summary.json
  [string]$WFDir = ".\tools\gate\wf_configs",
  [string]$Root  = ".",
  [switch]$Quiet,
  [switch]$Pack,                                   # 產出每週驗收證據包（不壓縮）
  [switch]$Zip,                                    # 若同時給 -Pack 則壓成 ZIP
  [switch]$ShowOnly                                # 只顯示結論，不跑 Preflight/Compose/Gate
)

$ErrorActionPreference = "Stop"
Set-Location $Root

# ---------- 共用小工具 ----------

function Get-CurrentWFriday {
  $today = (Get-Date).Date
  $dow   = [int]$today.DayOfWeek  # Sunday=0 … Friday=5
  $back  = (($dow - 5 + 7) % 7)
  return $today.AddDays(-$back).ToString('yyyy-MM-dd')
}

function Read-JsonSafe([string]$Path) {
  if (-not (Test-Path $Path)) { return $null }
  try {
    return Get-Content $Path -Raw | ConvertFrom-Json
  }
  catch {
    Write-Warning "解析 JSON 失敗：$Path - $($_.Exception.Message)"
    return $null
  }
}

function Get-Python {
  $candidates = @(
    ".\.venv\Scripts\python.exe",
    "python.exe",
    "python"
  )
  foreach ($p in $candidates) {
    $cmd = Get-Command $p -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
  }
  throw "找不到 python，可用路徑：$($candidates -join ', ')"
}

function Ensure-Dir([string]$Path) {
  if (-not (Test-Path $Path)) {
    New-Item -ItemType Directory -Path $Path | Out-Null
  }
}

function Get-AsOfDate {
  param(
    [string]$DateParam
  )
  if ($DateParam) { return $DateParam }
  return Get-CurrentWFriday
}

# ---------- Preflight 呼叫 ----------

function Invoke-Preflight([string]$Date, [string]$Root = ".") {

  $py      = Get-Python
  $rules   = Join-Path $Root "rules.yaml"
  $reports = Join-Path $Root "reports"

  Ensure-Dir $reports

  # 鎖定 EXPECT_DATE / EXPECT_DATE_FIXED（半開區間的 Start）
  if ($Date) {
    $env:ALPHACITY_ALLOW   = "1"
    $env:EXPECT_DATE       = $Date
    $env:EXPECT_DATE_FIXED = $Date
    if (-not $Quiet) {
      Write-Host "[Preflight] expect_date_fixed=$Date tz=Asia/Taipei" -ForegroundColor Cyan
    }
  }

  & $py .\scripts\preflight_check.py --rules $rules --export $reports --root .
  if ($LASTEXITCODE -ne 0) {
    throw "preflight_check.py 退出碼 $LASTEXITCODE"
  }

  $preflightPath = Join-Path $reports "preflight_report.json"
  if (-not (Test-Path $preflightPath)) {
    throw "Preflight 未輸出 $preflightPath"
  }

  if (-not $Quiet) {
    Write-Host "[OK] Preflight 完成，已輸出 $preflightPath" -ForegroundColor Green
  }

  return $preflightPath
}

# ---------- SAFE 路線：PASS/FAIL CSV → Compose wf_summary（只更新 pass_rate，不洗掉 factors） ----------

function Compose-WFSummaryFromCsv([string]$Reports = ".\reports") {
  $passPath = Join-Path $Reports "pass_results.csv"
  $failPath = Join-Path $Reports "fail_results.csv"

  if (-not (Test-Path $passPath) -and -not (Test-Path $failPath)) {
    throw "SAFE 路線：找不到 pass_results.csv / fail_results.csv，無法 Compose"
  }

  $passes = @()
  $fails  = @()
  if (Test-Path $passPath) { $passes = Import-Csv $passPath }
  if (Test-Path $failPath) { $fails  = Import-Csv $failPath }

  # 簡易且穩健：若兩檔都存在，以筆數作比例；若只有 pass 檔，視為全過；都沒有則 0
  $total = [double]($passes.Count + $fails.Count)
  $pr    = if ($total -gt 0) {
    [Math]::Min(1.0, $passes.Count / $total)
  } else {
    0.0
  }

  $wfPath      = Join-Path $Reports 'wf_summary.json'
  $hadExisting = Test-Path $wfPath
  $wf          = $null

  if ($hadExisting) {
    $wf = Read-JsonSafe $wfPath
  }
  if (-not $wf) {
    # 沒有舊檔 → 從空物件開始
    $wf = [pscustomobject]@{}
  }

  $windows = @(6,12,24)
  $nowStr  = (Get-Date).ToString("s")
  $sourceValue = if ($hadExisting) { "compose_embedded" } else { "compose_safe_minimal" }

  # --- overall 節點：更新 / 建立 pass_rate / windows / generated / source ---
  if ($wf.PSObject.Properties.Name -contains 'overall') {
    $overall = $wf.overall
    if (-not $overall) {
      $overall = [pscustomobject]@{}
      $wf.overall = $overall
    }
  }
  else {
    $overall = [pscustomobject]@{}
    Add-Member -InputObject $wf -MemberType NoteProperty -Name 'overall' -Value $overall
  }

  if ($overall.PSObject.Properties.Name -contains 'windows') {
    $overall.windows = $windows
  } else {
    Add-Member -InputObject $overall -MemberType NoteProperty -Name 'windows' -Value $windows
  }

  if ($overall.PSObject.Properties.Name -contains 'pass_rate') {
    $overall.pass_rate = [double]$pr
  } else {
    Add-Member -InputObject $overall -MemberType NoteProperty -Name 'pass_rate' -Value ([double]$pr)
  }

  if ($overall.PSObject.Properties.Name -contains 'generated') {
    $overall.generated = $nowStr
  } else {
    Add-Member -InputObject $overall -MemberType NoteProperty -Name 'generated' -Value $nowStr
  }

  if ($overall.PSObject.Properties.Name -contains 'source') {
    $overall.source = $sourceValue
  } else {
    Add-Member -InputObject $overall -MemberType NoteProperty -Name 'source' -Value $sourceValue
  }

  # --- wf 別名節點：同步 windows / pass_rate ---
  if ($wf.PSObject.Properties.Name -contains 'wf') {
    $wfNode = $wf.wf
    if (-not $wfNode) {
      $wfNode = [pscustomobject]@{}
      $wf.wf = $wfNode
    }
  }
  else {
    $wfNode = [pscustomobject]@{}
    Add-Member -InputObject $wf -MemberType NoteProperty -Name 'wf' -Value $wfNode
  }

  if ($wfNode.PSObject.Properties.Name -contains 'windows') {
    $wfNode.windows = $windows
  } else {
    Add-Member -InputObject $wfNode -MemberType NoteProperty -Name 'windows' -Value $windows
  }

  if ($wfNode.PSObject.Properties.Name -contains 'pass_rate') {
    $wfNode.pass_rate = [double]$pr
  } else {
    Add-Member -InputObject $wfNode -MemberType NoteProperty -Name 'pass_rate' -Value ([double]$pr)
  }

  # 寫回 wf_summary.json（保留原本的 factors / factor_slo 等欄位）
  $wf | ConvertTo-Json -Depth 20 | Set-Content $wfPath -Encoding UTF8
  if (-not $Quiet) {
    Write-Host "[OK] 更新 $wfPath  pass_rate=$pr  windows=$($windows -join ',')" -ForegroundColor Green
  }
}

# ---------- 因子 gate-ready SLO：呼叫 scripts\factor_slo_check.py ----------

function Invoke-FactorGateReadySLO {
  param(
    [string]$Root    = ".",
    [string]$Profile = "gate_prod",
    [string]$Engine  = "classic"
  )

  $rulesPath = Join-Path $Root "rules_factors.yaml"
  $wfPath    = Join-Path $Root "reports\wf_summary.json"
  $cli       = Join-Path $Root "scripts\factor_slo_check.py"

  # 檔案不齊 → 視為「本輪 Gate 不啟用因子 SLO」
  if (-not (Test-Path $rulesPath)) {
    if (-not $Quiet) {
      Write-Host "[FactorSLO] rules_factors.yaml 不存在，略過因子 gate-ready SLO 檢查" -ForegroundColor DarkYellow
    }
    return @{
      enabled       = $false
      satisfied     = $true
      result        = $null
      exit_code     = $null
      error_message = $null
    }
  }
  if (-not (Test-Path $wfPath)) {
    if (-not $Quiet) {
      Write-Host "[FactorSLO] reports\wf_summary.json 不存在，略過因子 gate-ready SLO 檢查" -ForegroundColor DarkYellow
    }
    return @{
      enabled       = $false
      satisfied     = $true
      result        = $null
      exit_code     = $null
      error_message = $null
    }
  }
  if (-not (Test-Path $cli)) {
    if (-not $Quiet) {
      Write-Host "[FactorSLO] scripts\factor_slo_check.py 不存在，略過因子 gate-ready SLO 檢查" -ForegroundColor DarkYellow
    }
    return @{
      enabled       = $false
      satisfied     = $true
      result        = $null
      exit_code     = $null
      error_message = $null
    }
  }

  $py = Get-Python

  $rootAbs  = (Resolve-Path $Root).Path
  $rulesAbs = (Resolve-Path $rulesPath).Path
  $wfAbs    = (Resolve-Path $wfPath).Path

  $args = @(
    $cli,
    "--root",       $rootAbs,
    "--rules-file", $rulesAbs,
    "--wf-summary", $wfAbs,
    "--profile",    $Profile,
    "--engine",     $Engine,
    "--json",
    "--strict"
  )

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName               = $py
  $psi.Arguments              = ($args -join " ")
  $psi.RedirectStandardOutput = $true
  $psi.RedirectStandardError  = $true
  $psi.UseShellExecute        = $false
  $psi.CreateNoWindow         = $true

  $proc = New-Object System.Diagnostics.Process
  $proc.StartInfo = $psi
  [void]$proc.Start()
  $stdout = $proc.StandardOutput.ReadToEnd()
  $stderr = $proc.StandardError.ReadToEnd()
  $proc.WaitForExit()
  $code = $proc.ExitCode

  if ($stderr -and -not $Quiet) {
    Write-Host "[FactorSLO] STDERR: $stderr" -ForegroundColor DarkYellow
  }

  if (-not $stdout) {
    return @{
      enabled       = $true
      satisfied     = $false
      result        = $null
      exit_code     = $code
      error_message = "empty_output"
    }
  }

  try {
    $obj = $stdout | ConvertFrom-Json
  }
  catch {
    return @{
      enabled       = $true
      satisfied     = $false
      result        = $null
      exit_code     = $code
      error_message = "invalid_json: $($_.Exception.Message)"
    }
  }

  $satisfied = $false
  if ($obj.PSObject.Properties.Name -contains 'satisfied') {
    $satisfied = [bool]$obj.satisfied
  }

  return @{
    enabled       = $true
    satisfied     = $satisfied
    result        = $obj
    exit_code     = $code
    error_message = $null
  }
}

# ---------- Gate 規則：Preflight + pass_rate + 因子 gate-ready SLO ----------

# Gate 輸出版本資訊（schema/spec），用於釘死 Gate 規格與 JSON 結構
$GateSchemaVersion = "gate_summary.v1"
$GateSpecVersion   = "gate_rules.v2.0"

function Invoke-Gate(
  [string]$WFDir,
  [string]$Reports = ".\reports",
  [string]$AsOf,
  [string]$Root = "."
) {

  $preflightPath = Join-Path $Reports "preflight_report.json"
  $wfPath        = Join-Path $Reports "wf_summary.json"

  $pre = Read-JsonSafe $preflightPath
  $wf  = Read-JsonSafe $wfPath
  if (-not $wf) {
    throw "找不到或無法解析 wf_summary.json：$wfPath"
  }

  # --- Preflight 判定 ---
  $pre_ok     = $false
  $pre_reason = "missing_preflight_report"

  if ($pre) {
    # 舊版 preflight_report 沒有 status/ok 欄位，一律視為 PASS
    $pre_ok     = $true
    $pre_reason = "legacy_report_no_status"

    if ($pre.PSObject.Properties.Name -contains 'status') {
      $pre_ok     = [string]$pre.status -match 'PASS|OK'
      $pre_reason = [string]$pre.status
    }
    elseif ($pre.PSObject.Properties.Name -contains 'ok') {
      $pre_ok     = [bool]$pre.ok
      $pre_reason = if ($pre.PSObject.Properties.Name -contains 'reason') {
        [string]$pre.reason
      } else {
        "legacy_report_no_status"
      }
    }
  }

  # --- WF 判定：pass_rate ≥ 0.80 ---
  $wf_overall = $null
  if ($wf.PSObject.Properties.Name -contains 'overall') {
    $wf_overall = $wf.overall
  }
  elseif ($wf.PSObject.Properties.Name -contains 'wf') {
    $wf_overall = $wf.wf
  }
  elseif ($wf.PSObject.Properties.Name -contains 'pass_rate') {
    $wf_overall = $wf
  }

  if (-not $wf_overall) {
    throw "wf_summary.json 缺少 overall/wf/pass_rate 欄位：$wfPath"
  }

  $wins      = @()
  $pass_rate = $null

  if ($wf_overall.PSObject.Properties.Name -contains 'windows') {
    $wins = @($wf_overall.windows)
  } else {
    $wins = @(6,12,24)
  }

  if ($wf_overall.PSObject.Properties.Name -contains 'pass_rate') {
    $pass_rate = [double]$wf_overall.pass_rate
  } else {
    throw "wf_summary.json 缺少 pass_rate 欄位：$wfPath"
  }

  $wf_pass_min       = 0.8
  $pass_rate_rounded = [math]::Round($pass_rate, 4)
  $wf_pass_ok        = ($pass_rate_rounded -ge $wf_pass_min)

  $checks = @()

  $checks += [ordered]@{
    name      = "preflight_ok"
    pass      = $pre_ok
    value     = $pre_ok
    threshold = $true
    detail    = $pre_reason
  }

  $checks += [ordered]@{
    name      = "wf_pass_rate"
    pass      = $wf_pass_ok
    value     = $pass_rate_rounded
    threshold = $wf_pass_min
  }

  # --- 因子 gate-ready SLO（使用 factor_slo_check.py） ---
  $factorSlo          = Invoke-FactorGateReadySLO -Root $Root -Profile "gate_prod" -Engine "classic"
  $factor_slo_enabled = $factorSlo.enabled
  $factor_slo_result  = $factorSlo.result
  $factor_slo_ok      = $factorSlo.satisfied

  if ($factor_slo_enabled) {
    $detail       = $null
    $min_factors  = $null
    $total_factor = $null

    if ($factor_slo_result -ne $null) {
      if ($factor_slo_result.PSObject.Properties.Name -contains 'min_factors') {
        $min_factors = $factor_slo_result.min_factors
      }
      if ($factor_slo_result.PSObject.Properties.Name -contains 'total_factors') {
        $total_factor = $factor_slo_result.total_factors
      }
      $detail = "min_factors=$min_factors, total_factors=$total_factor"
    }
    elseif ($factorSlo.error_message) {
      $detail = $factorSlo.error_message
    }

    $checks += [ordered]@{
      name      = "factor_gate_ready_slo"
      pass      = $factor_slo_ok
      value     = $factor_slo_ok
      threshold = $true
      detail    = $detail
    }
  }

  # --- 最終 Gate 判定 ---
  $all_pass = ($pre_ok -and $wf_pass_ok)
  $reason   = "all_rules_ok"

  if (-not $pre_ok) {
    $all_pass = $false
    $reason   = "preflight_fail"
  }
  elseif (-not $wf_pass_ok) {
    $all_pass = $false
    $reason   = "wf_pass_rate_fail"
  }
  elseif ($factor_slo_enabled -and -not $factor_slo_ok) {
    $all_pass = $false
    $reason   = "factor_slo_fail"
  }

  $gate_state = if ($all_pass) { "PASS" } else { "FAIL" }

  $as_of = Get-AsOfDate -DateParam $AsOf
  if (-not $as_of) { $as_of = "" }

  # 為本次 Gate 生成 run_id／run_type（方便後續稽核與追蹤）
  $run_id   = "gate-{0}" -f (Get-Date).ToString("yyyyMMdd-HHmmss")
  $run_type = "weekly"

  $overall = [ordered]@{
    as_of     = $as_of
    generated = (Get-Date).ToString("s")
    gate      = $gate_state
    pass_rate = $pass_rate_rounded
    windows   = @($wins)
    mode      = "weekly_gate"
    source    = "wf_summary.json"
    reason    = $reason
  }

  if ($factor_slo_enabled -and $factor_slo_result -ne $null) {
    $overall.factor_slo_satisfied = $factor_slo_ok
  }

  $wfNode = [ordered]@{
    pass_rate = $pass_rate_rounded
    windows   = @($wins)
  }

  $preflightReportPath = if (Test-Path $preflightPath) { $preflightPath } else { "" }

  $runNode = [ordered]@{
    wf_dir           = $WFDir
    rules            = ".\rules.yaml"
    wf_summary       = $wfPath
    preflight_report = $preflightReportPath
    run_id           = $run_id
    run_type         = $run_type
  }

  $factorSloNode = $null
  if ($factor_slo_enabled -and $factor_slo_result -ne $null) {
    $factorSloNode = $factor_slo_result
  }

  $gateSummary = [ordered]@{
    schema_version = $GateSchemaVersion
    spec_version   = $GateSpecVersion
    overall        = $overall
    wf             = $wfNode
    factors        = @{}          # 保留舊欄位，未來 per-factor 用
    factor_slo     = $factorSloNode
    checks         = $checks
    run            = $runNode
  }

  # --- 防呆：避免寫出 null 或不完整的 gate_summary ---
  if (-not $gateSummary) {
    throw "gateSummary 為空，停止寫入 gate_summary.json"
  }

  $overallNode = $gateSummary.overall
  if (-not $overallNode) {
    throw "gateSummary.overall 不存在，停止寫入 gate_summary.json"
  }

  $winsNode = $overallNode.windows
  if (-not $winsNode -or $winsNode.Count -eq 0) {
    throw "gateSummary.overall.windows 為空，停止寫入 gate_summary.json"
  }

  $prNode = $overallNode.pass_rate
  if ($null -eq $prNode) {
    throw "gateSummary.overall.pass_rate 缺失，停止寫入 gate_summary.json"
  }
  try {
    [void][double]$prNode
  }
  catch {
    throw "gateSummary.overall.pass_rate '$prNode' 無法轉為數值，停止寫入 gate_summary.json"
  }

  $gatePath = Join-Path $Reports "gate_summary.json"
  $gateSummary | ConvertTo-Json -Depth 10 | Set-Content $gatePath -Encoding UTF8

  if (-not $Quiet) {
    Write-Host ("Gate 結果：gate={0}, pass_rate={1}, windows={2}" -f $gate_state, $pass_rate_rounded, ($wins -join ",")) -ForegroundColor Yellow
    if ($factor_slo_enabled) {
      Write-Host ("   因子 gate-ready SLO：satisfied={0}" -f $factor_slo_ok) -ForegroundColor Yellow
    }
    Write-Host "gate_summary.json 已輸出：$gatePath"
  }

  return $gateSummary
}

# ---------- 主流程開始 ----------

if (-not $Date) {
  $Date = Get-CurrentWFriday
}

$asOf = $Date
if (-not $Quiet) {
  Write-Host "== Run-WFGate | as-of (W-FRI) = $asOf | mode=$Mode ==" -ForegroundColor Cyan
}

$Reports = ".\reports"
Ensure-Dir $Reports

$gate_state = $null

if ($ShowOnly) {
  # 只讀取 gate_summary.json 顯示狀態
  $gatePath = Join-Path $Reports "gate_summary.json"
  if (-not (Test-Path $gatePath)) {
    throw "找不到 $gatePath，無法 ShowOnly"
  }
  $g = Read-JsonSafe $gatePath
  if (-not $g) {
    throw "無法解析 $gatePath"
  }
  $ov         = $g.overall
  $gate_state = $ov.gate
  $pr         = $ov.pass_rate
  $wins       = $ov.windows -join ","
  $gen        = $ov.generated
  Write-Host ("[ShowOnly] Gate={0}, pass_rate={1}, windows={2}, generated={3}" -f $gate_state, $pr, $wins, $gen) -ForegroundColor Yellow
}
else {
  if ($Mode -eq "safe") {
    # 1) Preflight
    $preflightPath = Invoke-Preflight -Date $asOf -Root $Root

    # 2) SAFE Compose（若存在 PASS/FAIL CSV）
    $pass = Join-Path $Reports "pass_results.csv"
    $fail = Join-Path $Reports "fail_results.csv"
    if ( (Test-Path $pass) -or (Test-Path $fail) ) {
      Compose-WFSummaryFromCsv -Reports $Reports
    }
    else {
      if (-not $Quiet) {
        Write-Host "[SAFE] 未找到 PASS/FAIL CSV，改用原本 wf_summary.json（若存在）" -ForegroundColor DarkYellow
      }
    }

    # 3) Gate
    $gateSummary = Invoke-Gate -WFDir $WFDir -Reports $Reports -AsOf $asOf -Root $Root
    $gate_state  = $gateSummary.overall.gate
  }
  elseif ($Mode -eq "formal") {
    # 假設你已經備妥 wf_summary.json，只做 Gate（不重跑 Preflight/Compose）
    $gateSummary = Invoke-Gate -WFDir $WFDir -Reports $Reports -AsOf $asOf -Root $Root
    $gate_state  = $gateSummary.overall.gate
  }
  else {
    throw "未知 Mode：$Mode（僅支援 safe/formal）"
  }

  # 可選：產出 Pack / Zip
  if ($Pack) {
    $packRoot = ".\reports\releases"
    Ensure-Dir $packRoot
    $tag      = "Weekly_{0}" -f $asOf
    $packDir  = Join-Path $packRoot $tag
    Ensure-Dir $packDir

    $files = @(
      "preflight_report.json",
      "wf_summary.json",
      "gate_summary.json",
      "pass_results.csv",
      "fail_results.csv"
    ) | ForEach-Object { Join-Path $Reports $_ }

    foreach ($f in $files) {
      if (Test-Path $f) {
        Copy-Item $f $packDir -Force
      }
    }

    if (Test-Path ".\rules.yaml") {
      Copy-Item ".\rules.yaml" $packDir -Force
    }
    if (Test-Path ".\investable_universe.txt") {
      Copy-Item ".\investable_universe.txt" $packDir -Force
    }
    if (Test-Path (Join-Path $WFDir "wf_topN_6_12_24m.yaml")) {
      Copy-Item (Join-Path $WFDir "wf_topN_6_12_24m.yaml") $packDir -Force
    }

    if ($Zip) {
      $zipPath = "$packDir.zip"
      if (Test-Path $zipPath) { Remove-Item $zipPath -Force }
      Compress-Archive -Path $packDir -DestinationPath $zipPath
      "   - Weekly Pack 已建立並壓縮：$zipPath" | Write-Host
    }
    else {
      "   - Weekly Pack 已建立：$packDir" | Write-Host
    }
  }
}

# --- Exit Code：PASS 回傳 0；FAIL 回傳 1 ---
if ($gate_state -eq "PASS") {
  exit 0
}
else {
  exit 1
}
