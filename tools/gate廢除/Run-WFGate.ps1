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

function Convert-ToBoolOrFalse([object]$Value) {
  if ($null -eq $Value) { return $false }
  if ($Value -is [bool]) { return [bool]$Value }
  $s = [string]$Value
  if (-not $s) { return $false }
  return $s.Trim().Equals("True", [System.StringComparison]::OrdinalIgnoreCase)
}

function Write-PassFailFromDigTable {
  param(
    [string]$DigPath,
    [string]$PassPath,
    [string]$FailPath
  )

  $rows = Import-Csv $DigPath
  $passRows = New-Object System.Collections.Generic.List[object]
  $failRows = New-Object System.Collections.Generic.List[object]

  foreach ($row in $rows) {
    $okRank = Convert-ToBoolOrFalse $row.ok_rank_ic
    $okCov  = Convert-ToBoolOrFalse $row.ok_coverage
    $ok     = ($okRank -and $okCov)

    $item = [pscustomobject]@{
      factor_id = $row.factor_id
      window    = $row.window_m
      ok        = $ok
    }

    if ($ok) {
      $passRows.Add($item)
    }
    else {
      $failRows.Add($item)
    }
  }

  "factor_id,window,ok" | Set-Content -Path $PassPath -Encoding UTF8
  "factor_id,window,ok" | Set-Content -Path $FailPath -Encoding UTF8

  if ($passRows.Count -gt 0) {
    $passRows | Export-Csv -Path $PassPath -NoTypeInformation -Append -Encoding UTF8
  }
  if ($failRows.Count -gt 0) {
    $failRows | Export-Csv -Path $FailPath -NoTypeInformation -Append -Encoding UTF8
  }

  return [pscustomobject]@{
    pass_count = $passRows.Count
    fail_count = $failRows.Count
    total      = ($passRows.Count + $failRows.Count)
  }
}

function Get-IntOrZero([object]$Value) {
  if ($null -eq $Value) { return 0 }
  try { return [int]$Value } catch { return 0 }
}

function Normalize-Windows([object]$Wins) {
  if ($null -eq $Wins) { return @() }

  if ($Wins -is [string]) {
    return @(
      ($Wins -split ',' |
        ForEach-Object { $_.Trim() } |
        Where-Object { $_ -ne "" } |
        ForEach-Object { [int]$_ })
    )
  }

  return @($Wins)
}

function Get-FactorSloConfig {
  param(
    [string]$Root = ".",
    [string]$Profile = "gate_prod",
    [string]$Engine = "classic"
  )

  $fallback = @{
    source                 = "gate_prod_default"
    profile                = $Profile
    engine                 = $Engine
    min_factors            = 8
    min_factors_per_window = 3
    required_factors       = @("mom_6m","value_pe","quality_roeq")
    per_window_min         = @{ "6" = 3; "12" = 3; "24" = 2 }
  }

  $rulesPath = Join-Path $Root "rules_factors.yaml"
  if (-not (Test-Path $rulesPath)) { return $fallback }

  $yamlCmd = Get-Command ConvertFrom-Yaml -ErrorAction SilentlyContinue
  if (-not $yamlCmd) { return $fallback }

  try {
    $doc = Get-Content $rulesPath -Raw | ConvertFrom-Yaml
  }
  catch {
    return $fallback
  }

  if (-not $doc -or -not $doc.gate_ready) { return [pscustomobject]$fallback }

  $gateReady = $doc.gate_ready

  $profileNode = $null
  if ($Profile -and $gateReady.profiles) {
    foreach ($p in $gateReady.profiles.PSObject.Properties) {
      if ($p.Name -eq $Profile) { $profileNode = $p.Value; break }
    }
  }

  $missingProfileCfg =
    ($null -eq $profileNode) -or
    ($profileNode -is [hashtable] -and $profileNode.Count -eq 0) -or
    ($profileNode -is [pscustomobject] -and $profileNode.PSObject.Properties.Count -eq 0)

  if ($missingProfileCfg) {
    return [pscustomobject]$fallback
  }

  $merged = @{}
  foreach ($k in $fallback.Keys) { $merged[$k] = $fallback[$k] }

  $profilePairs = @()
  if ($profileNode -is [hashtable]) {
    foreach ($k in $profileNode.Keys) {
      $profilePairs += ,@($k, $profileNode[$k])
    }
  }
  else {
    foreach ($p in $profileNode.PSObject.Properties) {
      $profilePairs += ,@($p.Name, $p.Value)
    }
  }

  foreach ($pair in $profilePairs) {
    $k = $pair[0]
    $v = $pair[1]
    $isEmpty =
      ($null -eq $v) -or
      ($v -is [string] -and [string]::IsNullOrWhiteSpace($v)) -or
      ($v -is [hashtable] -and $v.Count -eq 0) -or
      ($v -is [System.Collections.IEnumerable] -and -not ($v -is [string]) -and (@($v).Count -eq 0))

    if (-not $isEmpty) { $merged[$k] = $v }
  }

  $required = @()
  $reqRaw = $merged["required_factors"]
  if ($reqRaw) {
    if ($reqRaw -is [string]) {
      $v = $reqRaw.Trim()
      if ($v) { $required += $v }
    }
    else {
      foreach ($v in $reqRaw) {
        if ($null -eq $v) { continue }
        $s = [string]$v
        if ($s) { $required += $s.Trim() }
      }
    }
  }

  $perWindowMin = @{}
  $perWindowNode = $merged["per_window"]
  if ($perWindowNode) {
    foreach ($p in $perWindowNode.PSObject.Properties) {
      $minVal = Get-IntOrZero $p.Value.min_factors
      if ($minVal -gt 0) {
        $perWindowMin[[string]$p.Name] = $minVal
      }
    }
  }
  elseif ($merged.ContainsKey("per_window_min")) {
    $perWindowNode = $merged["per_window_min"]
    if ($perWindowNode -is [hashtable]) {
      foreach ($k in $perWindowNode.Keys) {
        $minVal = Get-IntOrZero $perWindowNode[$k]
        if ($minVal -gt 0) {
          $perWindowMin[[string]$k] = $minVal
        }
      }
    }
    else {
      foreach ($p in $perWindowNode.PSObject.Properties) {
        $minVal = Get-IntOrZero $p.Value
        if ($minVal -gt 0) {
          $perWindowMin[[string]$p.Name] = $minVal
        }
      }
    }
  }

  return [pscustomobject]@{
    source                 = "rules_factors.yaml"
    profile                = $Profile
    engine                 = $Engine
    min_factors            = Get-IntOrZero $merged["min_factors"]
    min_factors_per_window = Get-IntOrZero $merged["min_factors_per_window"]
    required_factors       = $required
    per_window_min         = $perWindowMin
  }
}

function Build-FactorSloResult {
  param(
    [string[]]$PassFactors,
    [hashtable]$FactorWindows,
    [object]$Windows,
    [object]$Config
  )

  if (-not $Config) { return $null }

  $winsArr = Normalize-Windows $Windows
  $perWindowCounts = [ordered]@{}
  foreach ($w in $winsArr) {
    $perWindowCounts["$w"] = 0
  }

  foreach ($fid in $PassFactors) {
    $wins = $null
    if ($FactorWindows.ContainsKey($fid)) { $wins = $FactorWindows[$fid] }

    if ($wins -and $wins.Count -gt 0) {
      foreach ($w in $winsArr) {
        if ($wins.Contains([string]$w)) {
          $perWindowCounts["$w"] = Get-IntOrZero $perWindowCounts["$w"]
          $perWindowCounts["$w"] += 1
        }
      }
    }
    else {
      foreach ($w in $winsArr) {
        $perWindowCounts["$w"] = Get-IntOrZero $perWindowCounts["$w"]
        $perWindowCounts["$w"] += 1
      }
    }
  }

  $missingRequired = @()
  foreach ($rf in $Config.required_factors) {
    if ($PassFactors -notcontains $rf) {
      $missingRequired += $rf
    }
  }

  $satisfied = $true
  if ($Config.min_factors -gt 0 -and $PassFactors.Count -lt $Config.min_factors) {
    $satisfied = $false
  }

  foreach ($w in $winsArr) {
    $count = Get-IntOrZero $perWindowCounts["$w"]
    $wKey = [string]$w
    $specific = 0
    if ($Config.per_window_min.ContainsKey($wKey)) {
      $specific = Get-IntOrZero $Config.per_window_min[$wKey]
    }
    $effective = [Math]::Max([int]$Config.min_factors_per_window, $specific)
    if ($effective -gt 0 -and $count -lt $effective) {
      $satisfied = $false
    }
  }

  if ($missingRequired.Count -gt 0) {
    $satisfied = $false
  }

  return [ordered]@{
    name                    = "factor_gate_ready"
    profile                 = $Config.profile
    engine                  = $Config.engine
    source                  = $Config.source
    wf_summary_path         = ""
    min_factors             = $Config.min_factors
    min_factors_per_window  = $Config.min_factors_per_window
    per_window_min          = $Config.per_window_min
    required_factors        = $Config.required_factors
    total_factors           = $PassFactors.Count
    windows                 = @($winsArr)
    per_window_counts       = $perWindowCounts
    missing_required_factors = $missingRequired
    satisfied               = $satisfied
  }
}

function Update-WFSummaryFromPassResults {
  param(
    [object]$Wf,
    [object[]]$PassRows,
    [int[]]$Windows,
    [string]$Root = ".",
    [string]$Profile = "gate_prod",
    [string]$Engine = "classic"
  )

  $factorWindows = @{}
  foreach ($row in $PassRows) {
    $fid = $null
    if ($row.PSObject.Properties.Name -contains 'factor_id') {
      $fid = $row.factor_id
    }
    if (-not $fid) { continue }
    $fid = ([string]$fid).Trim()
    if (-not $fid) { continue }

    if (-not $factorWindows.ContainsKey($fid)) {
      $factorWindows[$fid] = New-Object System.Collections.Generic.HashSet[string]
    }

    $winVal = $null
    if ($row.PSObject.Properties.Name -contains 'window') {
      $winVal = $row.window
    }
    elseif ($row.PSObject.Properties.Name -contains 'window_m') {
      $winVal = $row.window_m
    }

    if ($winVal -ne $null) {
      $winStr = ([string]$winVal).Trim()
      if ($winStr) {
        [void]$factorWindows[$fid].Add($winStr)
      }
    }
  }

  if ($factorWindows.Count -eq 0) { return $false }

  $passIds = @($factorWindows.Keys | Sort-Object)

  $passedMap = [ordered]@{}
  foreach ($fid in $passIds) {
    $wins = $factorWindows[$fid]
    $winsArr = Normalize-Windows $wins
    $winList = @()
    if ($winsArr -and $winsArr.Count -gt 0) {
      $winList = @(
        $winsArr | Sort-Object -Unique
      )
    }
    $payload = if ($winList.Count -gt 0) {
      [ordered]@{ windows = $winList }
    }
    else {
      [ordered]@{}
    }
    $passedMap[$fid] = $payload
  }

  if ($Wf.PSObject.Properties.Name -contains 'factors_by_status') {
    $fbs = $Wf.factors_by_status
    if (-not $fbs) {
      $fbs = [pscustomobject]@{}
      $Wf.factors_by_status = $fbs
    }
  }
  else {
    $fbs = [pscustomobject]@{}
    Add-Member -InputObject $Wf -MemberType NoteProperty -Name 'factors_by_status' -Value $fbs
  }

  if ($fbs.PSObject.Properties.Name -contains 'passed') {
    $fbs.passed = $passedMap
  }
  else {
    Add-Member -InputObject $fbs -MemberType NoteProperty -Name 'passed' -Value $passedMap
  }

  if ($Wf.PSObject.Properties.Name -contains 'roster') {
    $roster = $Wf.roster
    if (-not $roster) {
      $roster = [pscustomobject]@{}
      $Wf.roster = $roster
    }
  }
  else {
    $roster = [pscustomobject]@{}
    Add-Member -InputObject $Wf -MemberType NoteProperty -Name 'roster' -Value $roster
  }

  if ($roster.PSObject.Properties.Name -contains 'passed') {
    $roster.passed = $passIds
  }
  else {
    Add-Member -InputObject $roster -MemberType NoteProperty -Name 'passed' -Value $passIds
  }

  $cfg = Get-FactorSloConfig -Root $Root -Profile $Profile -Engine $Engine
  if ($cfg) {
    $slo = Build-FactorSloResult -PassFactors $passIds -FactorWindows $factorWindows -Windows $Windows -Config $cfg
    if ($slo) {
      if ($Wf.PSObject.Properties.Name -contains 'factor_slo') {
        $Wf.factor_slo = $slo
      }
      else {
        Add-Member -InputObject $Wf -MemberType NoteProperty -Name 'factor_slo' -Value $slo
      }
    }
  }

  return $true
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

  [void](Update-WFSummaryFromPassResults -Wf $wf -PassRows $passes -Windows $windows -Root $Root -Profile "gate_prod" -Engine "classic")

  # 寫回 wf_summary.json（保留原本的 factors / factor_slo 等欄位）
  $wf | ConvertTo-Json -Depth 20 | Set-Content $wfPath -Encoding UTF8
  if (-not $Quiet) {
    Write-Host "[OK] 更新 $wfPath  pass_rate=$pr  windows=$($windows -join ',')" -ForegroundColor Green
  }
}

# ---------- 因子 gate-ready SLO：呼叫 scripts\p2\factor_slo_check.py ----------

function Invoke-FactorGateReadySLO {
  param(
    [string]$Root    = ".",
    [string]$Profile = "gate_prod",
    [string]$Engine  = "classic"
  )

  $rulesPath = Join-Path $Root "rules_factors.yaml"
  $wfPath    = Join-Path $Root "reports\wf_summary.json"
  $cli       = Join-Path $Root "scripts\p2\factor_slo_check.py"

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
      Write-Host "[FactorSLO] scripts\p2\factor_slo_check.py 不存在，略過因子 gate-ready SLO 檢查" -ForegroundColor DarkYellow
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

  $passPath = Join-Path $Reports "pass_results.csv"
  $needsSloInputs = $false
  if (-not ($wf.PSObject.Properties.Name -contains 'factors_by_status')) {
    $needsSloInputs = $true
  }
  elseif (-not $wf.factors_by_status -or -not ($wf.factors_by_status.PSObject.Properties.Name -contains 'passed')) {
    $needsSloInputs = $true
  }
  if (-not ($wf.PSObject.Properties.Name -contains 'factor_slo')) {
    $needsSloInputs = $true
  }

  if ($needsSloInputs -and (Test-Path $passPath)) {
    $passRows = Import-Csv $passPath
    $updated = Update-WFSummaryFromPassResults -Wf $wf -PassRows $passRows -Windows $wins -Root $Root -Profile "gate_prod" -Engine "classic"
    if ($updated) {
      $wf | ConvertTo-Json -Depth 20 | Set-Content $wfPath -Encoding UTF8
      if (-not $Quiet) {
        Write-Host "[SAFE] wf_summary 缺少 factor_slo inputs，已從 pass_results.csv 補齊" -ForegroundColor DarkYellow
      }
    }
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
      $digPath = Join-Path $Reports ("dig\\factor_dig_table.{0}.csv" -f $asOf)
      if (Test-Path $digPath) {
        $counts = Write-PassFailFromDigTable -DigPath $digPath -PassPath $pass -FailPath $fail
        if (-not $Quiet) {
          Write-Host ("[SAFE] 找不到 CSV 使用 dig_table 生成 生成到 {0}, {1} 生成筆數={2} (pass={3}, fail={4})" -f $pass, $fail, $counts.total, $counts.pass_count, $counts.fail_count) -ForegroundColor DarkYellow
        }
        Compose-WFSummaryFromCsv -Reports $Reports
      }
      else {
        if (-not $Quiet) {
          Write-Host "[SAFE] 未找到 PASS/FAIL CSV，改用原本 wf_summary.json（若存在）" -ForegroundColor DarkYellow
        }
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

