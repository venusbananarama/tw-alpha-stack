# 代號4｜Date‑ID MAX 指令＋監控（修正版）— 可搬移 Runbook（2025‑10‑19 接手版）

**口徑**：PowerShell 為主；`--end` 一律半開（不含終點）；週錨 **W‑FRI**；SSOT=`rules.yaml`；長跑標籤建議 **`S1_hist48`**；時區一律以 **Asia/Taipei** 作為資料對齊與日期窗計算（顯示時間如未註明亦以 Asia/Taipei）。

---

## 0) 現況／進度快照（交接面板）
- **入口統一**：一律由 `tools\Run-Max-Recent.ps1` 進入（Engine 正名 `Run-FullMarket-DateID-MaxRate.ps1`；舊名相容見「附錄A」，平時不建立 shim）。
- **長跑策略**：單進程固定 **48 rpm**（壓 <95/min）；如需加速，改**多進程分片**（每進程仍 ≤48 rpm）。
- **已知噪音**：`Run-DateID-Extras.ps1` 偶發寫檔錯誤（不阻斷主流程，但降低觀測性）。
- **當前 ck**：`state\dateid_checkpoint.json` 內 **`last_end = 2004-01-15`**（半開右端）。

---

## 1) 一鍵續跑（沿用 ck，單線 48 rpm）
```powershell
# 專案根
Set-Location C:\AI\tw-alpha-stack
$ErrorActionPreference = 'Stop'
$env:ALPHACITY_ROOT = (Get-Location).Path

# （可選）離線彙整最新窗，雙檢 last_end
pwsh -NoProfile -File .\tools\Summarize-DateId-Progress.ps1

# （可選）由 log 連鎖重建並覆寫 ck
pwsh -NoProfile -File .\tools\Build-HistoricalCheckpoint.ps1

# 帶 ck 續跑｜單線 48 rpm
pwsh -NoProfile -ExecutionPolicy Bypass -File .\tools\Run-Max-Recent.ps1 `
  -Start 2004-01-01 -End (Get-Date) `
  -WindowDays 1 -Workers 1 -BatchSize 60 `
  -ThrottleRPM 48 -MaxRPM 48 -StepRPM 0 -RampEveryWins 0 `
  -BackoffSeconds 5 -MaxBackoffSeconds 60 -MaxRetriesPerWin 2 `
  -UseCheckpoint -CheckpointPath .\state\dateid_checkpoint.json `
  -Tag S1_hist48 -Group ALL
```

**監控（前台 HUD）**
```powershell
pwsh -NoProfile -File .\tools\Watch-DateId-HUD.ps1 -RefreshSec 2
```

---

## 2) 快速健康檢查（語法/存在性/可觀測性）
> 若解析/存在性失敗 → 回到 0) 現況／進度快照 檢查路徑與檔名。

```powershell
# 1) 核心腳本語法正確
$files = @(
  '.\state\engine_transcript_wrapper.ps1',
  '.\tools\Run-Max-Recent.ps1',
  '.\tools\Select-IDs.ps1'
)
foreach($f in $files){
  $tokens=$null;$errors=$null
  [System.Management.Automation.Language.Parser]::ParseFile((Resolve-Path $f),[ref]$tokens,[ref]$errors) | Out-Null
  if($errors){ throw "ParserError in $f : $($errors | Out-String)" } else { "✅ Syntax OK: $f" }
}

# 2) 至少有一個 engine 檔
'Run-FullMarket-DateID-MaxRate.ps1','Run-FullMarket-DateIDMaxRate.ps1' |
  % { Join-Path .\tools $_ } | ?{ Test-Path $_ } | Select -First 1 |
  % { "✅ Engine found: $_" }

# 3) 追尾最新 log 並檢 exit=0 / 402 重試跡象
$lf = Get-ChildItem .\reports -File | ?{ $_.Name -like 'fullmarket_maxrate_*_*.log' } |
      Sort-Object LastWriteTime -Desc | Select-Object -First 1
(Get-Content $lf.FullName -Tail 120) -join "`n"
Select-String -Path $lf.FullName -Pattern 'HTTP 402|Payment Required|\[Invoke\]\s+exit=0' | Select -Last 10
```

---

## 附錄A（可選）— Engine 舊名相容（防「少連字號」踩雷）
> 原則：日常一律由 `Run-Max-Recent.ps1` 進入。只有在無法立即改動舊自動化時，才臨時建立**舊名 → 正名**的相容轉向（shim）。

```powershell
# 建立 Run-FullMarket-DateIDMaxRate.ps1 → 正名 的 shim（一次性）
$shim = '.\tools\Run-FullMarket-DateIDMaxRate.ps1'
Set-Content $shim -Encoding UTF8 -Value @'
param(
  [string]$Start='2004-01-01', [string]$End='', [int]$BatchSize=400,
  [int]$WindowDays=7, [int]$Workers=4, [switch]$Strict
)
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$real = Join-Path $here 'Run-FullMarket-DateID-MaxRate.ps1'
$psargs = @('-File', $real, '-Start', $Start)
if($End){ $psargs += @('-End', $End) }
$psargs += @('-BatchSize', $BatchSize, '-WindowDays', $WindowDays, '-Workers', $Workers)
if($Strict){ $psargs += '-Strict' }
pwsh -NoProfile -ExecutionPolicy Bypass @psargs
'@
"相容別名已建立：$shim"
```

---

## 3) Live Tail + 402 蜂鳴 ＆ 輕量 HUD（Run‑Max‑Recent 外層日誌）
```powershell
# 1) Live Tail + 402 蜂鳴
$lf = Get-ChildItem .\reports -File |
  Where-Object { $_.Name -like 'fullmarket_maxrate_*_*.log' } |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1
if(-not $lf){ throw '找不到 fullmarket_maxrate_*_*.log' }
"Tail: $($lf.FullName)"
Get-Content $lf.FullName -Wait -Tail 0 | ForEach-Object {
  if($_ -like '*HTTP 402*'){
    [console]::Beep(1200,120); Write-Host $_ -ForegroundColor Red
  } elseif($_ -like '*[Ramp]*' -or $_ -like '*FINMIND_THROTTLE_RPM=*'){
    Write-Host $_ -ForegroundColor Cyan
  } else { $_ }
}

# 2) 輕量 HUD（每 5 秒刷新；估算近況）
$lf = Get-ChildItem .\reports -File |
  Where-Object { $_.Name -like 'fullmarket_maxrate_*_*.log' } |
  Sort-Object LastWriteTime -Descending | Select-Object -First 1
if(-not $lf){ throw '找不到 fullmarket_maxrate_*_*.log' }
"HUD Log: $($lf.FullName)"
while($true){
  $sz1 = (Get-Item $lf.FullName).Length
  Start-Sleep -Seconds 5
  $sz2 = (Get-Item $lf.FullName).Length
  $delta = $sz2 - $sz1
  $tail = Get-Content $lf.FullName -Tail 2000
  $http = ($tail | Select-String -SimpleMatch 'HTTP ' | Measure-Object).Count
  $e402 = ($tail | Select-String -SimpleMatch 'HTTP 402' | Measure-Object).Count
  $lastWin = ($tail | Select-String -Pattern '^=== .* -> .* ===' | Select-Object -Last 1).Line
  $targetRPM = ($tail | Select-String -Pattern 'FINMIND_THROTTLE_RPM=\d+' | Select-Object -Last 1).Line
  Clear-Host
  Write-Host ('[Size Δ 5s] {0,8} bytes' -f $delta)
  Write-Host ('[HTTP lines] {0,8} (tail 2k)' -f $http)
  Write-Host ('[HTTP 402] {0,8} (tail 2k)' -f $e402)
  if($lastWin){ Write-Host ("[Last win] " + $lastWin) } else { Write-Host '[Last win] (n/a)' }
  if($targetRPM){ Write-Host ("[TargetRPM] " + $targetRPM) } else { Write-Host '[TargetRPM] (n/a)' }
}
```
> 備註：HUD 為輕量估算；正式分析請用外層 log 的時間化指標或增補時間戳。

---

## 4) 離線彙整 & 一鍵補 ck
```powershell
# 依窗頭 '=== s -> e ===' 彙整歷史，輸出 CSV
pwsh -NoProfile -File .\tools\Summarize-DateId-Progress.ps1

# 以「連續窗接龍」重建 ck（last_end）並安全寫入
pwsh -NoProfile -File .\tools\Build-HistoricalCheckpoint.ps1
```
> 目前 ck 寫入：`last_end = 2004-01-15`（半開右端）。

---

## 5) 速率策略（實務）
- **單線**：建議固定 **48 rpm**（壓 <95/min 安全域）。
- **加速**：改走**多進程分片**（每進程 ≤48 rpm）；402 → **指數回退**與**降速**；`Ramp` 僅在連續成功後提升。
- **Tag 策略**：歷史長跑固定用 `S1_hist48`；短測/驗線用 `S1_verify`。

---

## 6) Extras 噪音的最小補丁（寫檔容錯）
在 `Run-DateID-Extras.ps1` 寫檔前加入：
```powershell
$dst = $YourOutPath   # ← 目標輸出完整路徑（請接到原腳本變數）
$dir = Split-Path -Parent $dst
if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir -Force | Out-Null }

# 寫入改用 Set-Content / Add-Content，並降低噪音
$payload | Set-Content -LiteralPath $dst -Encoding UTF8 -ErrorAction SilentlyContinue
```
> 若仍 sporadic 鎖檔，可**暫停 Extras**，待歷史跑完再回補。

---

## 7) 每日排程（增量＋7 日自癒）— 標準範例（`Run‑Max‑Recent.ps1`）
> 以下示範以 `Run‑Max‑Recent.ps1` 為入口的標準排程範例。

```powershell
Import-Module ScheduledTasks -ErrorAction Stop
$pwsh = (Get-Command pwsh).Source
$pr = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Limited
$root = (Resolve-Path .).Path

# 01:40 每日增量（以 ck 續跑 → 今日）
$argI = @"
-NoProfile -ExecutionPolicy Bypass -File "$root\tools\Run-Max-Recent.ps1" `
  -Start $((Get-Date).Date.AddDays(-1).ToString('yyyy-MM-dd')) -End $((Get-Date).ToString('yyyy-MM-dd')) `
  -WindowDays 1 -Workers 1 -BatchSize 60 -ThrottleRPM 48 -MaxRPM 48 `
  -UseCheckpoint -CheckpointPath "$root\state\dateid_checkpoint.json" -Tag S1_daily -Group ALL
"@
$actI = New-ScheduledTaskAction -Execute $pwsh -Argument -Argument $argI
$trI = New-ScheduledTaskTrigger -Daily -At 01:40
Register-ScheduledTask -TaskName 'Alpha_DateID_Incremental' -Action $actI -Trigger $trI -Principal $pr -Force

# 03:40 近 7 日自癒（覆蓋短缺）
$argH = @"
-NoProfile -ExecutionPolicy Bypass -File "$root\tools\Run-Max-Recent.ps1" `
  -Start $((Get-Date).Date.AddDays(-7).ToString('yyyy-MM-dd')) -End $((Get-Date).ToString('yyyy-MM-dd')) `
  -WindowDays 1 -Workers 1 -BatchSize 60 -ThrottleRPM 48 -MaxRPM 48 `
  -UseCheckpoint -CheckpointPath "$root\state\dateid_checkpoint.json" -Tag S1_heal7 -Group ALL
"@
$actH = New-ScheduledTaskAction -Execute $pwsh -Argument $argH
$trH = New-ScheduledTaskTrigger -Daily -At 03:40
Register-ScheduledTask -TaskName 'Alpha_DateID_Heal7' -Action $actH -Trigger $trH -Principal $pr -Force
```

### 7.1) 多進程分片（每進程 ≤48 rpm）— 範例
```powershell
# 每進程自守 ≤48 rpm；總速率 ≈ shards × throttle，請自行留安全餘裕
$shards = 3
$root = (Resolve-Path .).Path
$script = Join-Path (Join-Path $root 'tools') 'Run-Max-Recent.ps1'
$ck = Join-Path (Join-Path $root 'state') 'dateid_checkpoint.json'
$start = (Get-Date).Date.AddDays(-7).ToString('yyyy-MM-dd')
$end = (Get-Date).ToString('yyyy-MM-dd')
$throttle = 48
$tagBase = 'S1_hist48_shard'
$group = 'ALL'
$pwsh = (Get-Command pwsh).Source

1..$shards | ForEach-Object {
  $tag = "$tagBase$_"
  $args = @(
    '-NoProfile','-ExecutionPolicy','Bypass','-File', $script,
    '-Start', $start, '-End', $end,
    '-WindowDays','1','-Workers','1','-BatchSize','60',
    '-ThrottleRPM', $throttle, '-MaxRPM', $throttle,
    '-StepRPM','0','-RampEveryWins','0',
    '-BackoffSeconds','5','-MaxBackoffSeconds','60','-MaxRetriesPerWin','2',
    '-UseCheckpoint','-CheckpointPath', $ck,
    '-Tag', $tag,'-Group', $group
  )
  Start-Process -FilePath $pwsh -ArgumentList $args | Out-Null
}
```

### 7.1.1) 多進程分片（4 shards｜總速 ≈ 88 rpm；每線 22）— 生產安全模板
> **原則**：以**總速 ≤ 88 rpm** 為例，4 線平分 → 每線 **22 rpm**；遇 402 由引擎內建**指數回退＋降速**處理。必要時把 `$totalRPM` 調低以預留餘裕。

```powershell
$shards   = 4
$totalRPM = 88                              # 總速安全線（可依帳戶風險調整）
$root     = (Resolve-Path .).Path
$script   = Join-Path (Join-Path $root 'tools') 'Run-Max-Recent.ps1'
$ck       = Join-Path (Join-Path $root 'state') 'dateid_checkpoint.json'
$start    = (Get-Date).Date.AddDays(-7).ToString('yyyy-MM-dd')
$end      = (Get-Date).ToString('yyyy-MM-dd')
$throttle = [math]::Floor($totalRPM / $shards)  # → 22
$tagBase  = "S1_sh${shards}_rpm${throttle}"
$group    = 'ALL'
$pwsh     = (Get-Command pwsh).Source

1..$shards | ForEach-Object {
  $tag  = ('{0}_{1}' -f $tagBase, $_)
  $args = @(
    '-NoProfile','-ExecutionPolicy','Bypass','-File', $script,
    '-Start', $start, '-End', $end,
    '-WindowDays','1','-Workers','1','-BatchSize','60',
    '-ThrottleRPM', $throttle, '-MaxRPM', $throttle,
    '-StepRPM','0','-RampEveryWins','0',
    '-BackoffSeconds','5','-MaxBackoffSeconds','60','-MaxRetriesPerWin','2',
    '-UseCheckpoint','-CheckpointPath', $ck,
    '-Tag', $tag,'-Group', $group
  )
  Start-Process -FilePath $pwsh -ArgumentList $args | Out-Null
}
```

### 7.1.2) 多進程分片（6 shards｜總速 ≈ 84 rpm；每線 14）— 生產安全模板
> 仍以**總速 ≤ 88 rpm**為基準，6 線配置 **84 rpm**（每線 14）。

```powershell
$shards   = 6
$totalRPM = 84
$root     = (Resolve-Path .).Path
$script   = Join-Path (Join-Path $root 'tools') 'Run-Max-Recent.ps1'
$ck       = Join-Path (Join-Path $root 'state') 'dateid_checkpoint.json'
$start    = (Get-Date).Date.AddDays(-7).ToString('yyyy-MM-dd')
$end      = (Get-Date).ToString('yyyy-MM-dd')
$throttle = [math]::Floor($totalRPM / $shards)  # → 14
$tagBase  = "S1_sh${shards}_rpm${throttle}"
$group    = 'ALL'
$pwsh     = (Get-Command pwsh).Source

1..$shards | ForEach-Object {
  $tag  = ('{0}_{1}' -f $tagBase, $_)
  $args = @(
    '-NoProfile','-ExecutionPolicy','Bypass','-File', $script,
    '-Start', $start, '-End', $end,
    '-WindowDays','1','-Workers','1','-BatchSize','60',
    '-ThrottleRPM', $throttle, '-MaxRPM', $throttle,
    '-StepRPM','0','-RampEveryWins','0',
    '-BackoffSeconds','5','-MaxBackoffSeconds','60','-MaxRetriesPerWin','2',
    '-UseCheckpoint','-CheckpointPath', $ck,
    '-Tag', $tag,'-Group', $group
  )
  Start-Process -FilePath $pwsh -ArgumentList $args | Out-Null
}
```

### 7.1.3) 多進程分片（8 shards｜總速 ≈ 88 rpm；每線 11）— 生產安全模板
> 8 線時以**每線 11 rpm** 控制在 **88 rpm** 左右；如上升至 12×8=96 可能貼近風險邊界，僅在 UI/日誌觀測穩定時酌情使用。

```powershell
$shards   = 8
$totalRPM = 88
$root     = (Resolve-Path .).Path
$script   = Join-Path (Join-Path $root 'tools') 'Run-Max-Recent.ps1'
$ck       = Join-Path (Join-Path $root 'state') 'dateid_checkpoint.json'
$start    = (Get-Date).Date.AddDays(-7).ToString('yyyy-MM-dd')
$end      = (Get-Date).ToString('yyyy-MM-dd')
$throttle = [math]::Floor($totalRPM / $shards)  # → 11
$tagBase  = "S1_sh${shards}_rpm${throttle}"
$group    = 'ALL'
$pwsh     = (Get-Command pwsh).Source

1..$shards | ForEach-Object {
  $tag  = ('{0}_{1}' -f $tagBase, $_)
  $args = @(
    '-NoProfile','-ExecutionPolicy','Bypass','-File', $script,
    '-Start', $start, '-End', $end,
    '-WindowDays','1','-Workers','1','-BatchSize','60',
    '-ThrottleRPM', $throttle, '-MaxRPM', $throttle,
    '-StepRPM','0','-RampEveryWins','0',
    '-BackoffSeconds','5','-MaxBackoffSeconds','60','-MaxRetriesPerWin','2',
    '-UseCheckpoint','-CheckpointPath', $ck,
    '-Tag', $tag,'-Group', $group
  )
  Start-Process -FilePath $pwsh -ArgumentList $args | Out-Null
}
```

### 7.2) 解除排程／清理 — 範例
```powershell
$names = @('Alpha_DateID_Incremental','Alpha_DateID_Heal7')
foreach($n in $names){
  $t = Get-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue
  if($t){
    Disable-ScheduledTask -TaskName $n -ErrorAction SilentlyContinue | Out-Null
    Unregister-ScheduledTask -TaskName $n -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Removed $n"
  } else {
    Write-Host "Not found: $n"
  }
}
```

---

## 8) 檔案樹（當前形狀）
```
C:\AI\tw-alpha-stack
├─ state
│  ├─ engine_transcript_wrapper.ps1        # 穩定版 HB+Transcript
│  ├─ dateid_checkpoint.json               # last_end=2004-01-15
│  └─ dateid_progress_history.csv          # Summarize 輸出
├─ tools
│  ├─ Run-Max-Recent.ps1                   # 新 orchestrator（相容別名）
│  ├─ Select-IDs.ps1                       # 名單交集/篩選
│  ├─ Summarize-DateId-Progress.ps1        # 離線彙整
│  ├─ Build-HistoricalCheckpoint.ps1       # 連續鏈→ck
│  └─ Watch-DateId-HUD.ps1                 # 監控 HUD
├─ configs
│  ├─ investable_universe.txt
│  └─ groups
│     ├─ ALL.txt
│     └─ TESTmini.txt
└─ reports\                                # fullmarket_maxrate_*_*.log（HB 持續寫入）
```

---

## 9) SSOT / Gate 對齊（北極星）
- **KPI**：週 RankIC ≥ 0.03、Sharpe ≥ 1.0、MaxDD ≤ 20%、年化 Turnover ≤ 500%。
- **Gate（Walk‑forward 三窗 6/12/24 月）**：Pass Ratio ≥ 0.80；`DSR_after_costs > 0`；`PSR ≥ 0.9`；t ≥ 2；`replay_mae_bps ≤ 2`；`backtest_vs_replay ≥ 0.99`；Live kill‑switch −10%/−15%。
- **SSOT**：`rules.yaml` 作為單一事實來源；`run_manifest.json` 綁定 `ssot_hash / data_ver / model_id`；報表對齊 `preflight_report.json`、`gate_summary.json`。
- **週錨**：W‑FRI。

---

## 10) 疑難排解（現成指令）
> 若未見最新 log 或找不到引擎 → 先回到 0) 現況／進度快照 交叉檢查。

```powershell
# 追尾目前跑的 log
$lf = Get-ChildItem .\reports -File | ?{ $_.Name -like 'fullmarket_maxrate_*_*.log' } |
      Sort-Object LastWriteTime -Desc | Select-Object -First 1
(Get-Content $lf.FullName -Tail 120) -join "`n"

# 看是否有 402 或 exit=0
Select-String -Path $lf.FullName -Pattern 'HTTP 402|Payment Required|\[Invoke\]\s+exit=0' | Select -Last 10

# 強停所有 Run-Max-Recent 子行程（必要時）
Get-CimInstance Win32_Process |
  ?{ $_.Name -eq 'pwsh.exe' -and $_.CommandLine -match 'Run-Max-Recent.ps1' } |
  % { Stop-Process -Id $_.ProcessId -Force }

# 檔名／路徑防呆
function Test-AlphaCityScript {
  param([Parameter(Mandatory)][string]$NameLike)
  $hits = Get-ChildItem .\tools -Filter "*$NameLike*" -ea SilentlyContinue
  if(-not $hits){ Write-Warning "找不到與 '$NameLike' 相符的腳本。請檢查連字號與大小寫。"; return }
  $hits | Select Name, FullName, Length, LastWriteTime | Format-Table -AutoSize
}

# 範例：
Test-AlphaCityScript -NameLike 'Run-FullMarket-DateID'
```

---

## 11) 證據鏈（Evidence Chain）
- `reports/preflight_report.json`：freshness / as‑of / schema / calendar 檢查。
- `reports/gate_summary.json`：WF/Gate 逐條門檻與結論。
- `configs/investable_universe.txt`：投資池清單（來源 `configs/universe.yaml`）。
- `run_manifest.json`：SSOT 哈希、`data_ver`、`model_id` 綁定。
- `reports/*.html|xlsx`：NAV/DD/Sharpe/成本明細、超額曲線。

---

## 備忘（詞彙/規範）
- **時區**：資料對齊與日期窗計算一律以 Asia/Taipei；顯示時間如未特別標註亦以 Asia/Taipei。
- `--end` **半開**；若需單日，優先使用 `-Date`。
- **Orchestrator 入口**：`Run-Max-Recent.ps1`；**Engine**：`Run-FullMarket-DateID-MaxRate.ps1`（舊名相容見附錄A）。
- **禁止**在 Python 進入 REPL 假象；以 PowerShell 為主。
- **最小監控**：近 5 分鐘吞吐、402 計數、窗尾穩步推進（`=== yyyy-MM-dd -> yyyy-MM-dd ===`）。
- **S1/S4** 舊指令：已 Retired（整併至 `Run-Max-Recent.ps1` 入口）。
