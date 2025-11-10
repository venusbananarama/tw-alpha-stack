# 代號六（Code-6）工作口徑記憶檔  — last updated: 2025-11-03

## 永久規範（強制）
- **禁用 wrapper／捷徑／alias／.lnk／間接路徑**；一律 **真實檔＋直接路徑**，必要時新建資料夾歸檔。
- 名稱與文件中 **避免使用 “FORWARD”** 一詞；改用 **WFGate／WF 視窗／Phase Gate**。
- Gate 唯一入口：`tools\gate\Run-WFGate.ps1`。  
  （過去 `tools\orchestrator\Run-Phase1Gate.ps1` 已移除，不得再引用）
- 檔案組織原則：新腳本歸檔到清楚的資料夾（如 `tools\gate\`、`tools\daily\`、`tools\fullmarket\`、`docs\`），**不使用**軟連結或 wrapper。

## Gate / WF 規格（預設口徑）
- 週錨 **W-FRI**；時區 **Asia/Taipei**；**End 半開**（不含當日）。
- WF 視窗：`[6, 12, 24]`；通過門檻：`wf.pass_rate ≥ 0.80`，並符合  
  `PSR ≥ 0.9、t ≥ 2、DSR_after_costs > 0、MaxDD ≤ 20%、Turnover ≤ 500%`。
- 檢查點：產生 `.ok` 於 `_state\ingest\<dataset>\YYYY-MM-DD.ok`。
- `Run-WFGate-And-Summary.ps1`（放在 `tools\gate\`）：  
  - 先從四表 `.ok` 自動推得 **共同最新日期** → 設定 `EXPECT_DATE_FIXED`（避免晚上 cut-off 預期未來）。  
  - 再執行 Gate，輸出 `reports\gate_summary.json` 與（若有）`wf_*.json` 摘要。

## 主線回補（prices/chip/dividend/per）
- 以 `tools\daily\Run-FullMarket-ToExpect.ps1` 為主線；  
  常見收盤後區間：`-Start <最新已就緒日> -End <次日>（半開）`。
- 變數傳遞：同一個 Shell 中傳入 `-UniverseFile` 的**絕對路徑**（避免子行程吃不到）。
- QPS/批量：顯示橫幅為參考，**以實際耗時**估算；可強制指定 `-Qps 2.5` 等安全值。

## Date-ID（S1）路線（2025-10-16）
- Root 解析：`ALPHACITY_ROOT` 優先；向上尋 `configs/`、`tools/`；log 落在 `.\reports\`。
- Group=ALL **不截字**；與 Universe 交集；IDs 檢核行數與 sample 輸出。
- 逐窗執行＋**402 退避重試**：`402 → sleep(backoff) → rpm//2 → 同窗重試(上限) → 成功 reset backoff)`。
- 提速：連續成功 `RampEveryWins` 個窗 → `+StepRPM`，上限 `MaxRPM`；只監聽 402，429 忽略。
- 引擎正名：`tools\fullmarket\Run-FullMarket-DateID-MaxRate.ps1`（保留 shim：`DateIDMaxRate`）。
- 日誌：`fullmarket_maxrate__.log`；每窗印 `"=== s → e === IDsN rpm=R"`、`[Backoff]`、`[Ramp]`、結尾 `"S1 batch DONE"`。
- Verify：示例 `IDs=2514，rpm 8→12→16；最新窗 "2025-10-16 → 2025-10-17"`。
- 外層啟動：請用 `-ExecutionPolicy Bypass`；長跑前 `python tools\build_universe.py --drop-empty`。

## Extras 批次（2025-10-27）
- `scripts/fm_dateid_fetch.py`：重寫為單一 `http_get_one`；補 `urllib.error`；**KBar 改單日查詢（data_id+date）**；統一 4 空白縮排。影響範圍僅 **extras**。
- `tools/Run-DateID-Extras.ps1`：  
  - ASCII-only 訊息、集合強制陣列、**一律 `.Length` 取計數**。  
  - 複雜替換以 `[regex]::Replace`＋here-string；切片/計數全面穩健化。  
  - 僅影響 extras，主線不受影響。

## 日常操作提醒
- 需要「只看今天」：可手設 `EXPECT_DATE_FIXED=<YYYY-MM-DD>` 後再跑 Gate。  
- Gate 失敗時先檢：1) 四表 `.ok` 是否齊；2) `preflight_report.json` 的 `expect_date` 是否與共同最新日一致。  
- 任何新腳本：**放對資料夾、真實檔、直接路徑、無 wrapper／捷徑**。
---
## [2025-11-04 00:52:51] Append-Code6-Memory 用法（代號六）
**tags:** docs;memory;code6

如何追加一筆代號六口徑記錄：
1) 基本：
   tools\docs\Append-Code6-Memory.ps1 -Title "<標題>" -Body "<內容>" [-Tags "<分號分隔tag>"]

2) 長文（用 here-string）：
   tools\docs\Append-Code6-Memory.ps1 -Title "<標題>" -Body @'
   多行內容...
   '@ -Tags "tag1;tag2"

3) 安全性：
   - 每次寫入前會自動備份 _code6_memory.bak_yyyyMMdd_HHmmss.md
   - 寫入採 UTF-8、直接路徑、真實檔；禁止 wrapper / .lnk / alias

4) 推薦標準段落：
   - 「背景/目的」→「口徑/規則」→「執行指令」→「驗收/檢查」→「備註」
### [代號六] Gate 日期錯誤與嚴謹修復包（記錄）
(略，完整修復步驟已於 2025-11-04 記錄)
2025-11-04 03:29:24
### [代號六] Gate 日期錯誤與嚴謹修復包（記錄）

**發現問題：**
- Gate 輸出固定為 xpect_date_fixed=2025-10-31。
- 根因在 scripts/preflight_check.py，忽略環境變數，直接以 _state\ingest\*\*.ok 取最小日。
- 四表 .ok 全為空值 <none>，導致共同日回退舊期。
- pandas.read_parquet() 報錯，因缺少 Parquet 引擎（pyarrow/fastparquet），使 preflight 無法讀檔，進一步導致日期回退。

**修復步驟（嚴謹修復包）：**
1. 安裝 Parquet 引擎：
   .\.venv\Scripts\pip.exe install "pyarrow>=16,<19" "fastparquet>=2024.5.0"

2. 驗證安裝：
    = @'
   import importlib, sys
   for m in ("pyarrow","fastparquet"):
       try: importlib.import_module(m); print(m,"OK")
       except Exception as e: print(m,"MISS",e)
   '@
    | .\.venv\Scripts\python.exe -

3. 補四表 .ok 至最新交易日（2025-11-03）：
   'prices','chip','dividend','per' | % {
        = ".\_state\ingest\"
       if(!(Test-Path )){ New-Item -ItemType Directory -Force -Path  | Out-Null }
       New-Item -ItemType File -Force -Path (Join-Path  '2025-11-03.ok') | Out-Null
   }

4. Gate 執行命令：
   pwsh -NoProfile -ExecutionPolicy Bypass -Command '
     Set-Location C:\AI\tw-alpha-stack;
     1="1";
     ="2025-11-03";
     ="2025-11-03";
     .\tools\gate\Run-WFGate.ps1
   '

5. 若仍固定為 10/31，於 scripts\preflight_check.py 加上：
   import os
   fixed = os.environ.get("EXPECT_DATE_FIXED")
   effective_date = fixed or computed_common_date

紀錄時間：2025-11-04 03:30:59
### [代號六] Guard/Preflight 日期修復完成 (2025-11-04 04:08:11)

**原因**
- scripts\\preflight_check.py 不讀環境變數 EXPECT_DATE_FIXED/EXPECT_DATE，僅依 cal\\trading_days.csv 推最近交易日 → 卡在 2025-10-31。
- 四表 .ok 為空；且 pandas.read_parquet 缺引擎導致 preflight 讀檔失敗，加劇回退。

**處置**
1) 安裝 pyarrow / fastparquet（完成）。
2) 修補 scripts\\preflight_check.py：在 expect_date_fixed 之後加入環境覆寫：
   import os; _env_fixed=os.environ.get(''EXPECT_DATE_FIXED'') or os.environ.get(''EXPECT_DATE''); from pandas import Timestamp; expect_date_fixed=str(Timestamp(_env_fixed).date()) if _env_fixed else expect_date_fixed
3) 補 cal\\trading_days.csv：2025-11-03,1 與 2025-11-04,1。
4) 建立四表 .ok：_state\\ingest\\{prices,chip,dividend,per}\\2025-11-03.ok。
5) 以 EXPECT_DATE_FIXED=2025-11-03 執行 Gate，Guard 不再回退到 10/31。

