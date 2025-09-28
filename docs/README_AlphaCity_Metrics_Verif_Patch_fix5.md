# AlphaCity Metrics Verification — fix5 (All-in-One)

## 1) 藍圖（Blueprint）
- **非阻塞輸出 + 早期心跳**：`Peek()` 迴圈；啟動即印 PID，首 60s 每 5s 心跳，其後 30s。
- **UTF-8 全面化**：PowerShell 與 Python `-X utf8` 強制 UTF‑8，避免 Big5/cp950 炸字。
- **強路徑/引號/單一字串參數**：repoRoot 由腳本定位；一律絕對路徑與引號；參數以單字串傳遞防切分。
- **metrics 彈性解析**：支援 calls/errors/rows/symbols 同義欄位；建模 `status` 分佈；NOOP 感知。
- **落地 fallback（時間窗＋目錄白名單）**：統計 `start→end+Δ` 期間於 `ParquetScope` 內的新 parquet 檔數。
- **最新 metrics 選取**：從輸出中收集所有 `metrics:` 路徑，挑最後寫入時間最新者；若無則回退目錄最新。
- **PASS 邏輯（可配置）**：
  - Full-Market：`errors=0`、`exit=0（若 StrictExitCode）` 且（`rows>0` **或** `landing>0` **或** `NOOP OK`）。
  - Single-Stock：`errors=0`、`exit=0（若 StrictExitCode）` 且（`rows>0` **或** `landing>0` **或** `NOOP OK*`）。
  - `NOOP OK*` 可透過 `-DisallowNoopFull/-DisallowNoopSingle` 關閉；有 `calls` 則要求 `≤3`，沒 `calls` 也可通過。
- **摘要可解釋**：每段皆輸出 `via rows/landing/noop` 理由、實際欄位名、exit code 與 status profile。

## 2) 可能發生的衝突（並行/版本/環境）
- **metrics 架構演進**：欄名更動導致舊規則失效 → 以「同義欄位群」解析，必要時落地 fallback。
- **NOOP 誤判**：與其它流程共用 datahub 造成落地噪音 → `ParquetScope` 白名單＋`start→end+Δ` 時間窗。
- **多實例併發**：多次執行混用 `metrics:` → 實作「所有路徑中挑最新」，避免選到舊檔。
- **路徑/工作目錄錯置**：在 System32 查相對路徑 → 強制 WorkingDirectory＝repo root；檢查用**絕對路徑**。
- **UTF‑8 炸字**：Console/程式碼頁差異 → `-X utf8` + `PYTHONIOENCODING/PYTHONUTF8` + 不輸出 emoji。
- **API 配額/網路**：限流導致 0 rows → 允許 NOOP PASS；提供 `-StrictExitCode` 以強制嚴格模式。
- **符號格式**：`2330.TW/2330` 差異 → 單股自動去 `.TW`。

## 3) 安裝
```powershell
Expand-Archive .\AlphaCity_Metrics_Verif_Patch_20250922_fix5_allin_one.zip -DestinationPath G:\AI\tw-alpha-stack -Force
Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
```

## 4) 使用
```powershell
# 快速（近1年/30天）
.\scripts\ps\Invoke-AlphaVerification.ps1 -Quick -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd

# 完整（2015–today）
.\scripts\ps\Invoke-AlphaVerification.ps1 -Start 2015-01-01 -End (Get-Date).ToString('yyyy-MM-dd') -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd

# 可選參數
# 關閉 NOOP PASS：
#   -DisallowNoopFull（全市場） -DisallowNoopSingle（單股）
# 嚴格 exit code：
#   -StrictExitCode
# 調整落地時間窗與目錄白名單：
#   -LandingWindowMins 15 -ParquetScope 'silver\alpha\prices','silver\alpha\chip'
```

## 5) 快捷 Profile（選配）
將 `_tools/AlphaCity.Profile.ps1` 複製到 `$PROFILE.CurrentUserAllHosts`，提供 `acd/acroot/acopen/acvenv/acpy/ack/acb/acwho` 等指令。

## 6) 疑難排除（精要）
- **看似卡住**：非阻塞＋心跳；若仍無動靜，`Get-Process -Id <PID>` 與 `datahub` 是否有新 parquet。
- **全 EMPTY**：通常是 NOOP；若想強制失敗，使用 `-DisallowNoopFull/-DisallowNoopSingle`。
- **找不到 metrics**：wrapper 尾端一定印 `metrics:`；無則回退 metrics 最新 CSV。
- **ExitCode 非 0**：加 `-StrictExitCode` 會直接 FAIL，摘要顯示 exit code 與第一段錯誤。