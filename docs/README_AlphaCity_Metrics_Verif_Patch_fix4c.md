
# AlphaCity Metrics Verif Patch — fix4c (final)

## Blueprint（你將得到）
1) **同步串流 + 心跳**
   - 不用 Runspace 事件；逐行讀 stdout/stderr，安全且穩定。
   - 啟動立即列印 **PID**；前 60 秒 **每 5 秒心跳**，之後每 30 秒。

2) **UTF‑8 全面化**
   - PowerShell Console/OutputEncoding + `PYTHONIOENCODING`/`PYTHONUTF8`。
   - Python 以 `-X utf8` 強制 UTF‑8（避免 cp950/Big5 無法輸出 ⚠ 等符號）。

3) **穩健路徑與引數**
   - 以本檔案路徑向上 **三層**計算 repo root（不會落在 `scripts\`）。
   - 直接執行 `<repo>\.venv\Scripts\python.exe`，**不再巢狀 powershell -Command**。
   - 所有路徑均以 **雙引號**包裹；用 **單一字串**傳參數，避免被切分。

4) **驗證流程**
   - Full‑Market（`TaiwanStockPrice @ TSE`）→ Single‑Stock（`2330: price+chip`）。
   - 解析 wrapper 的 `metrics: <csv>`；若無，回退 `metrics/` 最新 CSV。
   - **PASS/FAIL** 規則：
     - Full‑Market：`calls ≤ 3 && errors = 0 && rows > 0`
     - Single‑Stock：`errors = 0 && rows > 0 && symbols 非空（若有）`
   - 列出最近 10 個 parquet，驗證資料落地。

5) **Wrapper**（`emit_metrics_wrapper.py`）
   - 邊跑邊轉印；結束一定印：`=== Backfill Done ===  metrics: <abs path>`。
   - 若未印或檔案不存在，回退掃描 `metrics/`（支援多種命名）。

## 可能發生的衝突 & 對策
- **(A) 路徑計算錯誤 → 出現 `scripts\scripts` 或找不到 venv**  
  對策：fix4c 以「向上三層」取 repo root；所有路徑以 `Join-Path` 封裝且加引號。
- **(B) 只看到 `python.exe`（無引數）/ 卡住**  
  對策：改為「單一字串傳參數」；`[RUN]` 會完整印出命令；同時顯示 PID + 心跳。
- **(C) `cp950 codec can't encode`（⚠ 等字元）**  
  對策：Console/OutputEncoding 與 Python 強制 UTF‑8（`-X utf8` + env）。
- **(D) 在錯誤目錄下檢查產物（如 System32）**  
  對策：驗證腳本固定 `WorkingDirectory = repo root`；文件內提醒用 **絕對路徑**檢查。
- **(E) alias 衝突（`ac = Add-Content`）**  
  對策：不使用 `ac`；提供 `acd/acroot` 函式；若需 alias，指向 `function:*` 命名空間。
- **(F) metrics 未輸出 / 命名不一致**  
  對策：wrapper 解析 `metrics:`；無則回退最新 CSV（`*.csv / ingest* / ingest_summary_* / *metrics*`）。
- **(G) FinMind QPS/配額限制**  
  對策：可調 `-Qps`、`--hourly-cap`；先以 `-Quick` 確認流程健康再跑完整。
- **(H) 符號格式差異（`2330.TW` vs `2330`）**  
  對策：單股自動把 `.TW` 去掉再傳入。

## 安裝
```powershell
Expand-Archive .\AlphaCity_Metrics_Verif_Patch_20250922_fix4c_final.zip -DestinationPath G:\AI\tw-alpha-stack -Force
Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
```

## 使用
- **快速驗證（近 1 年 / 30 天）**
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 -Quick -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```
- **完整驗證（2015–today）**
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 -Start 2015-01-01 -End (Get-Date).ToString('yyyy-MM-dd') -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```

## 觀察重點（排障最短路徑）
- 是否立即看到 **[PID]** 與前 60 秒 **5 秒一次**的 **[HB ...]**。
- `=== Backfill Done ===  metrics: <abs path>` 是否出現；若無，`metrics/` 是否有新 CSV。
- `== Verification Summary ==` 的 PASS/FAIL 指標（calls / errors / rows）。
- `datahub` 下是否有最新 parquet 檔案（時間戳遞增）。
