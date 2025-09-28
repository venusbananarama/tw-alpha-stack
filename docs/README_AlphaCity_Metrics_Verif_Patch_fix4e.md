# AlphaCity Metrics Verif Patch — fix4e-hotfix (final)

## 1) 這包做了什麼（Blueprint）
- **非阻塞輸出 + 早期心跳**：以 `StreamReader.Peek()` 避免 ReadLine 阻塞，啟動即印 PID；前 60 秒每 5 秒心跳、之後 30 秒。
- **UTF‑8 全面化**：PowerShell 與 Python `-X utf8` 強制 UTF‑8，避免 `cp950`/Big5 符號炸字。
- **強路徑/引數**：以自身路徑向上三層推 repo root；直接呼叫 `.venv\Scripts\python.exe`；所有路徑加引號；以**單一字串**傳參數。
- **metrics 彈性解析**：支援（calls/errors/rows/symbols）的同義欄位；支援 `status` 分佈與 NOOP 偵測。
- **落地 fallback**：若 rows=0 或缺省，統計本階段時間窗（start→end+15m）新增的 parquet 檔數作為成功信號。
- **PASS 判定**：
  - Full‑Market：`errors=0` 且（`rows>0` **或** `landing>0` **或** （`status` 全為 `EMPTY/NOOP/CACHED/UPTODATE/SKIP` 且 `calls≤3`））
  - Single‑Stock：`errors=0` 且（`rows>0` **或** `landing>0`）且 `symbols`（若存在）需非空
- **輸出摘要**：印出實際採用的欄位名（col=...）、status 摘要、近 10 個 parquet。

## 2) 安裝
```powershell
Expand-Archive .\AlphaCity_Metrics_Verif_Patch_20250922_fix4e_hotfix_final.zip -DestinationPath G:\AI\tw-alpha-stack -Force
Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
```

## 3) 使用
```powershell
# 快速驗證（近 1 年 / 30 天）
.\scripts\ps\Invoke-AlphaVerification.ps1 -Quick -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd

# 完整驗證（2015–today）
.\scripts\ps\Invoke-AlphaVerification.ps1 -Start 2015-01-01 -End (Get-Date).ToString('yyyy-MM-dd') -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```

## 4) 快捷語法（選配；寫入 Profile）
```powershell
$dst = $PROFILE.CurrentUserAllHosts
Split-Path $dst | % { New-Item -Type Directory -Force $_ > $null }
Copy-Item .\_tools\AlphaCity.Profile.ps1 $dst -Force
. $dst
```
用法：`acd`/`acroot`（專案根）｜`acopen`（檔案總管）｜`acvenv`｜`ack`（驗證；`-Quick`）｜`acb`（回補 wrapper）｜`acwho`（檢查）。

## 5) 疑難排除（精簡）
- **看起來沒動**：fix4e 用非阻塞讀 + 心跳；若仍靜默，檢查 `Get-Process -Id <PID>` 與 `datahub` 是否有新 parquet。  
- **rows=0 但實際有寫入**：以 **landing>0** 判 PASS；或看 `status` 是否為 NOOP 類型。  
- **找不到 metrics**：wrapper 會在結尾印 `metrics: <path>`；無則回退 metrics 目錄最新 CSV。  
- **系統別名衝突**：不要建立 `ac` alias；用 `acd/acroot` 函式即可。

## 6) 變更紀錄（相對 fix4c/4d）
- 修正 `.Count` ParserError；NOOP 偵測大小寫不敏感。
- 改非阻塞輸出，心跳準時；增加落地時間窗限制（start→end+15m）。
- PASS 說明加入「via rows / via landing / via noop」三路徑。