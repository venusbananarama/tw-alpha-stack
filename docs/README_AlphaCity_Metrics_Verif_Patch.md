# AlphaCity Metrics & Verification Patch (2025-09-21)

這個補丁提供兩個檔案：
- `scripts/emit_metrics_wrapper.py`：外掛包裝器，保證任何回填腳本都會在輸出中出現一行 `=== Backfill Done ===  metrics: <path>`。
- `scripts/ps/Invoke-AlphaVerification.ps1`：強化版驗證腳本，對「全市場 prices」與「單股（prices + chip）」自動執行並評分，
  可以解析 wrapper 的 metrics 行，若取不到則退回專案 `metrics/` 目錄中的最新檔案。

## 安裝
1. 將 ZIP 解壓到專案根目錄（與現有 `scripts/`、`metrics/` 同層）。
2. 如 PowerShell 有執行限制：
   ```powershell
   Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
   ```

## 使用
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 ^
  -Start 2015-01-01 -End (Get-Date).ToString('yyyy-MM-dd') ^
  -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```

## 設計重點
- **可串流 & 不中斷**：wrapper 以行為單位轉印子程序輸出，並即時擷取 metrics。
- **雙保險**：若內部腳本未輸出 metrics，會改以 `metrics/ingest_summary_*.csv` 最新檔回補。
- **驗證穩健**：PS 腳本以寬鬆規則處理 `symbols` 欄位缺失；並固定以 rows/errors 為主判準。
- **零侵入**：不必修改你既有的 `finmind_backfill.py`；若先前加過暫時 patch，仍可相容。
