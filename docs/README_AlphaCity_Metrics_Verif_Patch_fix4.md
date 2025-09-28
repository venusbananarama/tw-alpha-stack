
# AlphaCity Metrics Verif Patch — fix4 (UTF-8 + Quick + Robust Args)

## 你得到什麼
- **Invoke-AlphaVerification.ps1（fix4）**：
  - 同步串流、安全心跳（30s）。
  - **強制 UTF‑8**（避免 cp950/Big5 炸字元）。
  - `-Quick`：全市場近 1 年、單股近 30 天。
  - 以 **字串組參數** 傳遞，避免陣列被吃掉造成只看到 `python.exe` 的問題。
  - 解析 `metrics:`；若缺省，回退 `metrics/` 最新 CSV。
  - PASS/FAIL Summary + 最近 10 個 parquet 落地清單。

- **emit_metrics_wrapper.py**：
  - 不中斷串流；結束一定輸出 `=== Backfill Done ===  metrics: <abs path>`。
  - 若未輸出路徑或檔案不存在，回退掃描 `metrics/`。

## 安裝
```powershell
Expand-Archive .\AlphaCity_Metrics_Verif_Patch_20250922_fix4_utf8_args.zip -DestinationPath G:\AI\tw-alpha-stack -Force
Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
```

## 使用
- 完整驗證（2015–today）：
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 -Start 2015-01-01 -End (Get-Date).ToString('yyyy-MM-dd') -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```
- 快速驗證（近 1 年 / 30 天）：
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 -Quick -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```
