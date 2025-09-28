# AlphaCity Metrics Verify — fix6b hotfix
- **非阻塞輸出讀取**（async ReadLine + 心跳），不會卡住畫面卻沒有心跳。
- **一定寫 JSON**：`finally` 寫入，路徑以專案根解析。
- **快速隔離**：`-SkipFull` 或 `-SkipSingle` 任選，只跑一半流程驗證。

## 安裝
```powershell
cd 'G:\AI\tw-alpha-stack'
Expand-Archive .\AlphaCity_Metrics_Verif_Patch_20250922_fix6b_asyncHB_skipfull.zip -DestinationPath . -Force
Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
```

## 測一趟（只跑單股，5 分鐘超時）
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 -Start 2025-09-19 -End 2025-09-21 `
  -SkipFull -Symbol 2330.TW -Workers 2 -Qps 1 `
  -SummaryJsonPath .\metrics\verify_summary_latest.json `
  -PhaseTimeoutMins 5 -VerboseCmd
```
看到 `[HB hh:mm:ss] phase=single-stock still running...` 表示心跳正常。
