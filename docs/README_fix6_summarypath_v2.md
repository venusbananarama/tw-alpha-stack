# AlphaCity Metrics Verify — fix6 hotfix (summary path, v2)
- 保證寫出 Summary JSON（finally），即使 NOOP / FAIL / 例外。
- `-SummaryJsonPath` 以專案根解析（非當前工作目錄）。
- 內含補救腳本：可從最新 metrics CSV 直接產生 JSON。

## 安裝
```powershell
cd 'G:\AI\tw-alpha-stack'
Expand-Archive .\AlphaCity_Metrics_Verif_Patch_20250922_fix6_hotfix_summarypath_v2.zip -DestinationPath . -Force
Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
```

## 驗證（會看到寫檔提示）
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 -Quick -Symbol 2330.TW `
  -SummaryJsonPath .\metrics\verify_summary_latest.json -VerboseCmd
```
若上一輪沒有 JSON，可用：
```powershell
.\scripts\ps\Write-VerifySummaryFromLatest.ps1 -OutPath .\metrics\verify_summary_latest.json
```
