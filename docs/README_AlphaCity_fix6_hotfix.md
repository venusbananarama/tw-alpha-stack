# AlphaCity Metrics Verification — fix6 hotfix (inline-if & join)
本包修正：
- PowerShell `Get-Flex` 內**行內 if** 與 **-join** 用法，改為合法可求值的區塊/運算子。
- 子程序輸出強制**不緩衝**（wrapper `python -u` + `PYTHONUNBUFFERED=1`）。
- 指標解析容錯（欄位名同義、大小寫不敏感）。
- PASS 規則：`via rows / via landing / via noop`（可用 `-DisallowNoop*` 關閉 NOOP PASS；`-StrictExitCode` 強制 exit=0）。

## 安裝
```powershell
cd 'G:\AI\tw-alpha-stack'
Expand-Archive .\AlphaCity_Metrics_Verif_Patch_20250922_fix6_hotfix_ifjoin.zip -DestinationPath . -Force
Unblock-File .\scripts\ps\Invoke-AlphaVerification.ps1
```

## 使用（快速）
```powershell
.\scripts\ps\Invoke-AlphaVerification.ps1 -Quick -Symbol 2330.TW -Workers 6 -Qps 1.6 -VerboseCmd
```

## 常見參數
- `-DisallowNoopFull -DisallowNoopSingle`：必須有 rows/landing 才 PASS。
- `-StrictExitCode`：內層 exit≠0 直接 FAIL。
- `-LandingWindowMins 15`：落地統計視窗（可調）。
- `-ParquetScope 'silver\alpha\prices','silver\alpha\chip'`：落地偵測白名單。
- `-SummaryJsonPath '.\metrics\verify_summary.json'`：輸出 JSON 給 CI。
- `-CalendarCsv '.\cal\trading_days.csv'`：NOOP 時註記非交易日原因。
