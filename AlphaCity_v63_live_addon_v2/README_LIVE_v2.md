# AlphaCity v6.3 Live Add-on (v2)

- 即時串流輸出、不寫任何檔案、不中斷既有流程。
- 不覆蓋既有 `ack` / wrapper；新增 `acklive` 供前景或背景使用。

## 使用（不改 $PROFILE）
```powershell
cd G:\AI\tw-alpha-stack
# 解壓後：
. .\tools\AckLive.ps1
acklive -Start (Get-Date).AddDays(-3).ToString('yyyy-MM-dd') -End (Get-Date).ToString('yyyy-MM-dd') -Symbol 2330.TW -NoFsScan
```

## 安裝（可選）
```powershell
.\tools\Install-AckLive.ps1 -Root . -AddToProfile
```

## 解除（可選）
```powershell
.\tools\Uninstall-AckLive.ps1 -Root . -RemoveScript
```
