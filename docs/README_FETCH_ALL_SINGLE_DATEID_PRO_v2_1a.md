# Fetch-All-SingleDateId Pro v2.1a

**這是修正版**：移除 C# 三元運算子、加入 `-ApiToken` 與 `-UseThreadJob`，避免 0% 卡住。

## 安裝
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_Pro_v2_1a.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 使用（穩定配方）
```powershell
cd G:\AI\tw-alpha-stack

.\scripts\ps\Fetch-All-SingleDateId_Pro_v2_1a.ps1 `
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv" `
  -ApiToken "<YOUR_TOKEN>" -UseThreadJob `
  -Workers 4 -Qps 1.2 `
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 疑難排解
- `Start-ThreadJob` 找不到 → 在 PowerShell 7+ 安裝/匯入 `ThreadJob` 模組：
  ```powershell
  Install-Module ThreadJob -Scope CurrentUser -Force
  Import-Module ThreadJob
  ```
- 看失敗 Job 的錯誤：
  ```powershell
  Get-Job -State Failed | % { "-----"; $_.Name; Receive-Job $_ -Keep }
  ```
