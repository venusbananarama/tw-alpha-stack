# Fetch-All-SingleDateId Pro v2.1

**新功能**
- `-ApiToken "<token>"`：直接把 token 注入每個子 Job，免設環境變數。
- `-UseThreadJob`：改用 ThreadJob（同進程）避免環境隔離造成的 0% 卡住。

## 安裝
```powershell
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_Pro_v2_1.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
```

## 用法
```powershell
cd G:\AI\tw-alpha-stack

# 最穩定：ThreadJob + 明確 token
.\scripts\ps\Fetch-All-SingleDateId_Pro_v2_1.ps1 `
  -UniverseCsv "G:\AI\tw-alpha-stack\datahub\_meta\investable_universe.csv" `
  -ApiToken "<YOUR_TOKEN>" -UseThreadJob `
  -Workers 4 -Qps 1.2 `
  -Start 2015-01-01 -End ((Get-Date).ToString("yyyy-MM-dd"))
```

## 疑難排解
- 0% 卡住 → 加上 `-UseThreadJob` 或提供 `-ApiToken`。
- 看子任務錯誤：`Get-Job | ? State -eq Failed | % { Receive-Job $_ -Keep }`
