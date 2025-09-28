# Fetch-All-SingleDateId v2.2d — Parallel Hotfix
- 修正 ForEach-Object -Parallel 的參數集錯誤，改以 $using: 捕捉外部變數。
- Sequential 模式仍可用顯式參數執行。

## 安裝
Expand-Archive "$env:USERPROFILE\Downloads\Fetch-All-SingleDateId_v2_2d.zip" -DestinationPath "G:\AI\tw-alpha-stack" -Force
