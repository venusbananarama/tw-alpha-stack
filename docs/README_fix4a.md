
# fix4a hotfix
- 修正 `$repoRoot` 解析（改為向上三層，避免落在 `...\scripts`）。
- 直接以 `python.exe` 啟動（不再巢狀 powershell -Command），引數：`-X utf8 <args>`。
- 其他邏輯沿用 fix4：UTF-8、Quick、metrics 解析/回退、同步串流、心跳。
