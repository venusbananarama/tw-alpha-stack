
# fix4e — Non-blocking stdout & NOOP-aware PASS
- 以 `StreamReader.Peek()` 避免 `ReadLine()` 阻塞 → 心跳一定會印出。
- Full-Market：若 `status` 全為 `EMPTY/NOOP/CACHED/UPTODATE/SKIP`，且 `calls<=3`，視為 **no-op OK**。
- PASS 規則：
  - Full-Market：`errors=0` 且 (`rows>0` 或 `landing>0` 或 `noop_ok`)
  - Single-Stock：`errors=0` 且 (`rows>0` 或 `landing>0`) 且 `symbols` 若存在需非空
- 保留：UTF-8、強路徑、PID、早期心跳、彈性 metrics 解析、parquet 落地 fallback。
