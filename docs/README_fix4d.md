
# fix4d — Flexible metrics & landing fallback
- 解析 metrics 欄位的**同義欄位**（calls/errors/rows/symbols）。
- 若 `rows` 缺省或為 0，採用 **parquet 落地數量**（自 phase 開始時間起）的 fallback。
- Full-Market 判定：`errors=0` 且 `(rows>0 或 calls≤3 或 landing>0)`。
- Single-Stock 判定：`errors=0` 且 `(rows>0 或 landing>0)`（`symbols` 有則需非空）。
- 保留 fix4 的：UTF‑8、強路徑、PID、早期心跳、單一字串參數、metrics 解析/回退、parquet 列表。
