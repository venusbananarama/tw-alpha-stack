# CODEX Patch — Verification fix5（一次到位）

## 1) 立即上手（fix5｜All-in-One）
下載：`AlphaCity_Metrics_Verif_Patch_20250922_fix5_allin_one.zip`

- 非阻塞輸出＋早期心跳、UTF‑8、強路徑/引號/單字串參數
- 「最新 metrics」選擇策略；彈性欄位解析；NOOP-aware；落地 fallback（時間窗＋白名單）
- PASS：Full-Market = errors=0 且 (rows>0 或 landing>0 或 NOOP OK)；Single-Stock 同理（可用參數關閉 NOOP）

## 4) 疑難排除（增補）
- 看似無輸出：心跳應每 5/30 秒出現；否則用 PID 檢查。
- 全為 EMPTY：若期望仍 PASS，確保未加 `-DisallowNoop*`；若要嚴格，開 `-StrictExitCode`。

## 5) 下載物
- `AlphaCity_Metrics_Verif_Patch_20250922_fix5_allin_one.zip`

## 6) 變更紀錄（fix5 新增）
- 新增 NOOP 無 calls 仍可 PASS；新增「via rows/landing/noop」理由。
- 限定 parquet 掃描路徑與時間窗；從多條 `metrics:` 中挑最新檔。