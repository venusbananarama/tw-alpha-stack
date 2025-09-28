# CODEX Patch — 將 1/4/5/6 節更新為 fix4e-hotfix 版（貼到 Codex）

## 1) 立即上手（Verification fix4e｜hotfix）
下載：`AlphaCity_Metrics_Verif_Patch_20250922_fix4e_hotfix_final.zip`

PASS 規則：
- Full‑Market：`errors=0` 且（`rows>0` **或** `landing>0` **或** `noop_ok`）
- Single‑Stock：`errors=0` 且（`rows>0` **或** `landing>0`）；`symbols` 欄位若存在需非空

> `noop_ok`：`status` 全為 `EMPTY/NOOP/CACHED/UPTODATE/SKIP` 且 `calls≤3`。

## 4) 疑難排除（節錄）
- **沒心跳/看似卡住**：fix4e 改非阻塞讀（Peek），前 60 秒每 5 秒心跳。  
- **rows=0 但資料落地**：以 landing>0 視為 PASS。  
- **找不到 metrics**：wrapper 末行 `metrics:`；無則回退 metrics 最新 CSV。

## 5) 下載物
- `AlphaCity_Metrics_Verif_Patch_20250922_fix4e_hotfix_final.zip`

## 6) 變更紀錄（新增 fix4e）
- 非阻塞輸出 + 早期心跳；NOOP-aware；彈性 metrics；落地時間窗；修正 `.Count` ParserError。