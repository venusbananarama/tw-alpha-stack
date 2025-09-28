# Changelog

## 2025-09-25 — 佈局標準化（封存標籤：20250925_0327）
- 執行 Standardize-ProjectLayout.ps1（-Apply），將舊路徑整併至 canonical 結構
- 封存歷史內容至 `_archive/20250925_0327/`
- 新增 `tools/Check-CanonicalLayout.ps1`，納入 pre-commit / 本機排程
- 清理殘留：移動 `install_tw_alpha_reporting*` → `scripts/install/`，`Check-FMStatus.ps1` → `tools/`，`make_report_safe/` → `scripts/reports/`
- 檢查結果：`[OK] Canonical layout passed`
