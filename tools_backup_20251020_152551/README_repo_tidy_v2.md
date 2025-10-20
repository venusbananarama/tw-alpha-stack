# repo_tidy_v2.ps1 — 專案清理/歸位器（安全版）

**放置位置**：把本檔放到 `./tools/`。

## 指令
```powershell
# 規劃（Dry-run；產出 _audit/*.csv 報表）
pwsh -File .\tools\repo_tidy_v2.ps1

# 真正執行（搬/刪；建議加 -Backup）
pwsh -File .\tools\repo_tidy_v2.ps1 -Apply -Backup
```

## 規則重點
- Canonical：`./scripts ./tools ./tasks ./schemas ./cal`
- 保護資料夾：`AlphaCity_v63_live_addon_v2`, `pkgB_profile_v6`, `pkgG_smoke_v6_2`, `pkgF_codex_patch_v6_2`, `.git`, `.venv`, `datahub`, `logs`, `metrics`, `backup`, `_audit`
- 已知映射：把 `pkg_v63_strict` 中 v6.3 檔案（scripts/tools/tasks/schemas/cal）歸位到根目錄對應位置；`pkgG_smoke_v6_2/Run-SmokeTests.ps1` 也會歸位到 `tools/`（若沒現成）
- 刪除規則：只刪 `pkg_*` 下 **與 Canonical 相同雜湊** 的副本
- 衝突：同名異雜湊 → 先備份再覆蓋 Canonical（以 v6.3 為準）
