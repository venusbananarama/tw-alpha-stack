# AlphaCity Phase-1 Test Kit (v1)

這包提供：
- `tools/Run-Phase1-Validation.ps1` — 一鍵驗收（會寫 manifest 到 `metrics/phase1_manifest_latest.json`）
- `tools/Clean-Phase1-Rollback.ps1` — 一鍵回滾/清理（預設 Dry-Run，需 `-Apply` 才會真的刪）

## 安裝
解壓到你的專案根目錄 `G:/AI/tw-alpha-stack/`。結構會是：
- `G:/AI/tw-alpha-stack/tools/Run-Phase1-Validation.ps1`
- `G:/AI/tw-alpha-stack/tools/Clean-Phase1-Rollback.ps1`

## 用法
### 一鍵驗收
```powershell
cd G:\AI\tw-alpha-stack
pwsh -NoProfile -ExecutionPolicy Bypass -File tools\Run-Phase1-Validation.ps1
```

### 一鍵回滾/清理
Dry-Run（只顯示要刪哪些）：
```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File tools\Clean-Phase1-Rollback.ps1
```

實際刪除（含 manifest 指到的報表/日誌）：
```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File tools\Clean-Phase1-Rollback.ps1 -Deep -Apply
```

只刪某日期後建立的 `_phase1_validation` 子資料夾：
```powershell
pwsh -NoProfile -ExecutionPolicy Bypass -File tools\Clean-Phase1-Rollback.ps1 -Since 2025-09-24
```

> 安全建議：先 Dry-Run 看清單再加 `-Apply`。
