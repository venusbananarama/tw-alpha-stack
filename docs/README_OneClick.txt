\
# OneClick_RunAll.ps1 使用說明

一鍵完成：環境檢查 → 回測/週度快照 → 自動開啟結果。

## 用法

- 預設跑回測：
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File OneClick_RunAll.ps1
```

- 指定模式為週度快照：
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File OneClick_RunAll.ps1 -Mode Weekly
```

- 如果不想最後停在 Pause：
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File OneClick_RunAll.ps1 -Mode Backtest -NoPause
```

## 內建參數
- Factors: `"composite_score mom_252_21 vol_20"`
- OutDir:
  - 回測：`out\OneClick_Backtest`
  - 週度快照：`out\OneClick_Weekly`
- 日期區間：2015-01-01 到 2020-12-31
- TopN=50, Rebalance=W, Costs=0.0005

如需修改，可直接打開 `OneClick_RunAll.ps1` 編輯。
