\
# FATAI All-in-One v5.1
在 v5 基礎上新增：
- `tools/check_env.ps1`：環境檢查（Python 版本、套件、ExecutionPolicy），可選 `-FixPolicy`。
- `scripts/validate_factors.py`：檢查 Parquet 是否有你要的因子欄位。
- `scripts/plot_nav.py`：從 `nav.csv` 畫出 `nav.png`（若無 matplotlib 會提示）。
- `QuickStart_Backtest.cmd`、`QuickStart_Weekly.cmd`：雙擊即可跑（內含示例參數）。
- `unified_run_backtest.ps1`、`check_weekly_after_patch.ps1`：新增 Transcript 日誌與 `-NoPause` 選項。

## 安裝
把 ZIP 解壓覆蓋到專案根目錄（例如 `G:\AI\tw-alpha-stack\`）。

## 建議先執行
```
powershell -NoProfile -ExecutionPolicy Bypass -File tools\check_env.ps1 -FixPolicy
```

## 回測（與 weekly 完全同參數制）
```
.\unified_run_backtest.ps1 `
  -Factors "composite_score mom_252_21 vol_20" `
  -OutDir "G:\AI\datahub\alpha\backtests\topN_50_W" `
  -Start "2015-01-01" -End "2020-12-31" `
  -FactorsPath "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" `
  -Config "configs\backtest_topN_example.yaml" `
  -TopN 50 -Rebalance "W" -Costs 0.0005
```

## 週度快照
```
.\check_weekly_after_patch.ps1 `
  -Factors "composite_score mom_252_21 vol_20" `
  -OutDir "G:\AI\datahub\alpha\backtests\grid_test\_weekly_check" `
  -Start "2015-01-01" -End "2020-12-31" `
  -FactorsPath "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" `
  -Config "configs\backtest_topN_fixed.yaml"
```

## 方便指令
- 開最新輸出：`.\open_last_results.ps1`（或 `-WeeklyPreview`）
- 驗證因子欄位：`python scripts\validate_factors.py --factors-path <parquet> --want "composite_score mom_252_21" --out out\validate.txt`
- NAV 畫圖：`python scripts\plot_nav.py --nav <outdir>\nav.csv`
