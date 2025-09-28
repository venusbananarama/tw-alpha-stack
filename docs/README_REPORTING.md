# tw-alpha-reporting（報表輸出 + 批量回測）
需求：Python 3.10+、pandas、numpy、matplotlib、pyyaml（可選 jinja2）

## 單次報表
(.venv) python scripts/make_report.py ^
  --nav-csv G:\AI\datahub\alpha\backtests\topN_50_M\nav.csv ^
  --date-col date --value-col nav ^
  --benchmark-csv G:\AI\data\bench\taiex_close.csv --bench-type close ^
  --bench-date-col date --bench-value-col close ^
  --out-dir G:\AI\datahub\alpha\backtests\topN_50_M ^
  --freq D ^
  --title "TopN=50 Composite"

## 批量回測（自動報表）
(.venv) python scripts/batch_backtest.py ^
  --factors-path G:\AI\datahub\alpha\alpha_factors.parquet ^
  --base-config configs\backtest_topN_fixed.yaml ^
  --combos configs\batch_backtest.example.yaml ^
  --out-root G:\AI\datahub\alpha\backtests\grid_test ^
  --jobs 2 ^
  --engine script ^
  --make-report ^
  --freq D
  