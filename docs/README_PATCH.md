# FATAI Patch Bundle (2025-09-16)

This bundle contains:
1. **Fixed** `src/twalpha/backtest/engine.py` (corrected function signature).
2. **New** `scripts/check_weekly_after_patch.ps1` with `-ConfigPath` (alias `-Config`) to avoid the parameter error.
3. **New** `scripts/weekly_factors_check.py` (weekly Spearman IC report).
4. **New** `backtest/longonly_topN.py` (equal-weight TopN weekly backtest).
5. **New** `configs/backtest_topN_fixed.yaml` (example config).

## Usage

### A) Weekly factor check
```powershell
.\scripts\check_weekly_after_patch.ps1 `
  -Factors "composite_score mom_252_21 vol_20" `
  -OutDir "G:\AI\datahublphaacktests\grid_test\_weekly_check" `
  -Start "2015-01-01" `
  -End "2020-12-31" `
  -Config "configs\backtest_topN_fixed.yaml" `
  -FactorsPath "G:\AI\datahublphalpha_factors_fixed.parquet"
```

Outputs: `weekly_ic.csv`, `summary.csv`, `REPORT.md`.

### B) Long-only TopN backtest (smoke run)
```powershell
py backtest\longonly_topN.py `
  --factors "G:\AI\datahublphalpha_factors_fixed.parquet" `
  --out-dir "G:\AI\datahublphaacktests	opN_50_M" `
  --config "configs\backtest_topN_fixed.yaml"
```

Outputs: `nav.csv`, `daily_ret.csv`, `weights.csv`, `stats.json`.

> Tip: Ensure your parquet has columns: `date`, `symbol`, `close`, `composite_score`.
> If your price column differs, edit the YAML (`price_col:`).

---

**Safe merge:** copy these files into your repo root preserving folders.
