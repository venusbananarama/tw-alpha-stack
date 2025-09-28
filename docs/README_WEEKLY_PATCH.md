# Weekly Rebalance Fix (W-FRI)

This patch updates `backtest/core.py` so that **weekly** rebalance uses
week-ending **Friday** (`W-FRI`) and filters out weekends before grouping.  
It prevents selecting weekend timestamps that lead to `no positions produced`.

## Files
- `backtest_patch_core_weekly_fri.py` – patcher script
- `check_weekly_after_patch.ps1` – convenience validator using your existing `scripts/project_check.py`

## How to use
1. Place both files into your repository root (same folder as `backtest/`).
2. Apply patch:
   ```powershell
   .\.venv\Scripts\python.exe backtest_patch_core_weekly_fri.py backtest\core.py
   ```
   You should see:
   ```
   [weekly-fix] Patched OK -> backtest\core.py
   [weekly-fix] Backup     -> backtest\core.py.bak_YYYYMMDD_HHMMSS
   ```
3. Validate weekly counts (ensure there are enough non-null factors on weekly dates):
   ```powershell
   .\check_weekly_after_patch.ps1 `
     -Factors "G:\AI\datahub\alpha\alpha_factors_fixed.parquet" `
     -OutDir  "G:\AI\datahub\alpha\backtests\grid_test\_diag_after_patch" `
     -FactorsList "composite_score mom_252_21 vol_20" `
     -Start "2015-01-01" -End "2020-12-31"
   ```
4. Re-run your batch (e.g., `launchers\run_all.ps1`).

## Notes
- The patch is idempotent-safe: it backs up the original file and only replaces
  the **weekly** branch; daily/monthly remain unchanged.
- If your `core.py` is highly customized and patterns don't match, the patcher
  will not overwrite anything and will print a warning instead.
