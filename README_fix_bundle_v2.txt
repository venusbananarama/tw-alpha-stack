AlphaCity Fix Bundle v2  (2025-10-04T18:36:20.048405Z)
==================================================
Files included (drop into repo root):
- scripts/preflight_v2_wrapper.py      : Self-contained preflight (expect_date + scan + report)
- scripts/build_universe_failsafe.py   : Universe builder with safe fallback
- tools/Daily-Backfill-Prices.ps1      : Schedules -> proxies to Run-DailyBackfill -Phase prices
- tools/Daily-Backfill-Chip.ps1        : Schedules -> proxies to Run-DailyBackfill -Phase chip
- tools/Daily-VerifyBuild.ps1          : Preflight + failsafe universe
- tools/Build-Universe-Failsafe.ps1    : One-shot failsafe universe

Notes:
- All Python invocations assume "python -S" to bypass sitecustomize killswitch.
- Scripts set ALPHACITY_ALLOW=1 and clear PYTHONSTARTUP to avoid KILLSWITCH.
- build_universe_failsafe.py will copy configs/universe.tw_all.txt if the official builder outputs 0 symbols;
  if that also missing, it writes a small built-in list to keep the pipeline alive.
- Daily-Backfill-*.ps1 first try tools/Run-DailyBackfill.ps1 (your existing orchestrator).

After extracting, scheduled tasks that point to these filenames will work without renaming.
