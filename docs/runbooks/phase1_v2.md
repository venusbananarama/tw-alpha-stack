# Phase-1 v2 Runbook (SSOT)

Purpose
- Single Python entrypoint for Phase-1 ingestion (HHF + HHD).
- Canonical layout: HHF monthly `data.parquet`; HHD daily `<ds>_YYYY-MM-DD.parquet`.
- Evidence chain: `_state/mainline/<dataset>/YYYY-MM-DD.ok` and `metrics/ingest_ledger.jsonl`.

Entrypoint
- `scripts/p1_daily_routine.py`

Common modes
- Backfill HHF: `--mode hhf --run-type backfill --start YYYY-MM-DD --end YYYY-MM-DD`
- Backfill HHD: `--mode hhd --run-type backfill --start YYYY-MM-DD --end YYYY-MM-DD`
- All: `--mode all --run-type backfill --start YYYY-MM-DD --end YYYY-MM-DD`
- Live (recent missing): `--mode all --run-type live --live-lookback 5`
- Rerun without skipping: `--no-skip-if-ok`
- Force unlock stale lock: `--force-unlock --lock-ttl-mins 180`

Key inputs
- Trading calendar: `datahub/ref/trading_days.csv`
- Universe ids: `configs/investable_universe.txt`
- Token: `FINMIND_TOKEN` (required)

Outputs
- OK markers: `_state/mainline/<dataset>/YYYY-MM-DD.ok`
- Ledger: `metrics/ingest_ledger.jsonl`
- Summary: `reports/phase1_runs/<run_id>/summary.json`
- Logs: `reports/phase1_runs/<run_id>/events.log`

Layout migration (dry-run default)
- `scripts/p1_migrate_layout.py --dry-run`
- `scripts/p1_migrate_layout.py --apply`

Notes
- `prices_daily` stays at `datahub/silver/alpha/prices_daily.parquet` (canonical).
- HHF readers should only load `yyyymm=*/data.parquet`.
