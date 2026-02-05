# Lock Appendix (SSOT)

Purpose
- Standard lockfile schema for auditability (pid/host/created_at/command/ttl).
- Deterministic, atomic create and safe stale handling.
- Consistent behavior across Phase-1/4/5/6 entrypoints.

Schema (JSON)
- version: int
- lock_id: string (hostname:pid:created_at_utc)
- pid: int
- hostname: string
- created_at_utc: ISO-8601 UTC string
- command: string
- ttl_minutes: int

Acquire / release
- Acquire uses atomic create (open with "x").
- Release removes the lock file.
- Active lock raises an error that includes pid/hostname/created_at_utc when present.

Stale rules
- Same hostname and pid not alive => stale (non-Windows only).
- Windows: pid liveness is not used for stale (TTL-only).
- ttl_minutes exceeded => stale.
- Legacy/non-JSON lock => only mtime is used for ttl_minutes.
- ttl_minutes <= 0 => treated as stale for break behavior.

TTL policy (current defaults)
- Phase-1: `--lock-ttl-mins` (default 180).
- Phase-4/5/6: 1440 minutes.

Phase-5 lock conflicts
- Phase-5 raises LockedError(message) without details; audit fields are included in the lock exception message.

Ops: purge tool
- `python scripts/purge_stale_locks.py --lock-root reports --ttl-minutes 1440 --report-json reports/lock_purge/lock_report.json`
- Report includes counts plus removed/kept/errors.

Verification
- Create a run and inspect the lock file content under `reports/*/_locks/*.lock`.
- Confirm stale removal by running the purge tool and checking the report JSON.
