from __future__ import annotations

from datetime import date
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def datahub_root(root: Path | None = None) -> Path:
    root = root or repo_root()
    return root / "datahub"


def silver_root(root: Path | None = None) -> Path:
    return datahub_root(root) / "silver" / "alpha"


def state_root(root: Path | None = None) -> Path:
    root = root or repo_root()
    return root / "_state" / "mainline"


def ledger_path(root: Path | None = None) -> Path:
    root = root or repo_root()
    return root / "metrics" / "ingest_ledger.jsonl"


def reports_root(root: Path | None = None) -> Path:
    root = root or repo_root()
    return root / "reports"


def run_dir(root: Path | None, run_id: str) -> Path:
    base = reports_root(root)
    return base / "phase1_runs" / run_id


def p1_gate_summary_path(root: Path | None = None) -> Path:
    base = reports_root(root)
    return base / "p1" / "gate_summary.json"


def api_rate_root(root: Path | None = None) -> Path:
    base = reports_root(root)
    return base / "_state" / "api_rate"


def finmind_bucket_state_path(root: Path | None = None) -> Path:
    return api_rate_root(root) / "finmind_bucket.json"


def finmind_bucket_lock_path(root: Path | None = None) -> Path:
    base = reports_root(root)
    return base / "_locks" / "finmind_bucket.lock"


def dividend_scan_state_path(root: Path | None = None) -> Path:
    base = reports_root(root)
    return base / "_state" / "phase1" / "dividend_scan_state.json"


def dividend_scan_lock_path(root: Path | None = None) -> Path:
    base = reports_root(root)
    return base / "_locks" / "dividend_scan_state.lock"


def dividend_evidence_path(root: Path | None, day: date) -> Path:
    base = state_root(root)
    return base / "dividend" / f"{day.isoformat()}.evidence.json"


def lock_path(root: Path | None = None, scope: str | None = None) -> Path:
    """Return Phase-1 lock path.

    Backward compatible:
      - scope is None or "global" -> reports/_locks/phase1.lock
      - scope is any other non-empty value -> reports/_locks/phase1.<scope>.lock

    This enables running multiple non-overlapping Phase-1 batches concurrently,
    as long as they do not write to the same dataset outputs.
    """
    base = reports_root(root) / "_locks"
    if not scope or scope.strip().lower() in ("global", "phase1"):
        return base / "phase1.lock"
    safe = "".join(ch for ch in scope.strip() if ch.isalnum() or ch in ("-", "_", "."))
    if not safe:
        return base / "phase1.lock"
    return base / f"phase1.{safe}.lock"


def ok_path(root: Path | None, dataset: str, day: date) -> Path:
    base = state_root(root)
    return base / dataset / f"{day.isoformat()}.ok"


def prices_daily_path(root: Path | None = None) -> Path:
    return silver_root(root) / "prices_daily.parquet"


def prices_daily_ok_path(root: Path | None, day: date) -> Path:
    base = state_root(root)
    return base / "prices_daily" / f"{day.isoformat()}.ok"
