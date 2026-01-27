from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from .errors import InputNotFoundError, LockedError
from .schemas import ArtifactNames


def default_run_id(as_of: str, profile: str) -> str:
    safe_profile = "".join(ch for ch in profile.strip() if ch.isalnum() or ch in ("-", "_"))
    safe_as_of = as_of.strip()
    return f"{safe_profile}.{safe_as_of}"


def _resolve_path(root: Path, path: str | Path) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    return (root / p).resolve()


def resolve_out_dir(
    root: str,
    as_of: str,
    run_id: Optional[str],
    profile: str,
    out_dir: Optional[str],
) -> str:
    root_path = Path(root)
    if out_dir:
        return str(_resolve_path(root_path, out_dir))
    rid = run_id if run_id else default_run_id(as_of, profile)
    return str((root_path / "reports" / "p5" / as_of / rid).resolve())


def resolve_universe_path(root: str, universe_arg: Optional[str]) -> Tuple[Optional[str], bool]:
    root_path = Path(root)
    if universe_arg:
        candidate = _resolve_path(root_path, universe_arg)
        if candidate.exists():
            return str(candidate), False
        fallback = (root_path / "investable_universe.txt").resolve()
        if fallback.exists():
            return str(fallback), True
        return None, True
    default_path = (root_path / "investable_universe.txt").resolve()
    if default_path.exists():
        return str(default_path), False
    return None, True


def _prices_candidates(root: Path) -> List[Path]:
    base = root / "datahub" / "silver" / "alpha"
    return [
        base / "prices_daily.parquet",
        base / "prices.parquet",
        base / "prices_adj.parquet",
        base / "daily_prices.parquet",
        base / "ohlcv_daily.parquet",
        base / "ohlcv.parquet",
        base / "prices_daily",
        base / "prices",
        base / "prices_adj",
        base / "daily_prices",
        base / "ohlcv_daily",
        base / "ohlcv",
    ]


def resolve_prices_path(root: str, as_of: str) -> str:
    root_path = Path(root)
    checked: List[str] = []
    for cand in _prices_candidates(root_path):
        checked.append(str(cand))
        if cand.exists():
            return str(cand.resolve())
    details = {"as_of": as_of, "checked": checked}
    raise InputNotFoundError("prices_path not found from candidates", details)


def build_resolved_paths(
    *,
    root: str,
    as_of: str,
    out_dir: str,
    universe_path: Optional[str],
    prices_path: str,
    reports_target_path: str,
    out_target_path: str,
) -> Dict[str, object]:
    return {
        "root": root,
        "as_of": as_of,
        "out_dir": out_dir,
        "universe_path": universe_path,
        "prices_path": prices_path,
        "reports_target_path": reports_target_path,
        "out_target_path": out_target_path,
    }


def acquire_lock(out_dir: str) -> str:
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    lock_path = out_path / "p5.lock"
    try:
        with lock_path.open("x", encoding="utf-8") as f:
            ts = datetime.utcnow().isoformat(timespec="seconds")
            f.write(ts)
    except FileExistsError as exc:
        raise LockedError(f"locked: {lock_path}") from exc
    return str(lock_path)


def release_lock(lock_path: str) -> None:
    try:
        Path(lock_path).unlink(missing_ok=True)
    except Exception:
        return


def known_artifacts(as_of: str) -> List[str]:
    return [
        ArtifactNames.P5_SUMMARY_JSON,
        ArtifactNames.P5_RUN_LOG,
        ArtifactNames.STRATEGY_POOL_JSON,
        ArtifactNames.STRATEGY_CORR_FILE,
        ArtifactNames.DECISION_TRACE_JSON,
        ArtifactNames.STRATEGY_ALLOC_CSV,
        ArtifactNames.TARGET_PORTFOLIO_CSV_FMT.format(as_of=as_of),
        "p5.lock",
    ]
