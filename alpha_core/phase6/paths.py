from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from .schemas import ResolvedPaths


class LockError(Exception):
    pass


def _resolve_path(root: Path, path: str | Path) -> Path:
    p = Path(path)
    if p.is_absolute():
        return p
    return (root / p).resolve()


def _pick_latest_dir(root: Path) -> Optional[Path]:
    if not root.exists():
        return None
    dirs = [p for p in root.iterdir() if p.is_dir()]
    if not dirs:
        return None
    return max(dirs, key=lambda p: p.stat().st_mtime)


def _resolve_exec_base(repo_root: Path, exec_root: Path, prev_exec_dir: Optional[str]) -> Path:
    if prev_exec_dir:
        cand = Path(prev_exec_dir)
        if not cand.is_absolute():
            under_exec = exec_root / cand
            if under_exec.exists():
                return under_exec.resolve()
        return _resolve_path(repo_root, prev_exec_dir)
    latest = _pick_latest_dir(exec_root)
    if latest is not None:
        return latest
    return exec_root / "latest"


def _resolve_snapshot_files(exec_base: Path) -> Tuple[Path, Path]:
    candidates = [
        exec_base / "exec_run",
        exec_base,
        exec_base / "fubon_snapshot",
    ]
    for base in candidates:
        pos = base / "positions.csv"
        acc = base / "account_snapshot.json"
        if pos.exists() or acc.exists():
            return pos, acc
    return exec_base / "exec_run" / "positions.csv", exec_base / "exec_run" / "account_snapshot.json"


def resolve_phase6_paths(
    root_dir: str | Path,
    as_of: str,
    prev_exec_dir: Optional[str] = None,
    snapshot_source: str = "exec",
) -> ResolvedPaths:
    root = Path(root_dir).resolve()
    target_csv = (root / "reports" / f"target_portfolio_{as_of}.csv").resolve()
    prices_dir = (root / "datahub" / "silver" / "alpha" / "prices").resolve()
    prices_daily = (root / "datahub" / "silver" / "alpha" / "prices_daily.parquet").resolve()
    prices_parquet = prices_dir if prices_dir.exists() else prices_daily
    calendar_csv = (root / "datahub" / "ref" / "trading_days.csv").resolve()
    lock_path = (root / "reports" / "p6" / "_locks" / f"{as_of}.lock").resolve()

    prev_exec_path: Optional[Path] = None
    prev_positions: Optional[Path] = None
    prev_account: Optional[Path] = None
    if snapshot_source == "exec":
        exec_root = (root / "reports" / "exec").resolve()
        prev_exec_path = _resolve_exec_base(root, exec_root, prev_exec_dir)
        prev_positions, prev_account = _resolve_snapshot_files(prev_exec_path)

    return {
        "root": str(root),
        "as_of": as_of,
        "out_dir": "",
        "target_csv": str(target_csv),
        "prices_parquet": str(prices_parquet),
        "calendar_csv": str(calendar_csv),
        "prev_exec_dir": str(prev_exec_path) if prev_exec_path is not None else None,
        "prev_positions_csv": str(prev_positions) if prev_positions is not None else None,
        "prev_account_json": str(prev_account) if prev_account is not None else None,
        "lock_path": str(lock_path),
        "rules_path": None,
        "benchmark_file": None,
    }


def build_out_dir(root_dir: str | Path, as_of: str, run_id: str, out_dir_override: Optional[str]) -> Path:
    root = Path(root_dir).resolve()
    if out_dir_override:
        return _resolve_path(root, out_dir_override)
    return (root / "reports" / "p6" / as_of / run_id).resolve()


def compute_run_id(inputs_hash: str, rules_hash: str, as_of: str) -> str:
    seed = f"{inputs_hash}:{rules_hash}"
    digest = _sha256_text(seed)[:12]
    return f"p6.{as_of}.{digest}"


def acquire_lock(lock_path: Path) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with lock_path.open("x", encoding="utf-8") as f:
            f.write(datetime.utcnow().isoformat(timespec="seconds"))
    except FileExistsError as exc:
        raise LockError(f"locked: {lock_path}") from exc


def release_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        return


def _sha256_text(text: str) -> str:
    import hashlib

    h = hashlib.sha256()
    h.update(text.encode("utf-8"))
    return h.hexdigest()
