from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from alpha_core.io import append_jsonlines, ensure_dir

from .errors import LockedError, OutDirNotEmptyError


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


def atomic_write_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def write_json_atomic(obj: Dict[str, Any], path: Path) -> None:
    payload = json.dumps(obj, ensure_ascii=False, indent=2, sort_keys=True, default=str)
    atomic_write_text(path, payload)


def write_parquet_atomic(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    df.to_parquet(tmp, index=False)
    tmp.replace(path)


def ensure_out_dir(out_dir: Path, *, force: bool) -> None:
    if out_dir.exists():
        has_any = any(out_dir.iterdir())
        if has_any and not force:
            raise OutDirNotEmptyError(f"out_dir not empty: {out_dir}")
        if force:
            shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)


def acquire_lock(lock_dir: Path, run_id: str) -> Path:
    ensure_dir(lock_dir)
    lock_path = lock_dir / f"{run_id}.lock"
    try:
        with lock_path.open("x", encoding="utf-8") as f:
            f.write(f"pid={os.getpid()}\n")
            f.write(f"run_id={run_id}\n")
            f.write(f"ts={now_iso()}\n")
    except FileExistsError:
        raise LockedError(f"locked: {lock_path}")
    return lock_path


def release_lock(lock_path: Path | None) -> None:
    if lock_path is None:
        return
    try:
        lock_path.unlink()
    except FileNotFoundError:
        return
    except Exception:
        return


def append_ledger(ledger_path: Path, record_dict: Dict[str, Any]) -> None:
    ensure_dir(ledger_path.parent)
    append_jsonlines(ledger_path, [record_dict])


def write_ok_flag(ok_path: Path) -> None:
    atomic_write_text(ok_path, f"ok {now_iso()}\n")


def compute_file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()
