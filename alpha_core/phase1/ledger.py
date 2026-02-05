from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


@dataclass
class LedgerRecord:
    dataset: str
    day: str
    exit: int
    retries: int
    duration_ms: int
    run_id: str
    message: str
    qps: Optional[float] = None
    rpm: Optional[int] = None
    run_type: Optional[str] = None
    ts: Optional[str] = None

    def to_json(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["ts"] = self.ts or now_iso()
        return payload


def append_ledger(path: Path, record: LedgerRecord) -> None:
    """Append a single JSONL record.

    This project is commonly run on Windows with multiple concurrent processes
    (batching). A plain append can interleave across processes and corrupt JSONL.

    We use a lightweight lock-file protocol to ensure one-writer-at-a-time.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = record.to_json()

    lock_path = path.with_name(path.name + ".lock")
    deadline = time.time() + 30.0
    pid = os.getpid()
    acquired = False

    while time.time() < deadline:
        try:
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            with lock_path.open("x", encoding="utf-8") as lf:
                lf.write(f"pid={pid}\n")
                lf.write(f"ts={now_iso()}\n")
            acquired = True
            break
        except FileExistsError:
            time.sleep(0.05)

    if not acquired:
        # Fail-safe: write to a per-process spool to avoid losing audit trails.
        spool_dir = path.parent / "ingest_ledger_spool"
        spool_dir.mkdir(parents=True, exist_ok=True)
        spool_path = spool_dir / f"{path.stem}.spool.{pid}.jsonl"
        with spool_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True))
            f.write("\n")
        return

    try:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=True))
            f.write("\n")
    finally:
        try:
            lock_path.unlink()
        except Exception:
            return
