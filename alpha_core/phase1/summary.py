from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def now_iso() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat()


@dataclass
class RunSummary:
    run_id: str
    mode: str
    run_type: str
    started_at: str = field(default_factory=now_iso)
    finished_at: Optional[str] = None
    status: str = "RUNNING"
    tasks: List[Dict[str, Any]] = field(default_factory=list)
    counts: Dict[str, int] = field(default_factory=lambda: {"ok": 0, "fail": 0, "skip": 0})
    meta: Dict[str, Any] = field(default_factory=dict)

    def record_task(self, payload: Dict[str, Any]) -> None:
        self.tasks.append(payload)
        status = payload.get("status")
        if status in self.counts:
            self.counts[status] += 1

    def finalize(self) -> None:
        self.finished_at = now_iso()
        if self.counts.get("fail", 0) > 0:
            self.status = "FAIL"
        else:
            self.status = "OK"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "mode": self.mode,
            "run_type": self.run_type,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status": self.status,
            "counts": dict(self.counts),
            "meta": dict(self.meta),
            "tasks": list(self.tasks),
        }


def write_summary(path: Path, summary: RunSummary) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(summary.to_dict(), ensure_ascii=True, indent=2)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(payload, encoding="utf-8")
    tmp.replace(path)
