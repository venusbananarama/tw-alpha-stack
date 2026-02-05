from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class CheckpointStore:
    root: Path

    def ok_path(self, dataset: str, day: date) -> Path:
        return self.root / dataset / f"{day.isoformat()}.ok"

    def exists(self, dataset: str, day: date) -> bool:
        return self.ok_path(dataset, day).is_file()

    def write_ok(self, dataset: str, day: date) -> Path:
        path = self.ok_path(dataset, day)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(f"ok {datetime.utcnow().isoformat()}\n", encoding="utf-8")
        tmp.replace(path)
        return path

    def remove_ok(self, dataset: str, day: date) -> None:
        path = self.ok_path(dataset, day)
        if path.exists():
            path.unlink()

    def latest_ok(self, dataset: str) -> Optional[date]:
        dir_path = self.root / dataset
        if not dir_path.is_dir():
            return None
        latest: Optional[date] = None
        for p in dir_path.glob("*.ok"):
            try:
                d = date.fromisoformat(p.stem)
            except Exception:
                continue
            if latest is None or d > latest:
                latest = d
        return latest
