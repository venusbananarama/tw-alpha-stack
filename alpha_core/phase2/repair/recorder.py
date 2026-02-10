from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from alpha_core.phase2.corelib.io import atomic_write_json


def _sha256_file(path: Path) -> Optional[str]:
    if not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


@dataclass
class RunDirRecorder:
    root: Path
    as_of: str
    run_id: str
    run_dir: Path = field(init=False)
    _params: Dict[str, Any] = field(default_factory=dict, init=False)
    _metrics: Dict[str, Any] = field(default_factory=dict, init=False)
    _tags: Dict[str, Any] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.root = self.root.resolve()
        self.run_dir = self.root / "reports" / "p2_runs" / self.as_of / self.run_id

    def start_run(self, tags: Optional[Mapping[str, Any]] = None) -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "artifacts").mkdir(parents=True, exist_ok=True)
        (self.run_dir / "attempt_logs").mkdir(parents=True, exist_ok=True)
        if tags:
            self.log_tags(dict(tags))

    def log_params(self, payload: Mapping[str, Any]) -> None:
        self._params.update(dict(payload))
        atomic_write_json(self.run_dir / "params.json", self._params, ensure_ascii=False, indent=2)

    def log_metrics(self, payload: Mapping[str, Any]) -> None:
        self._metrics.update(dict(payload))
        atomic_write_json(self.run_dir / "metrics.json", self._metrics, ensure_ascii=False, indent=2)

    def log_tags(self, payload: Mapping[str, Any]) -> None:
        self._tags.update(dict(payload))
        atomic_write_json(self.run_dir / "tags.json", self._tags, ensure_ascii=False, indent=2)

    def copy_artifact(self, src: Path, name: str) -> Path:
        dst = self.run_dir / "artifacts" / name
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_file():
            dst.write_bytes(src.read_bytes())
        return dst

    def write_artifact_json(self, rel_path: str, payload: Mapping[str, Any]) -> Path:
        dst = self.run_dir / rel_path
        dst.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(dst, dict(payload), ensure_ascii=False, indent=2)
        return dst

    def log_attempt(self, attempt_id: str, payload: Mapping[str, Any]) -> Path:
        return self.write_artifact_json(
            f"attempt_logs/{attempt_id}/attempt_summary.json",
            payload,
        )

    def write_manifest(
        self,
        *,
        resolved_paths: Mapping[str, Path],
        versions: Optional[Mapping[str, str]] = None,
        hashes: Optional[Mapping[str, str]] = None,
    ) -> Path:
        manifest: Dict[str, Any] = {
            "schema": "p2_repair_manifest.v1",
            "run_id": self.run_id,
            "as_of": self.as_of,
            "resolved_paths": {},
            "versions": dict(versions or {}),
            "hashes": dict(hashes or {}),
        }

        auto_hashes: Dict[str, str] = {}
        for key, value in resolved_paths.items():
            p = value.resolve()
            manifest["resolved_paths"][key] = str(p)
            digest = _sha256_file(p)
            if digest:
                auto_hashes[key] = digest

        merged_hashes = {**auto_hashes, **manifest["hashes"]}
        manifest["hashes"] = merged_hashes

        return self.write_artifact_json("manifest.json", manifest)

    def finalize(self, *, status: str, summary: Mapping[str, Any]) -> Path:
        payload: Dict[str, Any] = {
            "status": status,
            "summary": dict(summary),
        }
        return self.write_artifact_json("final_result.json", payload)

    def read_json(self, rel_path: str) -> Dict[str, Any]:
        path = self.run_dir / rel_path
        if not path.is_file():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}
