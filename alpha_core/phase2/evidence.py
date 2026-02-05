from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Dict, Mapping

from .contracts import now_iso
from . import paths


def _copy_if_small(src: Path, dest_dir: Path, max_bytes: int = 2_000_000) -> str | None:
    if not src.is_file():
        return None
    try:
        size = src.stat().st_size
    except OSError:
        return None
    if size > max_bytes:
        return None
    dest = dest_dir / src.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dest)
    return str(dest)


def build_evidence_pack(
    *,
    root: Path,
    run_id: str,
    artefacts: Mapping[str, Path],
) -> Path:
    root = root.resolve()
    evidence_dir = paths.evidence_dir(root, run_id)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    manifest_items: Dict[str, Dict[str, object]] = {}
    for name, path in artefacts.items():
        exists = path.is_file()
        copied = None
        if exists and path.suffix.lower() in (".json", ".csv", ".txt"):
            copied = _copy_if_small(path, evidence_dir)
        manifest_items[name] = {
            "path": str(path),
            "exists": exists,
            "copied": copied,
        }

    manifest = {
        "schema": "phase2_evidence.v1",
        "run_id": run_id,
        "generated": now_iso(),
        "artefacts": manifest_items,
    }
    manifest_path = evidence_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return evidence_dir
