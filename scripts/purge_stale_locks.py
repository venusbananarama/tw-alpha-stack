from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.common.lockfile import break_stale_lock  # noqa: E402


def _now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _iter_lock_files(lock_root: Path) -> List[Path]:
    if not lock_root.exists():
        return []
    if lock_root.is_file():
        if _is_phase1_b0_lock(lock_root):
            return []
        return [lock_root]
    lock_dirs: List[Path]
    if lock_root.name == "_locks":
        lock_dirs = [lock_root]
    else:
        lock_dirs = [p for p in lock_root.rglob("_locks") if p.is_dir()]
    files: List[Path] = []
    for base in lock_dirs:
        files.extend([p for p in base.rglob("*") if p.is_file()])
    files = [p for p in files if not _is_phase1_b0_lock(p)]
    return sorted(files, key=lambda p: str(p))


def _is_phase1_b0_lock(path: Path) -> bool:
    name = path.name
    if not (name.startswith("phase1.b0") and name.endswith(".lock")):
        return False
    return "_locks" in path.parts


def _summarize(info: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "path": info.get("path"),
        "reason": info.get("reason"),
        "pid": info.get("pid"),
        "hostname": info.get("hostname"),
        "created_at_utc": info.get("created_at_utc"),
        "command": info.get("command"),
        "age_minutes": info.get("age_minutes"),
        "source": info.get("source"),
    }


def _write_report(report_path: Path, report: Dict[str, Any]) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with report_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=True, indent=2, sort_keys=True)


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Purge stale lock files")
    parser.add_argument("--lock-root", required=True)
    parser.add_argument("--ttl-minutes", type=int, default=1440)
    parser.add_argument("--report-json", default=None)
    args = parser.parse_args(argv)

    lock_root = Path(args.lock_root)
    ttl_minutes = int(args.ttl_minutes)
    started_at = _now_iso()

    report: Dict[str, Any] = {
        "lock_root": str(lock_root),
        "ttl_minutes": ttl_minutes,
        "started_at_utc": started_at,
        "finished_at_utc": None,
        "counts": {"scanned": 0, "removed": 0, "kept": 0, "errors": 0},
        "removed": [],
        "kept": [],
        "errors": [],
    }

    if not lock_root.exists():
        report["counts"]["errors"] = 1
        report["errors"].append({"path": str(lock_root), "error": "lock_root_missing"})
        report["finished_at_utc"] = _now_iso()
        if args.report_json:
            _write_report(Path(args.report_json), report)
        print(f"lock_root missing: {lock_root}")
        return 2

    for path in _iter_lock_files(lock_root):
        report["counts"]["scanned"] += 1
        info = break_stale_lock(path, ttl_minutes)
        if info is None:
            report["counts"]["kept"] += 1
            report["kept"].append({"path": str(path)})
            continue
        if info.get("removed"):
            report["counts"]["removed"] += 1
            report["removed"].append(_summarize(info))
        else:
            report["counts"]["errors"] += 1
            report["errors"].append(
                {
                    "path": str(path),
                    "error": info.get("error") or "remove_failed",
                }
            )

    report["finished_at_utc"] = _now_iso()
    if args.report_json:
        _write_report(Path(args.report_json), report)

    print(
        "purge_done "
        f"scanned={report['counts']['scanned']} "
        f"removed={report['counts']['removed']} "
        f"kept={report['counts']['kept']} "
        f"errors={report['counts']['errors']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
