#!/usr/bin/env python3
"""
scripts/clean_project.py
- Classify and optionally archive/delete temp & patch files
- Default is DRY-RUN (no changes). Use --apply to move/delete.
- Never touches datahub/* by default.
"""
from __future__ import annotations
import argparse, re, shutil, sys
from pathlib import Path
from datetime import datetime

PATTERNS_ARCHIVE = [
    r"^patch_backtest_core_v\d+\.py$",
    r"^replace_longonly_topN_v\d+\.py$",
    r"^fix_factor_columns.*\.py$",
    r"^patch_fix_rbdates.*\.py$",
    r".*\.bak_\d{8}_\d{6}\.py$",
]
PATTERNS_DELETE = [
    r"__pycache__[/\\]?.*",
    r".*\.pyc$", r".*\.pyo$",
]

def match_any(name: str, pats) -> bool:
    return any(re.fullmatch(p, name) for p in pats)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".", help="Project root")
    ap.add_argument("--apply", action="store_true", help="Actually move/delete (otherwise DRY-RUN)")
    ap.add_argument("--archive-dir", default=None, help="Archive folder (default: ./archive_YYYYmmdd_HHMMSS)")
    ap.add_argument("--include-datahub", action="store_true", help="Allow actions under datahub/")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    archive = Path(args.archive_dir) if args.archive_dir else root / f"archive_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    to_move, to_delete = [], []

    for p in root.rglob("*"):
        if p.is_dir():
            # delete __pycache__ directories
            rel = p.relative_to(root).as_posix()
            if re.fullmatch(PATTERNS_DELETE[0], rel):
                to_delete.append(p)
            continue
        rel = p.relative_to(root).as_posix()
        # skip under datahub unless allowed
        if not args.include_datahub and rel.startswith("datahub/"):
            continue
        name = p.name
        if match_any(name, PATTERNS_ARCHIVE):
            to_move.append(p)
        elif match_any(name, PATTERNS_DELETE[1:]):
            to_delete.append(p)

    print(f"Root = {root}")
    print(f"Archive = {archive}")
    mode = "REAL" if args.apply else "DRY-RUN"
    print(f"Mode = {mode}")

    if not args.apply:
        for p in to_move:
            print(f"[DRYRUN][MOVE] {p}")
        for p in to_delete:
            print(f"[DRYRUN][DEL ] {p}")
        return

    archive.mkdir(parents=True, exist_ok=True)
    for p in to_move:
        dst = archive / p.name
        try:
            shutil.move(str(p), str(dst))
            print(f"[MOVED] {p.name}")
        except Exception as e:
            print(f"[SKIP] {p} -> {e}")
    for p in to_delete:
        try:
            if p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
            else:
                p.unlink(missing_ok=True)
            print(f"[DELETED] {p}")
        except Exception as e:
            print(f"[SKIP] {p} -> {e}")

if __name__ == "__main__":
    main()
