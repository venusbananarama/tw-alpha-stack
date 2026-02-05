#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path
from typing import List, Optional, Tuple

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.phase1 import ingest_engine  # noqa: E402


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Phase-1 DateID ingest entrypoint.")
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--day", required=True, help="YYYY-MM-DD")
    ap.add_argument("--ids", required=True, help="Comma-separated stock IDs")
    ap.add_argument("--repo-root", default=None)
    ap.add_argument("--datahub-root", default=None)
    ap.add_argument("--calls-per-hour", type=int, default=6000)
    ap.add_argument("--batch-size", type=int, default=None)
    ap.add_argument("--dateid-source", default=None)
    ap.add_argument("--config", default=None)
    ap.add_argument(
        "--gov-bank-bearer-env",
        default="FINMIND_GOV_BANK_BEARER",
        help="Env var name for gov_bank bearer token.",
    )
    ap.add_argument("--log-dir", default=None)
    return ap.parse_args(argv)


def _resolve_paths(args: argparse.Namespace) -> Tuple[Path, Path, Optional[Path]]:
    repo_root = Path(args.repo_root).resolve() if args.repo_root else _REPO_ROOT
    datahub_root = Path(args.datahub_root).resolve() if args.datahub_root else repo_root / "datahub"
    log_dir = Path(args.log_dir).resolve() if args.log_dir else None
    return repo_root, datahub_root, log_dir


def _emit_factory(log_dir: Optional[Path]) -> ingest_engine.LogFn:
    def _emit(message: str, err: bool) -> None:
        target = sys.stderr if err else sys.stdout
        print(message, file=target)
        if log_dir is not None:
            log_dir.mkdir(parents=True, exist_ok=True)
            log_path = log_dir / "p1_dateid_ingest.log"
            with log_path.open("a", encoding="utf-8") as f:
                f.write(message + "\n")

    return _emit


def _parse_ids(raw: str) -> List[str]:
    ids: List[str] = []
    for token in raw.split(","):
        t = token.strip()
        if t:
            ids.append(t)
    return ids


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    repo_root, datahub_root, log_dir = _resolve_paths(args)
    day = dt.date.fromisoformat(args.day)
    ids = _parse_ids(args.ids)

    emit = _emit_factory(log_dir)
    emit(f"START dataset={args.dataset} day={day.isoformat()} repo_root={repo_root}", False)

    config_path = Path(args.config).resolve() if args.config else None
    try:
        result = ingest_engine.ingest_dateid(
            dataset=args.dataset,
            day=day,
            ids=ids,
            repo_root=repo_root,
            datahub_root=datahub_root,
            calls_per_hour=args.calls_per_hour,
            config_path=config_path,
            gov_bank_bearer_env=args.gov_bank_bearer_env,
            log=emit,
        )
    except Exception as exc:
        emit(
            f"FAIL dataset={args.dataset} day={day.isoformat()} reason={exc}",
            True,
        )
        return 2

    status = "SKIP" if result.skipped else "OK"
    emit(
        f"FINISH status={status} dataset={args.dataset} day={day.isoformat()} "
        f"rows_written={result.rows_written} duration_sec={result.duration_sec:.2f}",
        False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
