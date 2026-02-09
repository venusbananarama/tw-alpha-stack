# scripts/p2/factor_combo.py
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Iterable, Optional, Sequence

LOG = logging.getLogger("factor_combo")


def _ensure_repo_root_on_syspath(repo_root: Path) -> None:
    """
    Allow running this script without installing the package.
    Assumption: repo_root contains 'alpha_core/'.
    """
    repo_root = repo_root.resolve()
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build factor combo plan from factor_eval summaries (Phase-2)."
    )
    p.add_argument(
        "--root",
        type=str,
        default=".",
        help="Repository root path (default: current directory).",
    )
    p.add_argument(
        "--as-of",
        type=str,
        required=True,
        help="As-of date (YYYY-MM-DD).",
    )
    p.add_argument(
        "--windows",
        type=int,
        nargs="+",
        default=[6],
        help="Walk-forward windows in months, e.g. 6 12 24. Default: 6.",
    )
    p.add_argument(
        "--max-per-window",
        type=int,
        default=3,
        help="Max factors to select per window. Default: 3.",
    )
    p.add_argument(
        "--factors",
        type=str,
        default="",
        help="Optional comma-separated factor_id list. If empty, auto-discover.",
    )
    p.add_argument(
        "--corr-path",
        type=str,
        default="",
        help="Optional correlation matrix path (csv/parquet). If missing, ignore corr.",
    )
    p.add_argument(
        "--max-corr",
        type=float,
        default=0.7,
        help="Max abs correlation threshold when corr-path is provided. Default: 0.7.",
    )
    p.add_argument(
        "--output",
        type=str,
        default="",
        help="Output JSON path. Default: <root>/reports/factor_combo.<as_of>.json",
    )
    p.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO",
    )
    return p.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    root = Path(args.root).resolve()
    _ensure_repo_root_on_syspath(root)

    try:
        from alpha_core.phase2.corelib.combo_lib import build_combo_plan, save_combo_plan  # noqa: E402
    except Exception as exc:
        LOG.error("Failed to import combo_lib from repo root=%s: %s", root, exc)
        return 1

    as_of: str = str(args.as_of).strip()
    windows = [int(w) for w in (args.windows or [6])]
    max_per_window = int(args.max_per_window)

    factor_ids = None
    if str(args.factors).strip():
        factor_ids = [s.strip() for s in str(args.factors).split(",") if s.strip()]

    corr_path = Path(args.corr_path).resolve() if str(args.corr_path).strip() else None

    if str(args.output).strip():
        output_path = Path(args.output).expanduser()
        if not output_path.is_absolute():
            output_path = (root / output_path).resolve()
    else:
        output_path = root / "reports" / f"factor_combo.{as_of}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    LOG.info(
        "factor_combo: root=%s as_of=%s windows=%s max_per_window=%d",
        root,
        as_of,
        windows,
        max_per_window,
    )
    if factor_ids is not None:
        LOG.info("  factors   = %s", ",".join(factor_ids))
    if corr_path is not None:
        LOG.info("  corr_path = %s (max_corr=%.3f)", corr_path, float(args.max_corr))
    LOG.info("  output    = %s", output_path)

    plan = build_combo_plan(
        root=root,
        as_of=as_of,
        windows=windows,
        max_factors_per_window=max_per_window,
        factor_ids=factor_ids,
        corr_path=corr_path,
        max_corr=float(args.max_corr),
        spec_version="factor_combo.v1",
    )
    save_combo_plan(plan, output_path)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
