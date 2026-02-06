# C:\AI\tw-alpha-stack\scripts\factor_combo.py
# -*- coding: utf-8 -*-
"""
factor_combo.py

Phase-2 Step3：因子組合 CLI 入口。

用途：
- 讀取 reports/factor_eval 下所有 *_summary.json
- 根據 alpha_core.phase2.corelib.combo_lib 的邏輯計算各視窗的 score 表
- 依照去相關條件（可選）挑選每個視窗的因子組合
- 輸出 reports/factor_combo.<as_of>.json

預期由 Run-Phase2-OneClick.ps1 在 Step3 呼叫，例如：

    .\.venv\Scripts\python.exe scripts\factor_combo.py ^
        --root . ^
        --as-of 2025-11-28 ^
        --windows 6 12 24 ^
        --max-per-window 3 ^
        --corr-path reports/factor_corr_2025-11-28_w6.csv
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, List, Optional

# ---------------------------------------------------------------------------
# 把 repo root（tw-alpha-stack）加進 sys.path，讓 alpha_core 可以被 import
# ---------------------------------------------------------------------------

_THIS_FILE = Path(__file__).resolve()
_REPO_ROOT = _THIS_FILE.parents[1]  # C:\AI\tw-alpha-stack

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.phase2.corelib.combo_lib import build_combo_plan, save_combo_plan  # noqa: E402


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build factor combo plan from factor_eval summaries."
    )
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Repo root path (default: current directory).",
    )
    parser.add_argument(
        "--as-of",
        type=str,
        required=True,
        help="As-of date (W-FRI), format YYYY-MM-DD.",
    )
    parser.add_argument(
        "--windows",
        type=int,
        nargs="+",
        default=[6],
        help="Walk-forward windows in months, e.g. 6 12 24. Default: 6.",
    )
    parser.add_argument(
        "--max-per-window",
        type=int,
        default=3,
        help="Maximum number of factors per window. Default: 3.",
    )
    parser.add_argument(
        "--factors",
        type=str,
        default="",
        help=(
            "Optional comma-separated factor_id list. "
            "If omitted, all *_summary.json under reports/factor_eval will be used."
        ),
    )
    parser.add_argument(
        "--corr-path",
        type=str,
        default="",
        help=(
            "Optional path to factor correlation matrix (CSV or Parquet). "
            "If not provided or file missing, correlation will be ignored."
        ),
    )
    parser.add_argument(
        "--output",
        type=str,
        default="",
        help=(
            "Output JSON path. "
            "Default: reports/factor_combo.<as_of>.json under root."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = _parse_args(argv)

    root = Path(args.root).resolve()
    as_of = args.as_of
    windows: List[int] = [int(w) for w in args.windows]
    max_per_window: int = int(args.max_per_window)

    if args.factors:
        factor_ids = [s.strip() for s in args.factors.split(",") if s.strip()]
    else:
        factor_ids = None

    corr_path = Path(args.corr_path).resolve() if args.corr_path else None

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = root / "reports" / f"factor_combo.{as_of}.json"

    print(
        "[factor_combo] root=%s as_of=%s windows=%s max_per_window=%d"
        % (root, as_of, windows, max_per_window)
    )
    if factor_ids is not None:
        print("[factor_combo] factors=%s" % ",".join(factor_ids))
    if corr_path is not None:
        print(f"[factor_combo] corr_path={corr_path}")

    plan = build_combo_plan(
        root=root,
        as_of=as_of,
        windows=windows,
        max_factors_per_window=max_per_window,
        factor_ids=factor_ids,
        corr_path=corr_path,
    )

    save_combo_plan(plan, output_path)


if __name__ == "__main__":
    main()


