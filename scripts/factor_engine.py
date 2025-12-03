from __future__ import annotations

"""
scripts.factor_engine

Phase-2 因子引擎的 CLI entry（命令列入口）。

角色：
- 提供穩定的 CLI 參數介面：--root / --impl-module / --rules / --factors / --start / --end 等。
- 將參數組成 FactorEngineConfig，交給 alpha_core.factor_engine.run_factor_engine 執行。
- 不實作任何商業邏輯，所有核心行為都在 alpha_core.factor_engine（單一實作）。
"""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# bootstrap：把 repo root 加進 sys.path，讓 alpha_core 可以被 import
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alpha_core.factor_engine import FactorEngineConfig, run_factor_engine  # noqa: E402


# ---------------------------------------------------------------------------
# 小工具：日期轉換
# ---------------------------------------------------------------------------


def _parse_date(s: Optional[str]) -> Optional[date]:
    """Parse YYYY-MM-DD into date; None stays None."""
    if not s:
        return None
    year, month, day = map(int, s.split("-"))
    return date(year, month, day)


# ---------------------------------------------------------------------------
# CLI 參數解析 / logging
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase-2 factor engine: compute factor parquet from silver data.",
    )
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Repository root directory (default: current directory).",
    )
    parser.add_argument(
        "--impl-module",
        type=str,
        default="alpha_core.factor_impl",
        help=(
            "Python module that implements compute_factor(root, factor_id, spec, "
            "start_date, end_date, ...) -> pandas.DataFrame. "
            "Example: alpha_core.factor_impl"
        ),
    )
    parser.add_argument(
        "--rules",
        type=str,
        default=None,
        help="Path to rules_factors.yaml (optional, used by factor_registry).",
    )
    parser.add_argument(
        "--factors",
        type=str,
        default=None,
        help=(
            "Comma-separated list of factor_ids to run. "
            "If omitted, engine will use all factors from registry."
        ),
    )
    parser.add_argument(
        "--start",
        type=str,
        default=None,
        help="Start date (YYYY-MM-DD, inclusive). If omitted, engine decides.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=None,
        help="End date (YYYY-MM-DD, exclusive). If omitted, engine decides.",
    )
    parser.add_argument(
        "--run-id-prefix",
        type=str,
        default="factor",
        help="Prefix for run_id (default: factor).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute and validate only; do not write parquet or ledger.",
    )
    parser.add_argument(
        "--max-factors",
        type=int,
        default=None,
        help="Maximum number of factors to run in this execution (default: no limit).",
    )
    parser.add_argument(
        "--factor-root",
        type=str,
        default=None,
        help=(
            "Optional override for factor parquet root directory. "
            "Default: <root>/datahub/silver/alpha/factor"
        ),
    )
    parser.add_argument(
        "--ledger-path",
        type=str,
        default=None,
        help=(
            "Optional override for factor ledger path. "
            "Default: <root>/metrics/factor_ledger.jsonl"
        ),
    )
    parser.add_argument(
        "--summary-path",
        type=str,
        default=None,
        help=(
            "Optional override for summary JSON path. "
            "Default: <root>/reports/factor_engine_summary.json"
        ),
    )
    parser.add_argument(
        "--windows",
        type=str,
        default="6,12,24",
        help=(
            "Comma-separated list of integer windows (e.g. '6,12,24'). "
            "Passed to compute_factor if it accepts a 'windows' parameter."
        ),
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    return parser.parse_args(argv)


def _configure_logging(level_name: str) -> logging.Logger:
    """統一設定 logging，避免不同入口各自定義格式。"""
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    return logging.getLogger("factor_engine")


# ---------------------------------------------------------------------------
# main：組 config → 呼叫 alpha_core.factor_engine
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    logger = _configure_logging(args.log_level)

    root = Path(args.root).resolve()
    rules_path = Path(args.rules).resolve() if args.rules else None
    start_date = _parse_date(args.start)
    end_date = _parse_date(args.end)

    factor_ids: List[str] = []
    if args.factors:
        factor_ids = [s.strip() for s in args.factors.split(",") if s.strip()]

    factor_root = Path(args.factor_root).resolve() if args.factor_root else None
    ledger_path = Path(args.ledger_path).resolve() if args.ledger_path else None
    summary_path = Path(args.summary_path).resolve() if args.summary_path else None

    # windows: 允許空字串；預設 "6,12,24"
    windows: Tuple[int, ...] = ()
    if args.windows:
        windows = tuple(
            int(x.strip()) for x in str(args.windows).split(",") if x.strip()
        )
    if not windows:
        windows = (6, 12, 24)

    cfg = FactorEngineConfig(
        root=root,
        impl_module=args.impl_module,
        rules_path=rules_path,
        factor_ids=factor_ids,
        start_date=start_date,
        end_date=end_date,
        run_id_prefix=args.run_id_prefix,
        dry_run=bool(args.dry_run),
        max_factors=args.max_factors,
        factor_root=factor_root,
        ledger_path=ledger_path,
        summary_path=summary_path,
        windows=windows,
    )

    try:
        run_factor_engine(cfg, logger=logger)
    except Exception as exc:  # noqa: BLE001
        logger.exception("Factor engine failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
