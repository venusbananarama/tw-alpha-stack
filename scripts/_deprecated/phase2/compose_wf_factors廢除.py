from __future__ import annotations

#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts.compose_wf_factors

極薄的一層 CLI 入口，負責呼叫 scripts.compose_factors_to_wf.compose_factors_to_wf。

設計目標：
- 給 PS1 / 手動操作一個穩定指令：
    python .\scripts\compose_wf_factors.py --root . --mode all
- 只負責：
    1) 解析基本參數（root / wf-windows / mode / slo-profile / slo-engine / log-level）
    2) 推導預設路徑：
         rules_factors.yaml          → <root>/rules_factors.yaml
         wf_summary.json             → <root>/reports/wf_summary.json
         factor_eval/*.json          → <root>/reports/factor_eval
    3) 呼叫 compose_factors_to_wf(...)，不實作任何商業邏輯
"""

import argparse
import logging
from pathlib import Path
from typing import Optional, Sequence

# compose 邏輯在同目錄的 compose_factors_to_wf.py
from compose_factors_to_wf import compose_factors_to_wf  # type: ignore[import]


# ---------------------------------------------------------------------------
# CLI 參數解析
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compose factor_eval summaries into wf_summary.json "
            "(factors / factor_candidates / factor_slo)."
        )
    )

    parser.add_argument(
        "--root",
        "-R",
        type=str,
        default=".",
        help="Project root directory (default: current directory).",
    )
    parser.add_argument(
        "--wf-windows",
        type=int,
        nargs="+",
        default=[6, 12, 24],
        help="WF 視窗（月），預設：6 12 24。",
    )
    parser.add_argument(
        "--mode",
        type=str,
        choices=["all", "factors_only"],
        default="all",
        help="輸出模式：all=寫 factors + factor_candidates；factors_only=只寫 factors。",
    )
    parser.add_argument(
        "--slo-profile",
        type=str,
        default=None,
        help="SLO profile 名稱（例如 dev/test/live），預設不指定 profile。",
    )
    parser.add_argument(
        "--slo-engine",
        type=str,
        default="classic",
        help="SLO engine key（classic / ai），預設 classic。",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging 等級（DEBUG / INFO / WARNING / ERROR），預設 INFO。",
    )

    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    root = Path(args.root).resolve()
    if not root.exists():
        raise SystemExit(f"root path does not exist: {root}")

    rules_file = root / "rules_factors.yaml"
    wf_summary_path = root / "reports" / "wf_summary.json"
    factor_eval_dir = root / "reports" / "factor_eval"

    logging.getLogger(__name__).info(
        "compose_wf_factors: root=%s rules=%s wf=%s eval_dir=%s windows=%s mode=%s profile=%s engine=%s",
        root,
        rules_file,
        wf_summary_path,
        factor_eval_dir,
        args.wf_windows,
        args.mode,
        args.slo_profile,
        args.slo_engine,
    )

    try:
        compose_factors_to_wf(
            root=root,
            rules_file=rules_file,
            wf_summary_path=wf_summary_path,
            factor_eval_dir=factor_eval_dir,
            wf_windows=list(args.wf_windows),
            mode=args.mode,
            slo_profile=args.slo_profile,
            slo_engine=args.slo_engine,
        )
    except Exception as exc:  # noqa: BLE001
        logging.getLogger(__name__).error("compose_factors_to_wf failed: %s", exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
