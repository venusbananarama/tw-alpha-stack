#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
scripts.factor_eval

Phase-2 factor evaluation CLI entry（命令列入口）

角色：
- 提供穩定的 CLI 介面：
    --root / --factors / --factor-id / --factor-list / --windows / --as-of / --log-level
- 收集 factor_ids 與 WF 視窗設定，交給 alpha_core.phase2.corelib.factor_eval.evaluate_factors 執行。
- 不實作評估邏輯，本身只是薄殼；評估與輸出格式由 alpha_core.phase2.corelib.factor_eval 負責。
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# bootstrap：把 repo root 加進 sys.path，讓 alpha_core 可以被 import
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from alpha_core.phase2.corelib.factor_eval import evaluate_factors  # noqa: E402


# ---------------------------------------------------------------------------
# CLI 參數解析
# ---------------------------------------------------------------------------


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase-2 factor evaluation CLI (factor_eval v1.1).",
    )
    parser.add_argument(
        "--root",
        type=str,
        default=".",
        help="Repo root directory (default: current directory).",
    )
    parser.add_argument(
        "--factors",
        type=str,
        default=None,
        help=(
            "Comma-separated list of factor IDs to evaluate, "
            "e.g. 'mom_6m,mom_12m,value_pe'. "
            "If omitted, you can use --factor-id or --factor-list."
        ),
    )
    parser.add_argument(
        "--factor-id",
        dest="factor_ids",
        action="append",
        help="Factor ID to evaluate (can be specified multiple times).",
    )
    parser.add_argument(
        "--factor-list",
        type=str,
        default=None,
        help="Path to a text file containing factor IDs (one per line).",
    )
    parser.add_argument(
        "--windows",
        type=str,
        default="6,12,24",
        help=(
            "Comma-separated list of walk-forward windows in months "
            "(default: '6,12,24')."
        ),
    )
    parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="As-of date (e.g., W-FRI YYYY-MM-DD) to record in eval JSON.",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# logging
# ---------------------------------------------------------------------------


def _configure_logging(level_name: str) -> logging.Logger:
    """統一設定 logging，避免不同入口各自定義格式。"""
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    return logging.getLogger("factor_eval")


# ---------------------------------------------------------------------------
# factor_id 收集
# ---------------------------------------------------------------------------


def _collect_factor_ids(ns: argparse.Namespace, logger: logging.Logger) -> List[str]:
    """
    從 CLI 參數收集 factor_id 清單。

    優先順序：
      1) --factors（逗號分隔）
      2) 多個 --factor-id
      3) --factor-list 檔案（一行一個；忽略空行與 # 開頭）
    """
    factor_ids: List[str] = []

    # 1) --factors（逗號分隔）
    if ns.factors:
        for token in str(ns.factors).split(","):
            token = token.strip()
            if token:
                factor_ids.append(token)

    # 2) --factor-id（可多次）
    if ns.factor_ids:
        for fid in ns.factor_ids:
            if not fid:
                continue
            s = str(fid).strip()
            if s:
                factor_ids.append(s)

    # 3) --factor-list 檔案
    if ns.factor_list:
        path = Path(str(ns.factor_list))
        if not path.exists():
            logger.error("factor-list file not found: %s", path)
        else:
            try:
                with path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        factor_ids.append(line)
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to read factor-list file %s: %r", path, exc)

    # 4) 去重並保留順序
    seen = set()
    uniq_ids: List[str] = []
    for fid in factor_ids:
        fid = str(fid).strip()
        if not fid or fid in seen:
            continue
        seen.add(fid)
        uniq_ids.append(fid)

    if not uniq_ids:
        logger.error(
            "No factor IDs specified. Use --factors, --factor-id, or --factor-list."
        )

    return uniq_ids


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None):  # -> int:
    ns = _parse_args(argv)
    logger = _configure_logging(ns.log_level)

    # 正規化 root
    root = Path(ns.root)
    try:
        root = root.resolve()
    except Exception:  # noqa: BLE001
        # 失敗就用原值，不中斷
        pass

    factor_ids = _collect_factor_ids(ns, logger)
    if not factor_ids:
        # 已在 _collect_factor_ids 中 log error
        return 1

    # windows: 逗號分隔字串 → int list
    windows: Tuple[int, ...] = ()
    if ns.windows:
        tokens = str(ns.windows).split(",")
        wins: List[int] = []
        for tok in tokens:
            tok = tok.strip()
            if not tok:
                continue
            try:
                wins.append(int(tok))
            except ValueError:
                logger.warning("Ignore invalid window value: %r", tok)
        windows = tuple(sorted(set(wins)))
    if not windows:
        windows = (6, 12, 24)

    logger.info(
        "Evaluating %d factors on windows=%s as_of=%s",
        len(factor_ids),
        list(windows),
        ns.as_of,
    )

    try:
        result_map = evaluate_factors(
            root=root,
            factor_ids=factor_ids,
            wf_windows=list(windows),
            as_of=ns.as_of,
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Factor evaluation failed: %s", exc)
        return 1

    # 成功時簡單列出產出的 JSON 路徑（方便人工 debug）
    for fid, path in result_map.items():
        logger.info("Eval JSON for %s: %s", fid, path)

    logger.info("Factor evaluation completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

