#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
factor_eval.py

Phase-2 factor evaluation skeleton generator.

設計目標：
- 產生 / 更新 reports/factor_eval/<factor_id>_summary.json。
- 目前處於 seed 階段，只建立 schema 完整的 skeleton（metrics 多為 null）。
- schema_version = "factor_eval.v1.1"
- windows key 一律為數字字串（"6" / "12" / "24"...），不再使用 "6m" 這種寫法。
- 之後若要接上真實評估，只需在 evaluate_single_factor() 裡補實作即可。
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

SCHEMA_VERSION = "factor_eval.v1.1"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class WindowMetrics:
    """單一 WF 視窗下的指標集合。

    註：目前多數欄位在 seed 階段會維持為 None，
    但欄位名稱要先固定下來，供後續 Gate / SLO 使用。
    """

    rank_ic: Optional[float] = None
    rank_ic_std: Optional[float] = None
    ic: Optional[float] = None
    ic_std: Optional[float] = None
    psr: Optional[float] = None
    dsr: Optional[float] = None
    turnover: Optional[float] = None
    coverage: Optional[float] = None
    max_corr: Optional[float] = None
    max_dd: Optional[float] = None

    # 過渡期兼容欄位（舊版可能使用 *_mean）
    rank_ic_mean: Optional[float] = None
    ic_mean: Optional[float] = None


@dataclass
class FactorEval:
    """單一因子的評估結果外框結構。"""

    factor_id: str
    schema_version: str
    as_of: Optional[str]
    created_at: str
    updated_at: str
    windows: Dict[str, Dict[str, Any]]  # window -> metrics dict


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_window_block() -> Dict[str, Any]:
    """建立單一視窗的欄位結構，預設全為 None。"""
    return asdict(WindowMetrics())


def build_factor_eval_skeleton(
    factor_id: str,
    wf_windows: Iterable[int],
    as_of: Optional[str] = None,
    schema_version: str = SCHEMA_VERSION,
) -> FactorEval:
    """建立單一因子的 eval JSON skeleton。"""
    now = _utc_now_iso()
    windows: Dict[str, Dict[str, Any]] = {}

    for w in wf_windows:
        key = str(int(w))
        windows[key] = init_window_block()

    return FactorEval(
        factor_id=factor_id,
        schema_version=schema_version,
        as_of=as_of,
        created_at=now,
        updated_at=now,
        windows=windows,
    )


def load_existing_eval_if_any(path: Path) -> Optional[Dict[str, Any]]:
    """若 eval JSON 已存在，讀取後回傳 dict；若不存在則回傳 None。"""
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # 保守處理：讀不到既有檔案就當作不存在，由新 skeleton 覆蓋。
        return None


def merge_existing_into_skeleton(
    skeleton: FactorEval,
    existing: Mapping[str, Any],
) -> FactorEval:
    """在合理範圍內，將既有資料 merge 回新 skeleton。

    規則（保守）：
    - 若 existing.schema_version 與目前不同，仍嘗試帶入 windows 內的數值。
    - 只對 windows 區塊嘗試比對與覆蓋，不處理其他自訂欄位。
    - 若 existing.windows 使用 "6m" 這類 key，會直接複製其內部欄位；
      但 seed 階段我們不做進一步轉換，由 compose_factors_to_wf 的 fallback 處理即可。
    """
    sk_dict = asdict(skeleton)
    windows = sk_dict.get("windows", {})

    existing_windows = existing.get("windows", {}) if isinstance(existing, Mapping) else {}
    if isinstance(existing_windows, Mapping):
        for k, v in existing_windows.items():
            if not isinstance(v, Mapping):
                continue
            # 若新 skeleton 已有對應 key，直接覆蓋；否則新增。
            if k in windows and isinstance(v, Mapping):
                merged = windows[k].copy()
                for mk, mv in v.items():
                    merged[mk] = mv
                windows[k] = merged
            else:
                windows[k] = dict(v)

    sk_dict["windows"] = windows
    # updated_at 用新的時間戳
    sk_dict["updated_at"] = _utc_now_iso()
    return FactorEval(**sk_dict)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def evaluate_single_factor(
    root: Path,
    factor_id: str,
    wf_windows: Iterable[int],
    as_of: Optional[str] = None,
) -> FactorEval:
    """評估單一因子並回傳 FactorEval 物件。

    目前 seed 階段只產生 skeleton：
      - 建立 FactorEval skeleton。
      - 若已有舊版 JSON，盡量 merge 其 windows 內容。
      - 不計算任何實際指標。
    """
    reports_dir = root / "reports" / "factor_eval"
    reports_dir.mkdir(parents=True, exist_ok=True)

    path = reports_dir / f"{factor_id}_summary.json"

    skeleton = build_factor_eval_skeleton(
        factor_id=factor_id,
        wf_windows=wf_windows,
        as_of=as_of,
    )

    existing = load_existing_eval_if_any(path)
    if existing is not None:
        result = merge_existing_into_skeleton(skeleton, existing)
    else:
        result = skeleton

    # 寫回檔案
    with path.open("w", encoding="utf-8") as f:
        json.dump(asdict(result), f, ensure_ascii=False, indent=2, sort_keys=True)

    return result


def evaluate_factors(
    root: Path,
    factor_ids: Iterable[str],
    wf_windows: Iterable[int],
    as_of: Optional[str] = None,
) -> Dict[str, Path]:
    """對多個 factor_id 進行評估，產生/更新 eval JSON。

    回傳:
        { factor_id: eval_json_path }
    """
    root = root.resolve()
    result: Dict[str, Path] = {}
    for factor_id in factor_ids:
        factor_id = str(factor_id).strip()
        if not factor_id:
            continue
        evaluate_single_factor(root=root, factor_id=factor_id, wf_windows=wf_windows, as_of=as_of)
        eval_path = root / "reports" / "factor_eval" / f"{factor_id}_summary.json"
        result[factor_id] = eval_path
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Phase-2 factor evaluation skeleton generator (factor_eval.v1.1)."
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=Path("."),
        help="Repo root directory (default: current directory).",
    )
    parser.add_argument(
        "--factor-id",
        dest="factor_ids",
        action="append",
        help="Factor ID to evaluate (can be specified multiple times).",
    )
    parser.add_argument(
        "--factor-list",
        type=Path,
        help="Path to a text file containing factor IDs (one per line).",
    )
    parser.add_argument(
        "--wf-windows",
        type=int,
        nargs="+",
        default=[6, 12, 24],
        help="Walk-forward windows in months (default: 6 12 24).",
    )
    parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="As-of date (e.g., W-FRI YYYY-MM-DD) to record in eval JSON.",
    )
    return parser.parse_args(argv)


def collect_factor_ids(ns: argparse.Namespace) -> List[str]:
    """從 CLI 參數收集 factor_id 清單。

    規則：
      - --factor-id 可重複。
      - --factor-list 可以從文字檔讀取（一行一個）。
    """
    factor_ids: List[str] = []

    if ns.factor_ids:
        factor_ids.extend(ns.factor_ids)

    if ns.factor_list:
        path: Path = ns.factor_list
        try:
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    factor_ids.append(line)
        except FileNotFoundError:
            raise SystemExit(f"factor-list file not found: {path}")

    # 去重並保留順序
    seen = set()
    uniq_ids: List[str] = []
    for fid in factor_ids:
        fid = str(fid).strip()
        if not fid or fid in seen:
            continue
        seen.add(fid)
        uniq_ids.append(fid)

    if not uniq_ids:
        raise SystemExit("No factor IDs specified. Use --factor-id or --factor-list.")

    return uniq_ids


def main(argv: Optional[List[str]] = None) -> int:
    ns = parse_args(argv)
    root: Path = ns.root

    factor_ids = collect_factor_ids(ns)
    wf_windows: List[int] = [int(w) for w in ns.wf_windows]

    evaluate_factors(
        root=root,
        factor_ids=factor_ids,
        wf_windows=wf_windows,
        as_of=ns.as_of,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
