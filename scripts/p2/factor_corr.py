# C:\AI\tw-alpha-stack\scripts\factor_corr.py
#!/usr/bin/env python
"""
factor_corr.py

Phase-2「corr」階段的單一入口 Script。

功能：
- 從 factor parquet 或預先產好的 panel 寬表讀取因子資料。
- 呼叫 alpha_core.phase2.corelib.corr_lib 計算：
    1) 相關矩陣（corr matrix）
    2) 攤平後的 pair list
    3) max |corr| 等摘要資訊
- 輸出：
    - reports/factor_corr/corr_matrix_<as_of>_<window>.parquet 或 .csv
    - reports/factor_corr/corr_pairs_<as_of>_<window>.parquet 或 .csv
    - reports/factor_corr/corr_summary_<as_of>_<window>.json

設計重點：
- CLI 介面與 Run-Phase2-OneClick.ps1 對齊（root/rules/as-of/windows/engine/profile/panel-source）。
- I/O 與 orchestration 集中在這支 script，數學計算委託 alpha_core.phase2.corelib.corr_lib。
- 不建立 wrapper / alias，只提供單一入口檔。
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass, asdict
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

# ---------------------------------------------------------------------------
# 確保 repo root 在 sys.path，讓 alpha_core 可以被 import
# ---------------------------------------------------------------------------

_THIS_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _THIS_DIR.parents[1]  # C:\AI\tw-alpha-stack

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from alpha_core.phase2.corelib.corr_lib import (  # type: ignore[import]
    compute_corr_matrix,
    corr_matrix_to_pairs,
    summarize_corr,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


class InsufficientFactorsError(RuntimeError):
    pass


@dataclass
class FactorCorrJob:
    """
    封裝一次相關性計算所需的設定（針對單一 window）。
    """

    root: Path
    input_path: Path
    columns: Optional[List[str]]
    method: str
    min_periods: int
    as_of: str
    window_label: str
    output_format: str  # 'parquet' or 'csv'

    @property
    def output_dir(self) -> Path:
        return self.root / "reports" / "factor_corr"

    @property
    def matrix_path(self) -> Path:
        ext = "parquet" if self.output_format == "parquet" else "csv"
        name = f"corr_matrix_{self.as_of}_{self.window_label}.{ext}"
        return self.output_dir / name

    @property
    def pairs_path(self) -> Path:
        ext = "parquet" if self.output_format == "parquet" else "csv"
        name = f"corr_pairs_{self.as_of}_{self.window_label}.{ext}"
        return self.output_dir / name

    @property
    def summary_path(self) -> Path:
        name = f"corr_summary_{self.as_of}_{self.window_label}.json"
        return self.output_dir / name


# ---------------------------------------------------------------------------
# Helpers: generic I/O
# ---------------------------------------------------------------------------


def _load_input_frame(path: Path) -> pd.DataFrame:
    """
    根據副檔名讀取 CSV 或 Parquet，回傳 DataFrame。
    """
    if not path.exists():
        raise FileNotFoundError(f"input file not found: {path}")

    suffix = path.suffix.lower()
    if suffix == ".csv":
        df = pd.read_csv(path)
    elif suffix in (".parquet", ".pq"):
        df = pd.read_parquet(path)
    else:
        raise ValueError(f"unsupported input file type: {suffix!r} (only .csv / .parquet)")

    if df.empty:
        raise ValueError(f"input DataFrame is empty: {path}")
    return df


def _ensure_output_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _write_frame(df: pd.DataFrame, path: Path, *, index: bool) -> None:
    """
    依照副檔名寫出 CSV 或 Parquet。
    """
    _ensure_output_dir(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(path, index=index)
    elif suffix in (".parquet", ".pq"):
        df.to_parquet(path, index=index)
    else:
        raise ValueError(f"unsupported output file type: {suffix!r} (only .csv / .parquet)")


# ---------------------------------------------------------------------------
# Helpers: factor plan / windows 解析
# ---------------------------------------------------------------------------


def parse_windows_arg(windows_str: str) -> List[int]:
    """
    將 '6,12,24' parse 成 [6, 12, 24]。
    """
    parts = [p.strip() for p in windows_str.split(",") if p.strip()]
    if not parts:
        raise ValueError("windows string is empty")
    windows: List[int] = []
    for p in parts:
        try:
            value = int(p)
        except ValueError as exc:  # noqa: PERF203
            raise ValueError(f"Invalid window value in --windows: {p!r}") from exc
        if value <= 0:
            raise ValueError(f"Window months must be positive, got {value}")
        windows.append(value)
    return windows


def parse_factors_arg(factors_str: str) -> List[str]:
    """
    將 'a,b,c' parse 成 ['a', 'b', 'c']，並去重保持順序。
    """
    parts = [p.strip() for p in factors_str.split(",") if p.strip()]
    if not parts:
        raise ValueError("factors string is empty")
    return list(dict.fromkeys(parts))


def _is_insufficient_panel_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "no usable factor series" in msg or "no parquet data found" in msg


def window_label_for_months(months: int) -> str:
    """
    6 → '6m', 12 → '12m' 等，用於輸出檔名。
    """
    return f"{int(months)}m"


def _iter_months_backward(as_of: date, months: int) -> List[Tuple[int, int]]:
    """
    從 as-of 所在月份起，往前數 months 個月份，回傳 (year, month) 列表。

    例如：as_of=2025-11-10, months=6 → [(2025, 11), (2025, 10), ..., (2025, 6)]
    """
    year = as_of.year
    month = as_of.month
    out: List[Tuple[int, int]] = []
    for _ in range(months):
        out.append((year, month))
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return out


def load_factor_plan(root: Path, as_of: str, engine: str) -> Dict:
    """
    讀取 reports/factor_plan.<as_of>.<engine>.json。
    """
    plan_path = root / "reports" / f"factor_plan.{as_of}.{engine}.json"
    if not plan_path.exists():
        raise FileNotFoundError(f"factor plan not found: {plan_path}")
    with plan_path.open("r", encoding="utf-8") as f:
        plan = json.load(f)
    return plan


def select_active_factors(plan: Dict) -> List[str]:
    """
    從 factor_plan JSON 中挑選 decided_action in (compute+eval, eval_only) 的因子。
    """
    items: Sequence[Dict] = plan.get("items", []) or []
    active: List[str] = []
    for item in items:
        if item.get("decided_action") not in ("compute+eval", "eval_only"):
            continue
        fid = item.get("factor_id")
        if not fid:
            continue
        active.append(str(fid))
    # 去重排序，保持穩定輸出
    return sorted(dict.fromkeys(active))


# ---------------------------------------------------------------------------
# Panel builder（panel_source = factor_parquet）
# ---------------------------------------------------------------------------


def _infer_value_column(df: pd.DataFrame, factor_id: str) -> str:
    """
    嘗試推斷因子值欄位名稱。
    """
    for candidate in ("value", factor_id, "factor_value"):
        if candidate in df.columns:
            return candidate
    raise ValueError(
        f"cannot infer value column for factor {factor_id!r}; "
        f"expected one of: 'value', '{factor_id}', 'factor_value'"
    )


def _filter_by_window(df: pd.DataFrame, window_months: int, window_label: str) -> pd.DataFrame:
    """
    依照 parquet 內的 window 欄位（若有）過濾到指定 window。
    """
    if "window_months" in df.columns:
        return df[df["window_months"] == int(window_months)]
    if "window" in df.columns:
        return df[df["window"] == int(window_months)]
    if "window_label" in df.columns:
        return df[df["window_label"] == window_label]
    # 沒有 window 欄位就直接原樣返回
    return df


def build_panel_from_factor_parquet(
    root: Path,
    factor_ids: Sequence[str],
    as_of_str: str,
    window_months: int,
) -> Path:
    """
    從 factor parquet 建立寬表 panel（rows: date / stock_id, cols: factor_id），
    並寫到 reports/factor_corr/panel_<as_of>_<window_label>.parquet。
    """
    if not factor_ids:
        raise ValueError("factor_ids is empty; nothing to build panel from")

    factor_root = root / "datahub" / "silver" / "alpha" / "factor"
    as_of_dt = pd.to_datetime(as_of_str).date()
    yms = _iter_months_backward(as_of_dt, window_months)
    window_label = window_label_for_months(window_months)

    series_list: List[pd.Series] = []

    for fid in factor_ids:
        frames: List[pd.DataFrame] = []
        for year, month in yms:
            yyyymm = year * 100 + month
            part_dir = factor_root / fid / f"yyyymm={yyyymm:04d}"
            if not part_dir.exists():
                continue
            for fp in sorted(part_dir.glob("*.parquet")):
                try:
                    df_part = pd.read_parquet(fp)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("failed to read parquet for factor %s from %s: %s", fid, fp, exc)
                    continue
                if df_part.empty:
                    continue
                frames.append(df_part)

        if not frames:
            logger.warning(
                "no parquet data found for factor %s in last %d months (up to %s)",
                fid,
                window_months,
                as_of_str,
            )
            continue

        df = pd.concat(frames, ignore_index=True)

        if "date" not in df.columns:
            raise ValueError(f"factor parquet for {fid!r} has no 'date' column")

        df["date"] = pd.to_datetime(df["date"])
        df = df[df["date"] <= pd.Timestamp(as_of_dt)]

        df = _filter_by_window(df, window_months=window_months, window_label=window_label)

        # 判斷橫截面 key（預設 date + stock_id 或 symbol）
        key_cols: List[str] = ["date"]
        if "stock_id" in df.columns:
            key_cols.append("stock_id")
        elif "symbol" in df.columns:
            key_cols.append("symbol")

        value_col = _infer_value_column(df, factor_id=fid)

        # 建立 MultiIndex Series：index = key_cols，name=factor_id
        s = df.set_index(key_cols)[value_col].rename(fid)
        series_list.append(s)

    if not series_list:
        raise ValueError(
            f"no usable factor series for any of factors={list(factor_ids)!r} "
            f"(window={window_months}m, as_of={as_of_str})"
        )

    panel = pd.concat(series_list, axis=1, join="inner")
    panel = panel.sort_index()

    panel_dir = root / "reports" / "factor_corr"
    panel_path = panel_dir / f"panel_{as_of_str}_{window_label}.parquet"
    _ensure_output_dir(panel_path)
    panel.to_parquet(panel_path)
    logger.info(
        "written factor panel to %s; shape=%s, n_factors=%d",
        panel_path,
        panel.shape,
        len(series_list),
    )
    return panel_path


def resolve_panel_path_from_args(
    root: Path,
    as_of: str,
    window_months: int,
    panel_source: str,
) -> Path:
    """
    依 panel_source 決定 panel 檔案位置。

    - factor_parquet：不在此函式處理（由 build_panel_from_factor_parquet 生成）。
    - panel_parquet：假設已有 panel_<as_of>_<window>.parquet 存在於 reports/factor_corr。
    """
    window_label = window_label_for_months(window_months)
    if panel_source == "panel_parquet":
        panel_path = root / "reports" / "factor_corr" / f"panel_{as_of}_{window_label}.parquet"
        if not panel_path.exists():
            raise FileNotFoundError(
                f"panel_source=panel_parquet, but panel file not found: {panel_path}"
            )
        return panel_path

    raise ValueError(f"resolve_panel_path_from_args called with unsupported panel_source={panel_source!r}")


# ---------------------------------------------------------------------------
# Core corr logic
# ---------------------------------------------------------------------------


def run_corr_job(job: FactorCorrJob) -> None:
    """
    執行一次相關性計算與輸出。
    """
    logger.info("factor_corr job started: %s", asdict(job))

    # 1) 讀取輸入寬表
    frame = _load_input_frame(job.input_path)
    logger.info("loaded input frame: shape=%s from %s", frame.shape, job.input_path)

    selected_columns = None
    if job.columns is not None:
        requested = list(dict.fromkeys(job.columns))
        available = [c for c in requested if c in frame.columns]
        missing = [c for c in requested if c not in frame.columns]
        use_columns = available
        log_fn = logger.warning if missing else logger.info
        log_fn(
            "corr factors resolved: requested=%s available_in_frame=%s missing=%s use=%s",
            requested,
            available,
            missing,
            use_columns,
        )
        if len(use_columns) < 2:
            raise InsufficientFactorsError(
                "corr needs at least 2 factors after filtering; "
                f"requested={requested!r} available_in_frame={available!r} "
                f"missing={missing!r} use={use_columns!r}"
            )
        selected_columns = use_columns

    # 2) 計算相關矩陣
    corr_matrix = compute_corr_matrix(
        frame,
        columns=selected_columns,
        method=job.method,
        min_periods=job.min_periods,
    )

    logger.info(
        "computed corr matrix: shape=%s (n_factors=%d)",
        corr_matrix.shape,
        corr_matrix.shape[1],
    )

    # 3) 矩陣摘要 + max |corr|
    summary = summarize_corr(corr_matrix)
    logger.info("max_abs_per_factor: %s", summary.max_abs_per_factor)

    # 4) 攤平為 pair list
    pairs_df = corr_matrix_to_pairs(
        summary.matrix,
        absolute=True,
        upper_triangle_only=True,
        skip_self=True,
        drop_na=True,
        sort_desc=True,
    )

    logger.info("pairs DataFrame: shape=%s", pairs_df.shape)

    # 5) 寫出檔案
    # 5.1 相關矩陣：保留 index（因為 row/col label 代表 factor_id）
    _write_frame(summary.matrix, job.matrix_path, index=True)
    logger.info("written corr matrix to %s", job.matrix_path)

    # 5.2 pair list：index 沒資訊意義，可以丟掉
    _write_frame(pairs_df, job.pairs_path, index=False)
    logger.info("written corr pairs to %s", job.pairs_path)

    # 5.3 摘要 JSON
    pairs_payload = [
        {
            "a": str(row["factor_1"]),
            "b": str(row["factor_2"]),
            "corr": float(row["corr"]),
        }
        for _, row in pairs_df.iterrows()
    ]

    summary_obj = {
        "as_of": job.as_of,
        "window_label": job.window_label,
        "method": job.method,
        "min_periods": job.min_periods,
        "n_factors": int(summary.matrix.shape[1]),
        "max_abs_corr_per_factor": summary.max_abs_per_factor,
        "pairs": pairs_payload,
        "input_path": str(job.input_path),
        "matrix_path": str(job.matrix_path),
        "pairs_path": str(job.pairs_path),
    }

    _ensure_output_dir(job.summary_path)
    with job.summary_path.open("w", encoding="utf-8") as f:
        json.dump(summary_obj, f, ensure_ascii=False, indent=2)

    logger.info("written corr summary JSON to %s", job.summary_path)
    logger.info("factor_corr job finished.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute factor correlation matrix / pairs / summary using alpha_core.phase2.corelib.corr_lib.",
    )

    parser.add_argument(
        "--root",
        "-R",
        type=str,
        default=".",
        help="Project root (default: current directory).",
    )
    parser.add_argument(
        "--rules",
        type=str,
        default="./rules_factors.yaml",
        help="Path to rules_factors.yaml (目前僅用於對齊介面，可供未來擴充使用)。",
    )
    parser.add_argument(
        "--as-of",
        type=str,
        required=True,
        help="As-of date label (YYYY-MM-DD), used in output filenames and window selection.",
    )
    parser.add_argument(
        "--windows",
        type=str,
        required=True,
        help="Comma-separated list of window months, e.g. '6,12,24'.",
    )
    parser.add_argument(
        "--factors",
        type=str,
        default=None,
        help="Comma-separated factor_ids to compute corr for (default: use active factors from plan).",
    )
    parser.add_argument(
        "--engine",
        type=str,
        choices=["classic", "ai"],
        default="classic",
        help="Factor engine kind (used to select factor_plan.<as_of>.<engine>.json).",
    )
    parser.add_argument(
        "--profile",
        type=str,
        choices=["dev", "test", "live"],
        default="test",
        help="Profile name (目前僅用於 log，未直接影響計算邏輯)。",
    )
    parser.add_argument(
        "--panel-source",
        type=str,
        choices=["factor_parquet", "panel_parquet"],
        default="factor_parquet",
        help="Source of panel data: 'factor_parquet' 會從 factor parquet 動態建寬表；"
        "'panel_parquet' 則重用既有 panel_<as_of>_<window>.parquet。",
    )
    parser.add_argument(
        "--output-format",
        type=str,
        choices=["parquet", "csv"],
        default="parquet",
        help="Output format for matrix / pairs files (default: parquet).",
    )
    parser.add_argument(
        "--method",
        type=str,
        default="pearson",
        help="Correlation method: pearson / spearman / kendall (default: pearson).",
    )
    parser.add_argument(
        "--min-periods",
        type=int,
        default=30,
        help="Minimum overlapping observations required for each pair (default: 30).",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level: DEBUG / INFO / WARNING / ERROR (default: INFO).",
    )

    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    root = Path(args.root).resolve()
    # rules_path 目前未直接使用，保留未來擴充性
    _rules_path = Path(args.rules).resolve()
    windows = parse_windows_arg(args.windows)
    as_of = args.as_of
    panel_source = args.panel_source
    output_format = args.output_format
    method = args.method
    min_periods = int(args.min_periods)
    requested_factors: Optional[List[str]] = None
    if args.factors:
        try:
            requested_factors = parse_factors_arg(args.factors)
        except ValueError as exc:
            logger.error("invalid --factors value: %s", exc)
            return 2

    logger.info(
        "factor_corr main start: root=%s as_of=%s windows=%s engine=%s profile=%s panel_source=%s",
        root,
        as_of,
        windows,
        args.engine,
        args.profile,
        panel_source,
    )

    if requested_factors is not None:
        factor_ids = requested_factors
        logger.info(
            "requested factors from --factors (n=%d): %s",
            len(factor_ids),
            ", ".join(factor_ids),
        )
    else:
        # 從 factor_plan 選出 active factors
        try:
            plan = load_factor_plan(root, as_of=as_of, engine=args.engine)
        except FileNotFoundError as exc:
            logger.error("failed to load factor_plan: %s", exc)
            return 1

        factor_ids = select_active_factors(plan)
        if not factor_ids:
            logger.warning(
                "no active factors found in factor_plan for as_of=%s engine=%s; nothing to do",
                as_of,
                args.engine,
            )
            return 0
        logger.info("active factors from plan: %s", ", ".join(factor_ids))

    as_of_dt = pd.to_datetime(as_of).date()  # 目前僅用於 log

    any_success = False
    any_insufficient = False

    for w in windows:
        window_label = window_label_for_months(w)
        logger.info("processing corr window=%dm (label=%s)", w, window_label)

        # 決定 panel 檔案路徑
        if panel_source == "factor_parquet":
            try:
                panel_path = build_panel_from_factor_parquet(
                    root=root,
                    factor_ids=factor_ids,
                    as_of_str=as_of,
                    window_months=w,
                )
            except Exception as exc:  # noqa: BLE001
                if requested_factors is not None and _is_insufficient_panel_error(exc):
                    logger.warning(
                        "corr factors resolved (panel build failed): "
                        "requested=%s available_in_frame=%s missing=%s use=%s",
                        requested_factors,
                        [],
                        requested_factors,
                        [],
                    )
                    any_insufficient = True
                    continue
                logger.error(
                    "failed to build panel from factor parquet for window=%dm: %s",
                    w,
                    exc,
                )
                continue
        else:
            try:
                panel_path = resolve_panel_path_from_args(
                    root=root,
                    as_of=as_of,
                    window_months=w,
                    panel_source=panel_source,
                )
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "failed to resolve panel path for window=%dm (panel_source=%s): %s",
                    w,
                    panel_source,
                    exc,
                )
                continue

        job = FactorCorrJob(
            root=root,
            input_path=panel_path,
            columns=factor_ids if requested_factors is not None else None,
            method=method,
            min_periods=min_periods,
            as_of=as_of,
            window_label=window_label,
            output_format=output_format,
        )

        try:
            run_corr_job(job)
            any_success = True
        except InsufficientFactorsError as exc:
            logger.error(
                "corr job aborted for window=%dm (label=%s): %s",
                w,
                window_label,
                exc,
            )
            any_insufficient = True
            continue
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "corr job failed for window=%dm (label=%s): %s",
                w,
                window_label,
                exc,
            )

    if not any_success:
        if requested_factors is not None and any_insufficient:
            logger.error(
                "no corr job succeeded; insufficient factors for requested set."
            )
            return 2
        logger.error("no corr job succeeded for any window; exiting with failure.")
        return 1

    logger.info("factor_corr completed for as_of=%s windows=%s", as_of_dt, windows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

