# C:\AI\tw-alpha-stack\alpha_core\corr_lib.py
#!/usr/bin/env python
"""
corr_lib.py

因子相關性（correlation）共用工具函式庫，設計給 Phase-2「corr」與後續
combo / capacity 模組使用。

設計重點：
- 專注在「已經準備好的寬表（wide）資料」上做相關性計算。
  - DataFrame index：任意（日期、日期×股票、日期×組合…皆可）
  - DataFrame columns：每一欄是一個 factor_id 或任意指標名稱
- 提供：
  1) 計算相關矩陣（corr matrix）
  2) 將相關矩陣攤平成 pair list（long format）
  3) 計算每個 factor 的最大 |corr|（可用來當 max_corr 指標）

注意：
- 本模組 **不負責讀 parquet / 掛 paths**，只吃 pandas.DataFrame。
  真正的 I/O 由 scripts/p2/factor_corr.py 或其它上層模組處理。
- 不使用任何 alias / Protocol 型別別名，以保持簡單直覺。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import logging
import math

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CorrSummary:
    """
    相關矩陣與摘要資訊。

    欄位說明：
    - matrix              : 相關係數矩陣（index / columns 同一組 labels）
    - max_abs_per_factor  : 每個欄位的「最大絕對相關值」
    """

    matrix: pd.DataFrame
    max_abs_per_factor: Dict[str, float]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ensure_numeric_frame(
    frame: pd.DataFrame,
    keep_columns: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    只保留數值欄位，並可選擇性地限制在 keep_columns 之內。

    若最後沒有任何欄位可用，會丟 ValueError。
    """
    if keep_columns is not None:
        keep_set = {str(c) for c in keep_columns}
        frame = frame.loc[:, [c for c in frame.columns if str(c) in keep_set]]

    numeric = frame.select_dtypes(include=["number"])
    if numeric.empty:
        raise ValueError("no numeric columns available for correlation")

    # 確保 columns 是 string，避免後續 mix type
    numeric = numeric.copy()
    numeric.columns = [str(c) for c in numeric.columns]
    return numeric


def _validate_method(method: str) -> str:
    """
    驗證相關性計算 method 是否支援，並回傳正常化後字串。
    """
    m = method.lower()
    if m not in ("pearson", "spearman", "kendall"):
        raise ValueError(f"unsupported correlation method: {method!r}")
    return m


# ---------------------------------------------------------------------------
# Public API：matrix 計算與摘要
# ---------------------------------------------------------------------------


def compute_corr_matrix(
    frame: pd.DataFrame,
    *,
    columns: Optional[Iterable[str]] = None,
    method: str = "pearson",
    min_periods: int = 30,
) -> pd.DataFrame:
    """
    計算寬表資料的相關矩陣。

    參數：
    - frame      : index 任意、columns 為各個 factor 或指標
    - columns    : 若指定，只從這些欄位中挑數值欄位計算；None 則使用所有數值欄位
    - method     : 'pearson' / 'spearman' / 'kendall'
    - min_periods: 每對欄位最少需要多少共同觀測點才計算相關性

    回傳：
    - corr_df: pandas.DataFrame，index/columns 為同一組欄位名稱
    """
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("frame must be a pandas.DataFrame")

    method_norm = _validate_method(method)
    numeric = _ensure_numeric_frame(frame, keep_columns=columns)

    if numeric.shape[1] < 2:
        raise ValueError(
            f"at least 2 numeric columns are required to compute correlation "
            f"(got {numeric.shape[1]})"
        )

    logger.debug(
        "compute_corr_matrix: n_rows=%d n_cols=%d method=%s min_periods=%d",
        numeric.shape[0],
        numeric.shape[1],
        method_norm,
        min_periods,
    )

    corr_df = numeric.corr(method=method_norm, min_periods=min_periods)
    return corr_df


def max_abs_corr_per_factor(
    corr_matrix: pd.DataFrame,
    *,
    skip_self: bool = True,
) -> Dict[str, float]:
    """
    從相關矩陣計算每個欄位的最大「絕對相關值」。

    參數：
    - corr_matrix: 方陣，index / columns 同一組 labels
    - skip_self  : True 時會先去掉對角線（自己與自己 = 1）

    回傳：
    - dict：{factor_id: max_abs_corr}
      若該欄位在移除自己後沒有任何值，則給 NaN。
    """
    if not isinstance(corr_matrix, pd.DataFrame):
        raise TypeError("corr_matrix must be a pandas.DataFrame")

    result: Dict[str, float] = {}

    for col in corr_matrix.columns:
        series = corr_matrix[col]

        if skip_self and col in series.index:
            series = series.drop(labels=[col])

        if series.empty:
            result[str(col)] = math.nan
            continue

        max_abs = float(series.abs().max())
        result[str(col)] = max_abs

    return result


def summarize_corr(
    corr_matrix: pd.DataFrame,
    *,
    skip_self: bool = True,
) -> CorrSummary:
    """
    封裝 compute_corr_matrix 的摘要結果。

    - matrix              : 原始相關矩陣（會 copy 一份）
    - max_abs_per_factor  : 每個 factor 的最大 |corr|（可作為 max_corr 指標）
    """
    if not isinstance(corr_matrix, pd.DataFrame):
        raise TypeError("corr_matrix must be a pandas.DataFrame")

    matrix_copy = corr_matrix.copy()
    max_map = max_abs_corr_per_factor(matrix_copy, skip_self=skip_self)
    return CorrSummary(matrix=matrix_copy, max_abs_per_factor=max_map)


# ---------------------------------------------------------------------------
# Public API：將 matrix 攤平為 pair list
# ---------------------------------------------------------------------------


def corr_matrix_to_pairs(
    corr_matrix: pd.DataFrame,
    *,
    absolute: bool = False,
    upper_triangle_only: bool = True,
    skip_self: bool = True,
    drop_na: bool = True,
    sort_desc: bool = True,
) -> pd.DataFrame:
    """
    將相關矩陣攤平成「(factor_1, factor_2, corr)」形式的 long DataFrame。

    參數：
    - corr_matrix       : 相關矩陣（index / columns 同一組 labels）
    - absolute          : True 則輸出 'abs_corr' 欄位，值為 abs(corr)
    - upper_triangle_only:
        True  → 只取上三角（i < j）
        False → 全部 pair（可配合 skip_self 控制是否保留 i==j）
    - skip_self         : True 時會略過 factor_1 == factor_2
    - drop_na           : True 時略過 corr 為 NaN 的 pair
    - sort_desc         : True 時依 abs(corr) 由大到小排序

    回傳：
    - df：欄位至少包含 ['factor_1', 'factor_2', 'corr']，
           若 absolute=True 則多一欄 'abs_corr'。
    """
    if not isinstance(corr_matrix, pd.DataFrame):
        raise TypeError("corr_matrix must be a pandas.DataFrame")

    labels = [str(c) for c in corr_matrix.columns]
    pairs: List[Tuple[str, str, float]] = []

    for i, f1 in enumerate(labels):
        for j, f2 in enumerate(labels):
            if skip_self and i == j:
                continue
            if upper_triangle_only and j <= i:
                # 只保留上三角（不含對角線）
                continue

            try:
                value = float(corr_matrix.loc[f1, f2])
            except Exception:
                value = math.nan

            if drop_na and (math.isnan(value) or pd.isna(value)):
                continue

            pairs.append((f1, f2, value))

    if not pairs:
        # 回傳空 DataFrame，但 schema 固定，方便上游處理
        columns = ["factor_1", "factor_2", "corr"]
        if absolute:
            columns.append("abs_corr")
        return pd.DataFrame(columns=columns)

    df = pd.DataFrame(pairs, columns=["factor_1", "factor_2", "corr"])

    if absolute:
        df["abs_corr"] = df["corr"].abs()

    if sort_desc:
        key_col = "abs_corr" if absolute else "corr"
        df = df.sort_values(by=key_col, ascending=False).reset_index(drop=True)

    return df


# ---------------------------------------------------------------------------
# Example usage (for local開發測試用；正式流程不會直接當 CLI 用)
# ---------------------------------------------------------------------------


def _example_usage() -> None:
    """
    簡單示範：建立假資料 → 算相關矩陣 → 攤平成 pair list。
    不會在正常匯入時執行，只在直接 python corr_lib.py 時跑。
    """
    import numpy as np

    rng = np.random.default_rng(42)
    n = 100

    df = pd.DataFrame(
        {
            "mom_6m": rng.normal(size=n),
            "mom_12m": rng.normal(size=n),
            "value_pe": rng.normal(size=n),
        }
    )

    corr = compute_corr_matrix(df, method="pearson", min_periods=20)
    summary = summarize_corr(corr)
    pairs = corr_matrix_to_pairs(corr, absolute=True, upper_triangle_only=True)

    print("=== corr matrix ===")
    print(summary.matrix)
    print("\n=== max_abs_per_factor ===")
    print(summary.max_abs_per_factor)
    print("\n=== top pairs ===")
    print(pairs.head())


if __name__ == "__main__":
    # 只做簡單 smoke test，不影響被當作函式庫匯入時的行為。
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    _example_usage()



