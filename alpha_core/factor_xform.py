# -*- coding: utf-8 -*-
"""
alpha_core.factor_xform

Cross-sectional transforms (winsorize / z-score / rank) on Series or
row-wise on DataFrames. Designed for factor values where each row is a
date and columns are stock_ids (or equivalent cross-sectional units).
"""

from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd


def winsorize_xsection(
    series: pd.Series,
    lower_q: float = 0.01,
    upper_q: float = 0.99,
) -> pd.Series:
    """
    Clip a cross-sectional Series to the given quantile bounds.
    NaN/inf are left as NaN for downstream handling.
    """
    if series.empty:
        return series

    s = series.astype(float).replace([np.inf, -np.inf], np.nan)
    lo = s.quantile(lower_q)
    hi = s.quantile(upper_q)
    return s.clip(lower=lo, upper=hi)


def zscore_xsection(
    series: pd.Series,
    ddof: int = 0,
    clip_std: float | None = None,
) -> pd.Series:
    """
    Cross-sectional z-score; keeps NaN where data missing.
    """
    if series.empty:
        return series

    s = series.astype(float).replace([np.inf, -np.inf], np.nan)
    mean = s.mean()
    std = s.std(ddof=ddof)
    if std == 0 or np.isnan(std):
        z = s - mean
    else:
        z = (s - mean) / std

    if clip_std is not None and clip_std > 0:
        z = z.clip(lower=-clip_std, upper=clip_std)
    return z


def rank_xsection(
    series: pd.Series,
    pct: bool = True,
    method: str = "average",
    center: bool = True,
) -> pd.Series:
    """
    Cross-sectional rank. If pct=True, returns percentiles in [0,1];
    center=True shifts to roughly [-0.5, 0.5].
    """
    if series.empty:
        return series

    s = series.astype(float).replace([np.inf, -np.inf], np.nan)
    ranked = s.rank(method=method, pct=pct)
    if pct and center:
        ranked = ranked - 0.5
    return ranked


def apply_xsection_xform(
    panel: pd.DataFrame,
    strategy: str = "zscore",
    winsor_limits: Tuple[float, float] = (0.01, 0.99),
    clip_std: float | None = 5.0,
    min_valid_per_row: int = 3,
) -> pd.DataFrame:
    """
    Apply cross-sectional transform row-wise on a 2D DataFrame.

    Each row is treated as one cross-section (e.g., one date).
    The shape/index/columns are preserved; values are transformed.
    """
    if panel.empty:
        return panel

    def _transform_row(row: pd.Series) -> pd.Series:
        s = winsorize_xsection(row, lower_q=winsor_limits[0], upper_q=winsor_limits[1])

        valid = s.replace([np.inf, -np.inf], np.nan).dropna()
        if valid.shape[0] < min_valid_per_row:
            return pd.Series(np.nan, index=row.index)

        if strategy == "rank":
            return rank_xsection(s, pct=True, center=True)
        # default zscore
        return zscore_xsection(s, ddof=0, clip_std=clip_std)

    transformed = panel.apply(_transform_row, axis=1)
    return transformed

