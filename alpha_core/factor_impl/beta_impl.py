# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.beta_impl

Beta family (beta_252d).
"""
from __future__ import annotations
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from alpha_core.factor_xform import apply_xsection_xform


def _get_price_column(df: pd.DataFrame) -> str:
    for col in ["adj_close", "close", "Close"]:
        if col in df.columns:
            return col
    return "adj_close"  # Fallback, might fail later but consistent


def _finite_df(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure DataFrame is finite (inf -> NaN), preserving shape/index/columns."""
    arr = df.to_numpy()
    mask = ~np.isfinite(arr)
    if mask.any():
        arr = arr.astype(float, copy=True)
        arr[mask] = np.nan
        return pd.DataFrame(arr, index=df.index, columns=df.columns)
    return df


def _finite_s(s: pd.Series) -> pd.Series:
    """Ensure Series is finite (inf -> NaN), preserving index."""
    arr = s.to_numpy()
    mask = ~np.isfinite(arr)
    if mask.any():
        arr = arr.astype(float, copy=True)
        arr[mask] = np.nan
        return pd.Series(arr, index=s.index, name=s.name)
    return s


def _ratio_inf(df: pd.DataFrame) -> tuple[int, float]:
    """Return total inf count and ratio over all cells (ratio over total size)."""
    inf_count = int(np.isinf(df.to_numpy()).sum())
    total = df.size if df.size else 1
    return inf_count, float(inf_count / total)


def _winsorize_wide(wide: pd.DataFrame, p: float) -> pd.DataFrame:
    """Clip each date's cross-section to [p, 1-p] quantiles."""
    def _clip_row(row: pd.Series) -> pd.Series:
        if row.count() == 0:
            return row
        lo = row.quantile(p)
        hi = row.quantile(1 - p)
        return row.clip(lower=lo, upper=hi)

    return wide.apply(_clip_row, axis=1)


def _zscore_by_row(wide: pd.DataFrame) -> pd.DataFrame:
    """Row-wise z-score; if std==0 keep zeros."""
    def _z(row: pd.Series) -> pd.Series:
        vals = row.to_numpy(dtype=float)
        mask = np.isfinite(vals)
        if not mask.any():
            return row * np.nan
        mu = np.nanmean(vals)
        sigma = np.nanstd(vals)
        if sigma == 0 or not np.isfinite(sigma):
            z = vals - mu
        else:
            z = (vals - mu) / sigma
        return pd.Series(z, index=row.index)

    return wide.apply(_z, axis=1)

def run_beta_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 beta 引擎入口。
    以 equal-weighted universe 報酬作為市場，計算 rolling beta，並轉成
    low-beta style（beta 越低因子值越高）。
    """
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # Params with defaults
    direction = str(kwargs.get("direction", "low")).lower()
    beta_window = int(kwargs.get("window_days", kwargs.get("beta_window_days", 252)))
    if beta_window <= 0:
        beta_window = 252
    min_obs = int(kwargs.get("min_obs", kwargs.get("min_periods", 0)))
    if min_obs <= 0:
        min_obs = max(60, int(round(beta_window * 0.8)))
    min_obs = min(min_obs, beta_window)
    winsor_p = float(kwargs.get("winsor_p", 0.01))
    market_mode = str(kwargs.get("market_mode", "ew")).lower()

    price_col = _get_price_column(prices)
    if price_col not in prices.columns:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices[["date", "stock_id", price_col]].copy()
    df.rename(columns={price_col: "adj_close"}, inplace=True)

    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    df = df.sort_values(["stock_id", "date"])

    # 個股報酬，使用 shift(1) 避免 look-ahead；價格 <=0 視為無效
    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    df.loc[df["adj_close"] <= 0, "adj_close"] = np.nan

    price_wide = df.pivot(index="date", columns="stock_id", values="adj_close").sort_index()
    log_px = _finite_df(np.log(price_wide))
    ret_panel = log_px.diff()
    ret_inf_count_total, ret_inf_ratio_total = _ratio_inf(ret_panel)
    ret_panel = _finite_df(ret_panel)
    ret_panel = ret_panel.dropna(how="all")

    if ret_panel.empty:
        raise ValueError(
            f"empty_factor_output: beta factor_id=beta_252d window={beta_window} "
            f"min_obs={min_obs} end_date={end_date}; "
            f"ret_wide_shape={ret_panel.shape} ret_inf_ratio_total={ret_inf_ratio_total:.6f} long_rows_final=0"
        )

    # equal-weighted 市場報酬
    rm = ret_panel.mean(axis=1, skipna=True).astype("float64")
    rm = _finite_s(rm)
    rm_inf_count = int(np.isinf(rm.to_numpy()).sum())
    rm_nan_ratio = float(rm.isna().mean()) if len(rm) else 0.0
    rm_std = float(rm.std(ddof=0)) if rm.notna().sum() else float("nan")
    rm_min = float(rm.min()) if rm.notna().sum() else float("nan")
    rm_max = float(rm.max()) if rm.notna().sum() else float("nan")
    if rm_inf_count > 0:
        raise ValueError(
            f"empty_factor_output: beta factor_id=beta_252d window={beta_window} min_obs={min_obs} end_date={end_date}; "
            f"rm_inf_count={rm_inf_count} rm_nan_ratio={rm_nan_ratio:.4f} rm_std={rm_std:.6f} rm_min={rm_min:.6f} rm_max={rm_max:.6f} "
            f"ret_inf_ratio_total={ret_inf_ratio_total:.6f} ret_wide_shape={ret_panel.shape} "
            f"date_min={ret_panel.index.min()} date_max={ret_panel.index.max()}"
        )
    if not rm.index.equals(ret_panel.index):
        raise ValueError(
            f"beta_index_mismatch: rm_index_equals_ret_index=False factor_id=beta_252d "
            f"ret_wide_shape={ret_panel.shape}"
        )

    w = beta_window
    min_periods = min_obs  # 不可低於 min_obs

    # 市場變異數（保持 index 對齊，先 finite）
    var_m = rm.rolling(w, min_periods=min_periods).var(ddof=0)
    var_m = _finite_s(var_m)
    # 避免除以 0/負值，再次轉 NaN
    var_m = var_m.where(var_m > 0)
    var_m_non_null_ratio = float(var_m.notna().mean()) if len(var_m) else 0.0
    if var_m.notna().sum() == 0:
        msg = (
            f"empty_factor_output: beta factor_id=beta_252d window={beta_window} "
            f"min_obs={min_obs} end_date={end_date}; "
            f"rm_non_null_ratio={float(rm.notna().mean()):.4f} "
            f"rm_inf_count={rm_inf_count} rm_nan_ratio={rm_nan_ratio:.4f} "
            f"rm_std={rm_std:.6f} rm_min={rm_min:.6f} rm_max={rm_max:.6f} "
            f"ret_inf_ratio_total={ret_inf_ratio_total:.6f} "
            f"rm_index_equals_ret_index={rm.index.equals(ret_panel.index)} "
            f"var_m_non_null_ratio={var_m_non_null_ratio:.4f} "
            f"ret_wide_shape={ret_panel.shape} date_min={ret_panel.index.min()} date_max={ret_panel.index.max()}"
        )
        raise ValueError(msg)

    # 避免對齊陷阱：顯式計算期望值
    rirm = ret_panel.mul(rm, axis=0)
    e_ri = ret_panel.rolling(w, min_periods=min_periods).mean()
    e_rm = rm.rolling(w, min_periods=min_periods).mean()
    e_rirm = rirm.rolling(w, min_periods=min_periods).mean()

    cov = e_rirm.sub(e_ri.mul(e_rm, axis=0), axis=0)
    cov = _finite_df(cov)

    beta_panel = cov.div(var_m, axis=0)
    beta_panel = _finite_df(beta_panel)

    # min_obs gating
    valid_counts = ret_panel.notna().rolling(w, min_periods=min_periods).sum()
    beta_panel = beta_panel.where(valid_counts >= min_obs)

    beta_panel = beta_panel.dropna(how="all")
    if beta_panel.empty:
        msg = (
            f"empty_factor_output: beta factor_id=beta_252d window={beta_window} "
            f"min_obs={min_obs} end_date={end_date}; "
            f"ret_wide_shape={ret_panel.shape} date_min={ret_panel.index.min()} date_max={ret_panel.index.max()} "
            f"market_ret_non_null_ratio={float(rm.notna().mean()):.4f} "
            f"var_m_non_null_ratio={float(var_m.notna().mean()):.4f} "
            f"cov_non_null_ratio={float(cov.notna().mean().mean()):.4f} "
            f"beta_non_null_ratio={float(beta_panel.notna().mean().mean()):.4f} long_rows_final=0 "
            f"ret_inf_ratio_total={ret_inf_ratio_total:.6f} rm_inf_count={rm_inf_count}"
        )
        raise ValueError(msg)

    if direction == "low":
        beta_panel = -beta_panel

    if winsor_p and winsor_p > 0:
        beta_panel = _winsorize_wide(beta_panel, p=winsor_p)

    beta_panel = _zscore_by_row(beta_panel)

    long = beta_panel.stack(dropna=True).reset_index()
    long.columns = ["date", "stock_id", "factor_value"]

    if long.empty:
        msg = (
            f"empty_factor_output: beta factor_id=beta_252d window={beta_window} "
            f"min_obs={min_obs} end_date={end_date}; "
            f"ret_wide_shape={ret_panel.shape} date_min={ret_panel.index.min()} date_max={ret_panel.index.max()} "
            f"market_ret_non_null_ratio={float(rm.notna().mean()):.4f} "
            f"var_m_non_null_ratio={float(var_m.notna().mean()):.4f} "
            f"beta_non_null_ratio={float(beta_panel.notna().stack().mean() if not beta_panel.empty else 0):.4f} "
            f"long_rows_final=0 "
            f"ret_inf_ratio_total={ret_inf_ratio_total:.6f} rm_inf_count={rm_inf_count}"
        )
        raise ValueError(msg)
    return long
