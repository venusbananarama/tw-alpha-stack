# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.liq_impl

Liquidity family (liq_turnover_20d, etc.)
Optimized by Gemini (Vectorized Implementation)
"""
from __future__ import annotations
from datetime import date
from typing import Any, Optional

import numpy as np
import pandas as pd

from alpha_core.factor_xform import apply_xsection_xform, winsorize_by_quantile


def _normalize_factor_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure factor frames use (date, stock_id, factor_value) with clean types."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    cols = df.columns
    stock_col = None
    for cand in ("stock_id", "stock", "code", "symbol"):
        if cand in cols:
            stock_col = cand
            break
    if stock_col is None:
        raise ValueError("factor frame missing stock_id column")

    value_col = None
    for cand in ("factor_value", "value", "factor"):
        if cand in cols:
            value_col = cand
            break
    if value_col is None:
        raise ValueError("factor frame missing factor_value column")

    out = df.copy()
    out.rename(columns={stock_col: "stock_id", value_col: "factor_value"}, inplace=True)
    out["date"] = pd.to_datetime(out["date"])
    out = out.dropna(subset=["date", "stock_id", "factor_value"])
    out["stock_id"] = out["stock_id"].astype(str)
    return out[["date", "stock_id", "factor_value"]].sort_values(["date", "stock_id"]).reset_index(drop=True)

def compute_turnover(
    prices: pd.DataFrame,
    window_days: int,
    *,
    transform: str = "log1p",
) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    
    # 檢查必要欄位
    cols = df.columns
    # 優先使用 turnover (成交金額)
    if "turnover" in cols:
        target_col = "turnover"
    elif "Trading_turnover" in cols:
        target_col = "Trading_turnover"
    elif "close" in cols and "volume" in cols:
        # 近似計算：收盤價 * 成交量
        df["turnover_proxy"] = df["close"] * df["volume"]
        target_col = "turnover_proxy"
    elif "adj_close" in cols and "volume" in cols:
         # Fallback proxy
        df["turnover_proxy"] = df["adj_close"] * df["volume"]
        target_col = "turnover_proxy"
    else:
        # 缺資料，回傳空
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = df.sort_values(["stock_id", "date"])

    # 計算滾動平均成交值 (Rolling Mean Turnover)
    df["liq"] = df.groupby("stock_id")[target_col].transform(
        lambda x: x.rolling(window=window_days, min_periods=max(1, window_days // 2)).mean()
    )
    
    # transform
    if transform == "log1p":
        df["factor_value"] = np.log1p(df["liq"])
    else:
        df["factor_value"] = df["liq"]

    df = df.dropna(subset=["factor_value"])
    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)


def run_liquidity_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    # 預留 shareholding/inst_total，目前未用
    shareholding: Optional[pd.DataFrame] = None,
    inst_total: Optional[pd.DataFrame] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 liq 引擎入口。
    Fix: Added window, end_date arguments to match __init__.py dispatch.
    """
    params = kwargs
    lookback = int(params.get("turnover_lookback_days", params.get("lookback_days", 20)))
    if lookback <= 0:
        lookback = 20

    transform = str(params.get("transform", "log1p")).lower()
    winsor_pctl = float(params.get("winsor_pctl", 0.0) or 0.0)
    do_zscore = bool(params.get("zscore", False))
    min_obs_per_day = int(params.get("min_obs_per_day", 50))
    direction = str(params.get("direction", "illiquid")).lower()

    df_liq = compute_turnover(prices, window_days=lookback, transform=transform)
    if df_liq.empty:
        return df_liq

    df_liq = _normalize_factor_frame(df_liq)
    wide = df_liq.pivot(index="date", columns="stock_id", values="factor_value")
    wide = wide.sort_index()

    if winsor_pctl and winsor_pctl > 0:
        wide = wide.apply(winsorize_by_quantile, axis=1, q=winsor_pctl)

    # size neutralization if provided / required
    neutralize_with = params.get("neutralize_with")
    aux_panels = params.get("_aux_factor_panels") if isinstance(params.get("_aux_factor_panels"), dict) else {}
    size_df = None

    require_size = False
    if neutralize_with:
        if isinstance(neutralize_with, str):
            require_size = neutralize_with == "size_log_mktcap"
        elif isinstance(neutralize_with, (list, tuple, set)):
            require_size = "size_log_mktcap" in set(map(str, neutralize_with))

    if require_size:
        if isinstance(aux_panels, dict):
            size_df = aux_panels.get("size_log_mktcap")
        if not isinstance(size_df, pd.DataFrame):
            raise ValueError(
                "liq_turnover_20d: required dependency size_log_mktcap not injected via _aux_factor_panels"
            )

    if isinstance(size_df, pd.DataFrame) and {"date", "stock_id", "factor_value"}.issubset(size_df.columns):
        size_df_norm = _normalize_factor_frame(size_df)
        size_wide = size_df_norm.pivot(index="date", columns="stock_id", values="factor_value")
        size_wide = size_wide.sort_index()

        common_dates = wide.index.intersection(size_wide.index)
        common_cols = wide.columns.intersection(size_wide.columns)
        wide = wide.loc[common_dates, common_cols]
        size_wide = size_wide.loc[common_dates, common_cols]

        resid_rows = []
        for dt, row in wide.iterrows():
            x = size_wide.loc[dt]
            y = row
            mask = x.notna() & y.notna()
            if mask.sum() < min_obs_per_day or x[mask].nunique() <= 1:
                resid_rows.append(pd.Series(np.nan, index=wide.columns, dtype=float))
                continue
            x1 = x[mask].astype(float)
            y1 = y[mask].astype(float)
            X = np.vstack([np.ones(len(x1)), x1.to_numpy()]).T
            try:
                beta, *_ = np.linalg.lstsq(X, y1.to_numpy(), rcond=None)
            except np.linalg.LinAlgError:
                resid_rows.append(pd.Series(np.nan, index=wide.columns, dtype=float))
                continue
            y_hat = X @ beta
            resid = pd.Series(np.nan, index=wide.columns, dtype=float)
            resid.loc[mask] = y1.to_numpy() - y_hat
            resid_rows.append(resid)
        wide = pd.DataFrame(resid_rows, index=wide.index)

    if do_zscore:
        wide = apply_xsection_xform(wide, strategy="zscore")

    long = wide.stack(dropna=True).reset_index()
    long.columns = ["date", "stock_id", "factor_value"]

    # direction handling: illiquid => larger values mean less liquid (invert residual)
    if direction == "illiquid":
        long["factor_value"] = -long["factor_value"]
    elif direction == "liquid":
        long["factor_value"] = long["factor_value"]

    return long
