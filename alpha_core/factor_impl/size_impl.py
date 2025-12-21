# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.size_impl

Size family (size_log_mktcap).
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import date
from typing import Any, Optional

from alpha_core.factor_xform import apply_xsection_xform, winsorize_by_quantile

def run_size_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    shareholding: Optional[pd.DataFrame] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 size 引擎入口。
    計算 Log Market Cap。
    """
    lag_trading_days = int(kwargs.get("lag_trading_days", 0) or 0)
    smooth_days = int(kwargs.get("smooth_days", 0) or 0)
    winsor_pctl = float(kwargs.get("winsor_pctl", 0.0) or 0.0)
    do_zscore = bool(kwargs.get("zscore", False))
    transform = str(kwargs.get("transform", "") or "").lower()

    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    
    # 嘗試尋找股本欄位，若無則暫用股價 (Price Only)
    # (正式環境應 merge shareholding 來算真實市值)
    target = None
    if "market_cap" in df.columns:
        target = df["market_cap"]
    elif "close" in df.columns:
        target = df["close"] # Fallback: Log Price
    elif "adj_close" in df.columns:
        target = df["adj_close"]
    
    if target is None:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df["factor_value"] = np.log(target)
    
    # 清洗 Inf/-Inf
    df = df.replace([np.inf, -np.inf], np.nan).dropna(subset=["factor_value"])

    df = df.sort_values(["stock_id", "date"])

    # trading-day lag
    if lag_trading_days > 0:
        df["factor_value"] = df.groupby("stock_id")["factor_value"].shift(lag_trading_days)

    # optional smoothing across time
    if smooth_days and smooth_days > 1:
        df["factor_value"] = (
            df.groupby("stock_id")["factor_value"]
            .rolling(window=smooth_days, min_periods=max(1, smooth_days // 2))
            .mean()
            .reset_index(level=0, drop=True)
        )

    df = df.dropna(subset=["factor_value"])

    base_cols = df[["date", "stock_id", "factor_value"]].copy()

    # Early return when no cross-sectional transforms are requested
    if winsor_pctl <= 0 and not do_zscore and transform not in ("small", "mid"):
        return base_cols.reset_index(drop=True)

    wide = (
        base_cols.sort_values(["date", "stock_id"])
        .pivot(index="date", columns="stock_id", values="factor_value")
        .sort_index()
    )

    if winsor_pctl and winsor_pctl > 0:
        wide = wide.apply(winsorize_by_quantile, axis=1, q=winsor_pctl)

    if do_zscore:
        wide = apply_xsection_xform(
            wide,
            strategy="zscore",
            winsor_limits=(0.0, 1.0),
            clip_std=None,
        )

    long = wide.stack(dropna=True).reset_index()
    long.columns = ["date", "stock_id", "factor_value"]

    if transform == "small":
        long["factor_value"] = -long["factor_value"]
    elif transform == "mid":
        long["factor_value"] = -long["factor_value"].abs()

    return long.sort_values(["date", "stock_id"]).reset_index(drop=True)
