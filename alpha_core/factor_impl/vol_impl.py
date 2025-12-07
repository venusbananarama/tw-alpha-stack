# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.vol_impl

Volatility family (vol_20d, etc.)
Optimized by Gemini (Vectorized Implementation)
"""
from __future__ import annotations
from datetime import date
from typing import Any

import numpy as np
import pandas as pd

from alpha_core.factor_xform import apply_xsection_xform

def _get_price_column(df: pd.DataFrame) -> str:
    """
    自動偵測價格欄位。
    優先順序: adj_close > close > Close
    """
    for col in ["adj_close", "close", "Close"]:
        if col in df.columns:
            return col
    
    raise KeyError(
        f"Price column not found in input DataFrame. "
        f"Available columns: {list(df.columns)}"
    )

def compute_volatility(
    prices: pd.DataFrame,
    window_days: int,
) -> pd.DataFrame:
    """
    計算滾動波動度 (Rolling Standard Deviation of Returns)
    """
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    price_col = _get_price_column(prices)

    df = prices[["date", "stock_id", price_col]].copy()
    df.rename(columns={price_col: "adj_close"}, inplace=True)

    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    
    df = df.sort_values(["stock_id", "date"])

    df["ret"] = df.groupby("stock_id")["adj_close"].pct_change()

    # 使用完整 window 計算波動度，避免偏向短期噪音
    df["vol"] = df.groupby("stock_id")["ret"].transform(
        lambda x: x.rolling(window=window_days, min_periods=window_days).std()
    )

    df = df.dropna(subset=["vol"])
    df = df.rename(columns={"vol": "factor_value"})
    
    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)


def run_vol_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 vol 引擎入口。
    Fix: Added window, end_date arguments to match __init__.py dispatch.
    """
    params = kwargs
    
    # 預設 20 日 (約一個月)
    default_days = 20
    lookback = int(params.get("lookback_days", default_days))
    if lookback <= 0:
        lookback = default_days

    df_vol = compute_volatility(prices, window_days=lookback)
    if df_vol.empty:
        return df_vol

    # low-vol style: 波動低應該 factor 大，先 log1p 平滑，再取負號反轉
    df_vol["factor_value"] = -np.log1p(df_vol["factor_value"].astype(float))

    wide = df_vol.pivot(index="date", columns="stock_id", values="factor_value")
    wide = apply_xsection_xform(wide, strategy="zscore")
    long = wide.stack(dropna=True).reset_index()
    long.columns = ["date", "stock_id", "factor_value"]
    return long
