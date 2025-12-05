# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.vol_impl

Volatility family (vol_20d, etc.)
Optimized by Gemini (Vectorized Implementation)
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import date
from typing import Any, Dict

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

    # 1. Identify Price Column (Fix for KeyError)
    price_col = _get_price_column(prices)

    # 2. Prepare
    df = prices[["date", "stock_id", price_col]].copy()
    # Rename to internal standard 'adj_close'
    df.rename(columns={price_col: "adj_close"}, inplace=True)

    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    
    df = df.sort_values(["stock_id", "date"])

    # 3. Calculate Returns
    # groupby -> pct_change
    df["ret"] = df.groupby("stock_id")["adj_close"].pct_change()

    # 4. Rolling Std
    # min_periods 設為 window的一半，避免初期資料不足全變 NaN
    df["vol"] = df.groupby("stock_id")["ret"].transform(
        lambda x: x.rolling(window=window_days, min_periods=max(1, window_days // 2)).std()
    )

    # 5. Cleanup
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
    # 如果 params 有指定 lookback_days 則優先使用
    lookback = int(params.get("lookback_days", default_days))
    
    return compute_volatility(prices, window_days=lookback)