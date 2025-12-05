# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.mom_impl

Momentum family (mom_6m, mom_12m).
Optimized by Gemini (Vectorized Implementation)
"""

from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import date
from typing import Any, Optional

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

def compute_momentum(
    prices: pd.DataFrame,
    *,
    lookback_days: int,
) -> pd.DataFrame:
    """
    向量化 Momentum 計算 (Vectorized)
    """
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 1. Identify Price Column
    price_col = _get_price_column(prices)

    # 2. Sort & Select
    # Rename target column to 'adj_close' internally for consistency
    df = prices[["date", "stock_id", price_col]].copy()
    df.rename(columns={price_col: "adj_close"}, inplace=True)

    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    
    df = df.sort_values(["stock_id", "date"])

    # 3. Shift
    df["past_price"] = df.groupby("stock_id")["adj_close"].shift(lookback_days)

    # 4. Filter
    valid_mask = (df["adj_close"] > 0) & (df["past_price"] > 0)
    df = df.loc[valid_mask].copy()

    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 5. Compute
    df["factor_value"] = np.log(df["adj_close"] / df["past_price"])

    # 6. Return
    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)


def run_mom_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 mom 引擎統一入口。
    Accepts window/end_date explicit context.
    """
    params = kwargs
    
    # 優先使用 params 中的設定，否則由 window 自動推算
    # 簡單假設：1 month approx 21 trading days
    default_days = int(window * 21)
    
    lb = int(params.get("lookback_days", default_days))
    if lb <= 0:
        lb = default_days if default_days > 0 else 21

    return compute_momentum(prices, lookback_days=lb)