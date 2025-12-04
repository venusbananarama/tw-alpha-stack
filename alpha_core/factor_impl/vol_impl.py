# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.vol_impl
Optimized by Gemini (Vectorized)
"""
from __future__ import annotations
from typing import Any, Dict
import numpy as np
import pandas as pd

def compute_realized_vol(
    prices: pd.DataFrame,
    *,
    window_days: int,
) -> pd.DataFrame:
    """
    向量化計算 Realized Volatility
    """
    if window_days <= 1:
        raise ValueError(f"window_days must be >1, got {window_days!r}")

    # 1. 準備資料與排序 (Groupby rolling 需要排序過的資料)
    df = prices[["date", "stock_id", "adj_close"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])

    # 2. 向量化計算 Log Return
    # shift(1) 會自動在每個 group 內部運作
    df["prev_close"] = df.groupby("stock_id")["adj_close"].shift(1)
    
    # 過濾無效價格 (避免 log(0) 錯誤)
    valid_mask = (df["adj_close"] > 0) & (df["prev_close"] > 0)
    df = df.loc[valid_mask].copy()
    
    df["log_ret"] = np.log(df["adj_close"] / df["prev_close"])

    # 3. 向量化 Rolling Std
    # groupby().rolling() 會產生 MultiIndex (stock_id, original_index)
    # 我們只取需要的 series 並重設 index 對齊回原表
    rolling_std = (
        df.groupby("stock_id")["log_ret"]
        .rolling(window=window_days, min_periods=window_days)
        .std(ddof=1)
        .reset_index(0, drop=True) # 移除 stock_id index level，對齊原 df index
    )

    df["factor_value"] = rolling_std

    # 4. 清理與輸出
    df = df.dropna(subset=["factor_value"])
    
    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    return df[["date", "stock_id", "factor_value"]].sort_values(["date", "stock_id"]).reset_index(drop=True)

def run_vol_factor(
    *,
    prices: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    window_days = int(params.get("window_days", 0))
    if window_days <= 1:
        raise ValueError(f"invalid window_days={window_days!r}")
    return compute_realized_vol(prices, window_days=window_days)