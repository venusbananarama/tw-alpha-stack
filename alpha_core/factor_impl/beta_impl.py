# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.beta_impl

Beta family (beta_252d).
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from datetime import date
from typing import Any

def _get_price_column(df: pd.DataFrame) -> str:
    for col in ["adj_close", "close", "Close"]:
        if col in df.columns:
            return col
    return "adj_close" # Fallback, might fail later but consistent

def run_beta_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 beta 引擎入口。
    目前尚未接入大盤指數 (Benchmark)，暫以 '波動度' 作為 Beta 的代理 (Proxy)，
    確保 Pipeline 暢通。
    """
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # Smart column detection
    price_col = _get_price_column(prices)
    if price_col not in prices.columns:
         return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices[["date", "stock_id", price_col]].copy()
    df.rename(columns={price_col: "adj_close"}, inplace=True)

    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    
    df = df.sort_values(["stock_id", "date"])
    
    # 預設 252 天
    lookback = int(kwargs.get("lookback_days", 252))
    
    # Proxy implementation: Rolling Volatility
    df["ret"] = df.groupby("stock_id")["adj_close"].pct_change()
    df["beta_proxy"] = df.groupby("stock_id")["ret"].transform(
        lambda x: x.rolling(window=lookback, min_periods=lookback//2).std()
    )
    
    df = df.dropna(subset=["beta_proxy"])
    df = df.rename(columns={"beta_proxy": "factor_value"})
    
    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)