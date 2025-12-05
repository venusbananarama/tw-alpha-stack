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
    
    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)