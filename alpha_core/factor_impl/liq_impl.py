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

from alpha_core.factor_xform import apply_xsection_xform

def compute_turnover(
    prices: pd.DataFrame,
    window_days: int,
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
    
    # 取 log 避免極端值 (Log Turnover)
    df["factor_value"] = np.log1p(df["liq"])

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
    lookback = int(params.get("lookback_days", 20))
    if lookback <= 0:
        lookback = 20

    df_liq = compute_turnover(prices, window_days=lookback)
    if df_liq.empty:
        return df_liq

    # 高流動性應該因子值大；取 log1p 後直接標準化
    wide = df_liq.pivot(index="date", columns="stock_id", values="factor_value")
    wide = apply_xsection_xform(wide, strategy="zscore")
    long = wide.stack(dropna=True).reset_index()
    long.columns = ["date", "stock_id", "factor_value"]
    return long
