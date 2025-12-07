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
    以 equal-weighted universe 報酬作為市場，計算 rolling beta，並轉成
    low-beta style（beta 越低因子值越高）。
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
    
    # 預設 252 天；需要至少半窗才產生 beta
    lookback = int(kwargs.get("lookback_days", 252))
    if lookback <= 0:
        lookback = 252
    min_periods = max(lookback // 2, 20)

    # 個股報酬
    df["ret"] = df.groupby("stock_id")["adj_close"].pct_change()
    ret_panel = df.pivot(index="date", columns="stock_id", values="ret")

    if ret_panel.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # equal-weighted 市場報酬
    mkt_ret = ret_panel.mean(axis=1, skipna=True)
    mkt_var = mkt_ret.rolling(window=lookback, min_periods=min_periods).var()

    def _rolling_beta(col: pd.Series) -> pd.Series:
        cov = col.rolling(window=lookback, min_periods=min_periods).cov(mkt_ret)
        beta = cov / mkt_var
        # 轉成 low-beta：取負號
        return -beta

    beta_panel = ret_panel.apply(_rolling_beta, axis=0)
    beta_panel = beta_panel.replace([np.inf, -np.inf], np.nan).dropna(how="all")
    if beta_panel.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    beta_panel = apply_xsection_xform(beta_panel, strategy="zscore")
    long = beta_panel.stack(dropna=True).reset_index()
    long.columns = ["date", "stock_id", "factor_value"]
    return long
