# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.liq_impl
Optimized by Gemini (Vectorized)
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
import pandas as pd

_TURNOVER_CANDS: List[str] = ["turnover", "turnover_rate", "turnover_ratio", "TurnoverRate", "volume_ratio"]

def compute_turnover_mean(prices: pd.DataFrame, *, window_days: int) -> pd.DataFrame:
    """
    向量化計算 Rolling Mean Turnover
    """
    if window_days <= 0:
        raise ValueError(f"window_days must be >0, got {window_days!r}")
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 1. 準備資料
    df = prices.copy(deep=False)
    # 欄位標準化邏輯 (Inline 簡化)
    if "date" not in df.columns: raise ValueError("Missing 'date'")
    stock_col = next((c for c in ("stock_id", "stock", "code", "symbol") if c in df.columns), None)
    if not stock_col: raise ValueError("Missing stock-id column")
    if stock_col != "stock_id": df = df.rename(columns={stock_col: "stock_id"})
    
    t_col = next((c for c in _TURNOVER_CANDS if c in df.columns), None)
    if t_col is None:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 2. 排序與轉型
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])
    
    vals = pd.to_numeric(df[t_col], errors="coerce")
    
    # 3. 向量化 Rolling Mean
    # 注意：這裡直接對整個 Series 做 transform 風格的計算
    rolling_mean = (
        df.groupby("stock_id")[t_col]
        .rolling(window=window_days, min_periods=window_days)
        .mean()
        .reset_index(0, drop=True) # 對齊原始 DataFrame index
    )

    df["factor_value"] = rolling_mean

    # 4. 清理結果
    df = df.dropna(subset=["factor_value"])
    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    return df[["date", "stock_id", "factor_value"]].sort_values(["date", "stock_id"]).reset_index(drop=True)

def run_liquidity_factor(*, prices: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    window_days = int(params.get("window_days", 0))
    if window_days <= 0:
        raise ValueError(f"invalid window_days={window_days!r}")
    return compute_turnover_mean(prices, window_days=window_days)