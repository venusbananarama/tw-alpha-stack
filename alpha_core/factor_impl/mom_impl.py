# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.mom_impl

Momentum family (mom_6m, mom_12m).
Optimized by Gemini (Vectorized Implementation)
"""

from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, Any

def compute_momentum(
    prices: pd.DataFrame,
    *,
    lookback_days: int,
) -> pd.DataFrame:
    """
    向量化 Momentum 計算 (Vectorized)
    效能大幅優化，移除 groupby loop。
    """
    # 1. 為了確保 shift 正確，先對整張表進行一次排序
    # 只取需要的欄位以節省記憶體
    df = prices[["date", "stock_id", "adj_close"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])

    # 2. 使用 Groupby + Shift 進行向量化位移 (不切斷 DataFrame)
    # 這比 for loop 快非常多
    df["past_price"] = df.groupby("stock_id")["adj_close"].shift(lookback_days)

    # 3. 處理數據清洗 (Vectorized filtering)
    # 確保當期價格與過去價格都 > 0 (避免 log 報錯)
    # 且 past_price 不為 NaN (shift 產生的空值)
    valid_mask = (df["adj_close"] > 0) & (df["past_price"] > 0)
    df = df.loc[valid_mask].copy()

    # 4. 若全空則提早返回
    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 5. 計算 Factor Value
    df["factor_value"] = np.log(df["adj_close"] / df["past_price"])

    # 6. 整理並回傳
    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)


def run_mom_factor(
    *,
    prices: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    """
    Phase-2 mom 引擎統一入口。
    """
    # 增加錯誤處理：確保 lookback_days 存在且合法
    lb = int(params.get("lookback_days", 0))
    if lb <= 0:
        raise ValueError(f"invalid lookback_days={lb}. strict positive integer required.")

    return compute_momentum(prices, lookback_days=lb)