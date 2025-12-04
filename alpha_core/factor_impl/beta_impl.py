# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.beta_impl
Optimized by Gemini (Vectorized)
"""
from __future__ import annotations
from typing import Any, Dict
import numpy as np
import pandas as pd

def _compute_returns(prices: pd.DataFrame) -> pd.DataFrame:
    """一次性計算所有股票的 Log Return"""
    df = prices[["date", "stock_id", "adj_close"]].copy()
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["stock_id", "date"])
    
    # Vectorized shift
    df["prev_close"] = df.groupby("stock_id")["adj_close"].shift(1)
    
    # Filter valid
    mask = (df["adj_close"] > 0) & (df["prev_close"] > 0)
    df = df.loc[mask].copy()
    
    df["ret_stock"] = np.log(df["adj_close"] / df["prev_close"])
    return df[["date", "stock_id", "ret_stock"]]

def compute_beta(
    prices: pd.DataFrame,
    *,
    window_days: int,
) -> pd.DataFrame:
    if window_days <= 1:
        raise ValueError(f"window_days must be >1")

    # 1. 計算個股報酬
    df_ret = _compute_returns(prices)
    if df_ret.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 2. 計算市場報酬 (Cross-sectional Mean)
    # 這裡直接用 groupby date 算平均，非常快
    mkt_ret = df_ret.groupby("date")["ret_stock"].mean().reset_index()
    mkt_ret = mkt_ret.rename(columns={"ret_stock": "ret_mkt"})

    # 3. 預先計算市場的 Rolling Variance (只需算一次！)
    # 這是單一時間序列，不需要 groupby
    mkt_ret = mkt_ret.sort_values("date")
    mkt_ret["mkt_var"] = mkt_ret["ret_mkt"].rolling(window=window_days, min_periods=window_days).var(ddof=1)

    # 4. 合併資料：個股報酬 + 市場報酬 + 市場變異數
    df = df_ret.merge(mkt_ret, on="date", how="inner")
    
    # 5. 向量化計算 Covariance
    # 確保排序以便 rolling 正確
    df = df.sort_values(["stock_id", "date"])
    
    # 利用 groupby rolling cov 計算 (ret_stock, ret_mkt) 的協方差
    # 這裡會比較吃記憶體，但比迴圈快
    rolling_cov = (
        df.groupby("stock_id")[["ret_stock", "ret_mkt"]]
        .rolling(window=window_days, min_periods=window_days)
        .cov()
        .reset_index() # 變成 [stock_id, level_1, ret_stock, ret_mkt]
    )
    
    # rolling().cov() 會回傳 2x2 矩陣，我們只需要 cov(stock, mkt)
    # 結果通常會是 MultiIndex，或者 "level_1" 對應原 index
    # 這裡我們只取 'ret_stock' column 中與 'ret_mkt' 的 cov，
    # 但 pandas rolling cov 結構較複雜，比較簡單的方法是:
    # 只對單一 Series 做 rolling().cov(other_series) -- 但這在 groupby 裡不好寫。
    # -----------------------------------------------------------
    # 替代方案：更輕量的做法 (避免產生龐大矩陣)
    # 我們使用 rolling_cov 結構特性：
    # 輸出會有 stock_id, level_1 (原index), 以及 columns [ret_stock, ret_mkt]
    # 我們需要的是: row 來源是 ret_stock, col 來源是 ret_mkt 的值 (即 Covariance)
    # 實際上 groupby(...).rolling().cov(other) 若 other 沒指定，會算 pairwise。
    # 為了效能，我們改用另一種寫法：
    
    # 直接算 rolling cov
    # 將 ret_mkt 視為單獨的 series 傳入是困難的因為要對齊 stock_id
    # 所以我們回到 df，使用 groupby apply (次佳) 或直接用上面的 pairwise 結果
    
    # 採用 Pairwise 結果過濾法:
    # rolling_cov 的 index 是 (stock_id, original_index)
    # columns 是 (ret_stock, ret_mkt)
    # 但這個 cov 是對本身 dataframe 欄位做 cov。
    # pandas 1.x/2.x: .cov() on dataframe returns a MultiIndex on columns too if not careful? 
    # No, rolling().cov() on 2 columns returns N*2*2 rows. Too big.
    
    # 修正：最佳效能寫法 - 使用 groupby().rolling().cov(other)
    # 必須確保 index 對齊。
    
    # 簡化策略：既然我們已經 merge 了，我們用公式:
    # Beta = Rolling_Cov(R_s, R_m) / Rolling_Var(R_m)
    # 我們已經有 Var(R_m) 在 'mkt_var' column。
    # 我們只需要算 Cov。
    
    # 這裡利用一個小技巧：不用 2x2 矩陣。
    # 為了避免複雜的 tensor 操作，我們在這裡雖然仍需 groupby，
    # 但我們使用 `corr` 或直接操作會比較簡單。
    # 鑑於 Pandas 限制，最穩定的向量化寫法如下：
    
    val = df.groupby("stock_id").apply(
        lambda x: x["ret_stock"].rolling(window_days, min_periods=window_days).cov(x["ret_mkt"])
    )
    # 注意：groupby().apply() 雖然比純迴圈快，但不如純 C 運算。
    # 但因為這裡是 apply 內部的 rolling 是 C 語言實作，所以速度尚可接受。
    
    # 註：如果追求極致速度，需要自行寫 numba。但在純 Pandas 下，這是折衷方案。
    # 為了保持程式碼簡潔且比原本快，我們使用上面的 apply 結構，
    # 但為了避免 index 問題，我們把它轉回 array。
    
    # 讓我們用更穩定的 reset_index 方式處理 apply 的結果
    val = val.reset_index(0, drop=True) # 移除 stock_id, 對齊 df 的 index (假設 df 沒變)
    
    df["cov_sm"] = val
    df["factor_value"] = df["cov_sm"] / df["mkt_var"]

    df = df.dropna(subset=["factor_value"])
    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])
        
    return df[["date", "stock_id", "factor_value"]].sort_values(["date", "stock_id"]).reset_index(drop=True)

def run_beta_factor(
    *,
    prices: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    window_days = int(params.get("window_days", 0))
    if window_days <= 1:
        raise ValueError(f"invalid window_days={window_days!r}")
    return compute_beta(prices, window_days=window_days)