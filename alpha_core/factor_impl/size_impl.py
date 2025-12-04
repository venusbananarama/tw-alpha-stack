# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.size_impl
Optimized by Gemini (Cleaned)
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
import numpy as np
import pandas as pd

_MKTCAP_CANDS: List[str] = ["market_cap", "mktcap", "market_value", "total_market_value", "mv"]

def compute_log_mktcap(prices: pd.DataFrame) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices.copy(deep=False)
    
    # 標準化欄位
    if "date" not in df.columns: raise ValueError("Missing 'date'")
    df["date"] = pd.to_datetime(df["date"])
    
    stock_col = next((c for c in ("stock_id", "stock", "code", "symbol") if c in df.columns), None)
    if not stock_col: raise ValueError("Missing stock-id column")
    if stock_col != "stock_id": df = df.rename(columns={stock_col: "stock_id"})

    mc_col = next((c for c in _MKTCAP_CANDS if c in df.columns), None)
    if mc_col is None:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 計算
    vals = pd.to_numeric(df[mc_col], errors="coerce")
    mask = vals > 0
    
    res = df.loc[mask, ["date", "stock_id"]].copy()
    res["factor_value"] = np.log(vals[mask])

    return res.sort_values(["date", "stock_id"]).reset_index(drop=True)

def run_size_factor(*, prices: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    _ = params
    return compute_log_mktcap(prices)