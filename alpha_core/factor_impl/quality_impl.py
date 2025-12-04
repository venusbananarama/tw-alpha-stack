# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.quality_impl
Optimized by Gemini (Cleaned)
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
import pandas as pd

_NET_INCOME_CANDS: List[str] = ["net_income", "netincome", "net_income_parent", "ni_parent", "ni", "NetIncome", "NetIncomeParent"]
_EQUITY_CANDS: List[str] = ["equity", "shareholders_equity", "total_equity", "Equity", "ShareholdersEquity", "TotalEquity"]

def _normalize_and_pick(df: pd.DataFrame, candidates: List[str]) -> tuple[pd.DataFrame, Optional[str]]:
    """標準化並尋找欄位 (共用邏輯)"""
    out = df.copy(deep=False) # 淺複製，節省記憶體
    if "date" not in out.columns:
        raise ValueError("Missing 'date' column")
    out["date"] = pd.to_datetime(out["date"])

    stock_col = next((c for c in ("stock_id", "stock", "code", "symbol") if c in out.columns), None)
    if not stock_col:
        raise ValueError(f"Missing stock-id column: {list(out.columns)}")
    
    if stock_col != "stock_id":
        out = out.rename(columns={stock_col: "stock_id"})
        
    target = next((c for c in candidates if c in out.columns), None)
    return out, target

def compute_roeq(financials: pd.DataFrame) -> pd.DataFrame:
    if financials.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 1. 處理 Net Income
    df, ni_col = _normalize_and_pick(financials, _NET_INCOME_CANDS)
    # 2. 處理 Equity (重複利用 df)
    eq_col = next((c for c in _EQUITY_CANDS if c in df.columns), None)

    if ni_col is None or eq_col is None:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 3. 轉型與計算
    ni_vals = pd.to_numeric(df[ni_col], errors="coerce")
    eq_vals = pd.to_numeric(df[eq_col], errors="coerce")

    mask = (eq_vals > 0) & ni_vals.notna()
    
    # 建立結果表
    res = df.loc[mask, ["date", "stock_id"]].copy()
    res["factor_value"] = ni_vals[mask] / eq_vals[mask]

    return res.sort_values(["date", "stock_id"]).reset_index(drop=True)

def run_quality_factor(*, financials: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    _ = params
    return compute_roeq(financials)