# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.value_impl
Optimized by Gemini (Cleaned)
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional
import pandas as pd

_PE_CANDIDATES: List[str] = ["pe", "PE", "pe_ttm", "pe_positive", "pe_ttm_positive"]
_PB_CANDIDATES: List[str] = ["pb", "PB", "pb_ttm", "pb_positive", "pb_ttm_positive"]

def _normalize_and_get_col(df: pd.DataFrame, candidates: List[str]) -> tuple[pd.DataFrame, str]:
    """
    標準化 DataFrame 並找出目標欄位
    """
    # 淺層複製，避免修改原始資料
    out = df.copy(deep=False) 
    
    if "date" not in out.columns:
        raise ValueError("Missing 'date' column")
    out["date"] = pd.to_datetime(out["date"])

    # 找 stock_id
    stock_col = next((c for c in ("stock_id", "stock", "code", "symbol") if c in out.columns), None)
    if not stock_col:
        raise ValueError(f"Missing stock-id column: {list(out.columns)}")
    
    if stock_col != "stock_id":
        out = out.rename(columns={stock_col: "stock_id"})
        
    # 找 value 欄位
    target_col = next((c for c in candidates if c in out.columns), None)
    if not target_col:
        raise ValueError(f"None of candidate columns found: {candidates}")
        
    return out, target_col

def compute_reciprocal(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """通用計算倒數邏輯 (1/x)"""
    # 強制轉型並處理錯誤
    vals = pd.to_numeric(df[col_name], errors="coerce")
    
    # 向量化過濾與計算
    mask = vals > 0
    
    # 為了避免 SettingWithCopyWarning，這裡建立一個乾淨的新 DataFrame
    res = df.loc[mask, ["date", "stock_id"]].copy()
    res["factor_value"] = 1.0 / vals[mask]
    
    return res.sort_values(["date", "stock_id"]).reset_index(drop=True)

def compute_value_from_pe(per: pd.DataFrame) -> pd.DataFrame:
    df, col = _normalize_and_get_col(per, _PE_CANDIDATES)
    return compute_reciprocal(df, col)

def compute_value_from_pb(per: pd.DataFrame) -> pd.DataFrame:
    df, col = _normalize_and_get_col(per, _PB_CANDIDATES)
    return compute_reciprocal(df, col)

def run_value_factor(
    *,
    per: pd.DataFrame,
    params: Dict[str, Any],
) -> pd.DataFrame:
    mode = (params.get("mode") or "pe").lower()
    if mode == "pe":
        return compute_value_from_pe(per)
    elif mode == "pb":
        return compute_value_from_pb(per)
    else:
        raise ValueError(f"unsupported value_factor mode={mode!r}")