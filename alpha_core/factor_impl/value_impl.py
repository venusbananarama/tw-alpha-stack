# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.value_impl
Optimized by Gemini (Cleaned)
"""
from __future__ import annotations
from typing import Any, List
from datetime import date
import pandas as pd

# Fix: 加入 FinMind 標準欄位 'PER' 和 'PBR'
_PE_CANDIDATES: List[str] = ["PER", "per", "PE", "pe", "pe_ttm", "pe_positive", "pe_ttm_positive"]
_PB_CANDIDATES: List[str] = ["PBR", "pbr", "PB", "pb", "pb_ttm", "pb_positive", "pb_ttm_positive"]

def _normalize_and_get_col(df: pd.DataFrame, candidates: List[str]) -> tuple[pd.DataFrame, str]:
    """
    標準化 DataFrame 並找出目標欄位
    """
    out = df.copy(deep=False) 
    
    if "date" not in out.columns:
        raise ValueError("Missing 'date' column")
    if not pd.api.types.is_datetime64_any_dtype(out["date"]):
        out["date"] = pd.to_datetime(out["date"])

    stock_col = next((c for c in ("stock_id", "stock", "code", "symbol") if c in out.columns), None)
    if not stock_col:
        raise ValueError(f"Missing stock-id column: {list(out.columns)}")
    
    if stock_col != "stock_id":
        out = out.rename(columns={stock_col: "stock_id"})
        
    target_col = next((c for c in candidates if c in out.columns), None)
    if not target_col:
        # 詳細列出目前有的欄位，方便除錯
        raise ValueError(f"None of candidate columns found: {candidates}. Available: {list(out.columns)}")
        
    return out, target_col

def compute_reciprocal(df: pd.DataFrame, col_name: str) -> pd.DataFrame:
    """通用計算倒數邏輯 (1/x)"""
    vals = pd.to_numeric(df[col_name], errors="coerce")
    mask = vals > 0
    
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
    window: int,
    end_date: date,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 value 引擎統一入口。
    """
    params = kwargs
    mode = (params.get("mode") or "pe").lower()
    
    if mode == "pe":
        return compute_value_from_pe(per)
    elif mode == "pb":
        return compute_value_from_pb(per)
    else:
        raise ValueError(f"unsupported value_factor mode={mode!r}")