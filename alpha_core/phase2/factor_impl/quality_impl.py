# -*- coding: utf-8 -*-
"""
alpha_core.phase2.factor_impl.quality_impl

Quality factor based on ROE / ROEQ.
Optimized by Gemini (Vectorized Implementation)

修復重點：
- 支援 FinMind 的 Long Format (type/value) 格式。
- 保留原本的 Wide Format (Columns) 支援。
- 統一執行 Winsorize -> Z-Score -> Clip 標準化流程。
- Fix: 解決 SettingWithCopyWarning (明確使用 .copy())
"""

from __future__ import annotations

from datetime import date
from typing import Any, List

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# 小工具
# ---------------------------------------------------------------------------


def _find_roe_column(df: pd.DataFrame) -> str:
    """
    嘗試從欄位裡找出 ROE / ROEQ 類型欄位 (用於 Wide Format)。
    """
    candidates: List[str] = [
        "roeq",
        "roe_q",
        "roe_ttm",
        "roe",
        "ROEQ",
        "ROE_TTM",
        "ROE",
        "ReturnOnEquity", # FinMind 標準名稱
    ]

    for col in candidates:
        if col in df.columns:
            return col

    # fallback
    for col in df.columns:
        if "roe" in col.lower():
            return col

    raise ValueError(
        f"No ROE/ROEQ-like column found in finstmt columns={list(df.columns)[:20]} ..."
    )


def _winsorize_series(
    s: pd.Series, lower_q: float = 0.01, upper_q: float = 0.99
) -> pd.Series:
    """
    簡單 winsorize：把低於 1% / 高於 99% 的值夾回來。
    """
    if s.empty:
        return s

    lo = s.quantile(lower_q)
    hi = s.quantile(upper_q)

    return s.clip(lower=lo, upper=hi)


# ---------------------------------------------------------------------------
# 品質因子計算邏輯
# ---------------------------------------------------------------------------


def compute_quality_from_roe(finstmt: pd.DataFrame) -> pd.DataFrame:
    """
    從 finstmt 取 ROE / ROEQ 類欄位，轉成日頻品質因子。
    自動適應 Long Format (type/value) 與 Wide Format (columns)。
    """
    if finstmt is None or finstmt.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = finstmt.copy()

    # 1. 基本欄位檢查與正規化
    if "date" not in df.columns:
        raise ValueError("finstmt is missing 'date' column")

    # 找 stock_id
    stock_col = next((c for c in ("stock_id", "stock", "code", "symbol") if c in df.columns), None)
    if stock_col is None:
        raise ValueError(f"finstmt is missing stock-id column. columns={list(df.columns)}")
    
    if stock_col != "stock_id":
        df = df.rename(columns={stock_col: "stock_id"})

    # date 正規化
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.dropna(subset=["date"]).copy()
    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 2. 資料提取 (Extraction)
    # 分支 A: Long Format (FinMind 原始格式 - type/value)
    if "type" in df.columns and "value" in df.columns:
        # 定義可能的 ROE 鍵值 (FinMind 常見名稱)
        roe_keys = ["ReturnOnEquity", "ROE", "ROEQ", "EPS"] # 優先找 ReturnOnEquity
        
        # 篩選 rows
        mask = df["type"].isin(roe_keys)
        target_df = df.loc[mask].copy()
        
        if target_df.empty:
             # 若找不到 ROE，回傳空 (不報錯，視為無數據)
             return pd.DataFrame(columns=["date", "stock_id", "factor_value"])
        
        # 如果有多種 type 同時存在，這裡簡單去重或取第一個
        # 將 'value' 改名為 'raw' 以便後續處理
        target_df = target_df.rename(columns={"value": "raw"})
        # Fix: 加入 .copy() 避免 SettingWithCopyWarning
        df_proc = target_df[["date", "stock_id", "raw"]].copy()

    # 分支 B: Wide Format (已轉置過的格式 - ROE 在欄位名)
    else:
        roe_col = _find_roe_column(df)
        df_proc = df[["date", "stock_id", roe_col]].copy()
        df_proc = df_proc.rename(columns={roe_col: "raw"})

    # 3. 數值處理 (Processing)
    df_proc["raw"] = pd.to_numeric(df_proc["raw"], errors="coerce")
    df_proc = df_proc.dropna(subset=["raw"])

    if df_proc.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 4. 逐日標準化 (Winsorize + Z-Score)
    def _per_date_standardize(g: pd.DataFrame) -> pd.DataFrame:
        x = g["raw"]
        
        # winsorize
        x_w = _winsorize_series(x)
        
        # z-score
        mean = float(x_w.mean())
        std = float(x_w.std(ddof=0))

        if not np.isfinite(std) or std <= 0.0:
            g["factor_value"] = 0.0
        else:
            z = (x_w - mean) / std
            # 3) 限制極端值
            g["factor_value"] = z.clip(-5.0, 5.0)

        return g[["date", "stock_id", "factor_value"]]

    out = (
        df_proc.groupby("date", group_keys=False)
        .apply(_per_date_standardize)
        .sort_values(["date", "stock_id"])
        .reset_index(drop=True)
    )

    return out


# ---------------------------------------------------------------------------
# 對外入口（factor_engine 用）
# ---------------------------------------------------------------------------


def run_quality_factor(
    *,
    finstmt: pd.DataFrame,
    window: int,
    end_date: date,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 quality_roeq 統一入口。

    目前：
    - window / end_date 暫時只當 context，不直接參與計算，
      但保留在函式簽名裡，跟其他因子家族保持一致。
    - 未來若要支援多種品質定義，可以從 kwargs["mode"] 讀取。

    回傳：
        DataFrame[date, stock_id, factor_value]
    """
    mode = (kwargs.get("mode") or "roe").lower()

    if mode in ("roe", "roeq"):
        return compute_quality_from_roe(finstmt)
    else:
        raise ValueError(f"Unsupported quality factor mode={mode!r}")
