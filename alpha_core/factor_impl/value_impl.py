# alpha_core/factor_impl/value_impl.py
# -*- coding: utf-8 -*-
"""
Value factor implementation (PE-based).

目標：
- 輸入：Phase-1 的 per 銀河表（含 date / stock_id / 某種 PE 欄位）。
- 輸出：date / stock_id / factor_value（越便宜 → 值越大），供 Phase-2 使用。
- 不動 Gate / SLO 憲法，只把因子本身算得乾淨、穩定。

設計：
- 先從 per 裡找出「PE 類」欄位（自動偵測常見命名）。
- 把 PE 映射成 value_raw = 1 / PE。
- 依每個 date 做：
    1. winsorize（1% / 99%）
    2. 橫斷面 z-score → factor_value
"""

from __future__ import annotations

from datetime import date
from typing import Any, List, Optional

import numpy as np
import pandas as pd


def _resolve_pe_column(df: pd.DataFrame) -> str:
    """
    嘗試從 per 資料裡找出「PE 類」欄位名稱。

    會用 lower-case 來比對常見欄位名稱：
    - 'pe', 'per', 'pe_ttm', 'pe_ratio', 'peratio' 等

    找不到時會 raise ValueError，讓上層 log 出來。
    """
    lower_map = {c.lower(): c for c in df.columns}

    candidates: List[str] = [
        "pe",
        "per",
        "pe_ttm",
        "per_ttm",
        "pe_ratio",
        "peratio",
        "pe1",
        "p/e",
    ]

    for name in candidates:
        if name in lower_map:
            return lower_map[name]

    # fallback：所有含 "pe" 的欄位都列出來
    fuzzy = [orig for orig in df.columns if "pe" in orig.lower()]
    if len(fuzzy) == 1:
        return fuzzy[0]

    raise ValueError(
        f"Unable to locate PE-like column in per dataframe. "
        f"Columns={list(df.columns)}"
    )


def _cs_winsorized_zscore(
    x: pd.Series,
    lower_quantile: float = 0.01,
    upper_quantile: float = 0.99,
) -> pd.Series:
    """
    單一橫斷面序列做 winsorize + z-score。

    - winsorize：依 quantile 截斷極端值。
    - z-score  ：(x - mean) / std；std 為 0 時回傳 0。
    """
    arr = x.to_numpy(dtype="float64")

    if arr.size == 0:
        return pd.Series(np.nan, index=x.index)

    lo = np.nanquantile(arr, lower_quantile)
    hi = np.nanquantile(arr, upper_quantile)

    arr = np.clip(arr, lo, hi)

    mu = np.nanmean(arr)
    sigma = np.nanstd(arr)

    if not np.isfinite(sigma) or sigma == 0.0:
        z = np.zeros_like(arr)
    else:
        z = (arr - mu) / sigma

    return pd.Series(z, index=x.index)


def compute_value_pe(
    per: pd.DataFrame,
    *,
    min_pe: float = 0.1,
    max_pe: float = 100.0,
    eps: float = 1e-12,
) -> pd.DataFrame:
    """
    從 per 銀河表計算 value_pe 因子。

    參數：
    - per   ：必須包含 date / stock_id / 某個 PE 類欄位。
    - min_pe：小於等於這個值的 PE 視為不可信（設 NaN）。
    - max_pe：大於等於這個值的 PE 視為極端（設 NaN）。
    - eps   ：避免除以 0 的微小補值。

    回傳：
    - df_factor：含 (date, stock_id, factor_value) 的 DataFrame。
    """
    if "date" not in per.columns or "stock_id" not in per.columns:
        raise ValueError("per dataframe must contain 'date' and 'stock_id' columns")

    df = per.copy()

    # 嘗試找出正確的 PE 欄位
    try:
        pe_col = _resolve_pe_column(df)
    except ValueError:
        # 若找不到 PE，回傳空，不讓程式 crash
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = df[["date", "stock_id", pe_col]].rename(columns={pe_col: "pe_raw"})
    df["pe_raw"] = pd.to_numeric(df["pe_raw"], errors="coerce")

    # 移除不合理或極端的 PE（虧損、超大倍數）
    df.loc[df["pe_raw"] <= min_pe, "pe_raw"] = np.nan
    df.loc[df["pe_raw"] >= max_pe, "pe_raw"] = np.nan

    # 1 / PE：越便宜 → 值越大
    # 注意：這裡只對正 PE 取倒數，負 PE 已經在上面被設為 NaN
    df["value_raw"] = 1.0 / (df["pe_raw"] + eps)

    # 依 date 做 winsor + z-score
    df["factor_value"] = (
        df.groupby("date", group_keys=False)["value_raw"]
        .transform(_cs_winsorized_zscore)
    )

    # 清理結果
    out = df[["date", "stock_id", "factor_value"]].copy()
    out = out.dropna(subset=["factor_value"])
    out = out.sort_values(["date", "stock_id"]).reset_index(drop=True)
    return out


def run_value_factor(
    *,
    per: pd.DataFrame,
    window: int,
    end_date: date,
    prices: Optional[pd.DataFrame] = None,  # 設為可選，兼容介面
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 引擎用的入口函式。

    簽名設計重點：
    - 保持與 factor_engine._route_and_compute 呼叫相容。
    - 顯式接收 window / end_date。
    - prices 為 Optional，避免 TypeError。

    實作：
    - 目前不直接使用 prices / window，只是為了 API 一致性。
      真正計算全部委託給 compute_value_pe()。
    """
    # 僅使用 per；prices / window 目前不進計算，但保留參數以免壞 API。
    # kwargs 可以用來傳遞 min_pe / max_pe 等參數
    
    # 簡單參數提取
    min_pe = float(kwargs.get("min_pe", 0.1))
    max_pe = float(kwargs.get("max_pe", 100.0))

    df_factor = compute_value_pe(per, min_pe=min_pe, max_pe=max_pe)
    return df_factor


__all__ = [
    "compute_value_pe",
    "run_value_factor",
]