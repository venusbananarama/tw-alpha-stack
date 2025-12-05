# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.quality_impl

Quality factor based on ROE / ROEQ.

設計重點：
- 只碰「因子怎麼算」，不改任何 Gate / SLO 規則。
- 從銀河 finstmt 資料裡找出 ROE / ROEQ 類欄位。
- 對每個交易日做：
    1) 去極值（winsorize）
    2) 標準化成 z-score
    3) 限制在 [-5, 5]
- 回傳欄位固定為：date, stock_id, factor_value

與 factor_engine 的介面：
- 被 factor_impl.__init__ 呼叫為 run_quality_factor(...)
- 需要支援參數：
    * finstmt: pd.DataFrame
    * window: int
    * end_date: datetime.date
    * **kwargs: 之後若想加 mode 等，可以從這裡讀
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
    嘗試從欄位裡找出 ROE / ROEQ 類型欄位。

    先用幾個常見名稱，找不到再用「名稱含 roe」的簡單 heuristics。
    找不到就 raise，讓上層 log 出來。
    """
    candidates: List[str] = [
        "roeq",
        "roe_q",
        "roe_ttm",
        "roe",
        "ROEQ",
        "ROE_TTM",
        "ROE",
    ]

    for col in candidates:
        if col in df.columns:
            return col

    # fallback：只要欄位名稱裡有 "roe" 就勉強當作 ROE
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

    Input:
        finstmt: 必須含有 date, stock_id, <roe_col>

    Output:
        DataFrame[date, stock_id, factor_value]
    """
    if finstmt is None or finstmt.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = finstmt.copy()

    # 基本欄位檢查
    if "date" not in df.columns:
        raise ValueError("finstmt is missing 'date' column")

    stock_col = None
    for cand in ("stock_id", "stock", "code", "symbol"):
        if cand in df.columns:
            stock_col = cand
            break
    if stock_col is None:
        raise ValueError(
            f"finstmt is missing stock-id column. columns={list(df.columns)}"
        )

    if stock_col != "stock_id":
        df = df.rename(columns={stock_col: "stock_id"})

    # date 正規化
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.loc[df["date"].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 找出 ROE 欄位
    roe_col = _find_roe_column(df)

    # 數值化
    vals = pd.to_numeric(df[roe_col], errors="coerce")
    mask = np.isfinite(vals)
    df = df.loc[mask, ["date", "stock_id"]].copy()
    df["raw"] = vals[mask]

    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 逐日做 winsorize + 標準化
    def _per_date_standardize(g: pd.DataFrame) -> pd.DataFrame:
        x = g["raw"]

        # 1) winsorize
        x_w = _winsorize_series(x)

        # 2) 計算平均與標準差
        mean = float(x_w.mean())
        std = float(x_w.std(ddof=0))

        if not np.isfinite(std) or std <= 0.0:
            # 如果這天分佈太奇怪，就全部給 0，避免發瘋
            g["factor_value"] = 0.0
        else:
            z = (x_w - mean) / std
            # 3) 限制極端值
            g["factor_value"] = z.clip(-5.0, 5.0)

        return g[["date", "stock_id", "factor_value"]]

    out = (
        df.groupby("date", group_keys=False)
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