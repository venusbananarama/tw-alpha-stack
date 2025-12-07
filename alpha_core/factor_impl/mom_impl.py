# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.mom_impl

Momentum family (mom_6m, mom_12m, mom_short_resid).
Optimized by Gemini (Vectorized Implementation)
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd

from alpha_core.factor_xform import apply_xsection_xform


def _get_price_column(df: pd.DataFrame) -> str:
    """
    自動偵測價格欄位。
    優先順序: adj_close > close > Close
    """
    for col in ["adj_close", "close", "Close"]:
        if col in df.columns:
            return col

    raise KeyError(
        f"Price column not found in input DataFrame. "
        f"Available columns: {list(df.columns)}"
    )


def compute_momentum(
    prices: pd.DataFrame,
    *,
    lookback_days: int,
    end_date: date,
    skip_recent_days: int = 0,
    min_history_days: int = 0,
) -> pd.DataFrame:
    """
    向量化 Momentum 計算 (Vectorized)

    - 使用 log(p_t / p_{t-lookback})
    - 可選擇排除最近 skip_recent_days 以避免短期反轉
    - 可要求每檔至少 min_history_days 歷史才計算
    """
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    price_col = _get_price_column(prices)

    df = prices[["date", "stock_id", price_col]].copy()
    df.rename(columns={price_col: "adj_close"}, inplace=True)

    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    # 僅保留 end_date 以前的資料，並排除最近 skip_recent_days
    cutoff = pd.to_datetime(end_date) - timedelta(days=skip_recent_days)
    df = df[df["date"] <= cutoff]

    # 依個股檢查歷史長度；不足則整檔剔除
    if min_history_days > 0:
        counts = df.groupby("stock_id")["date"].transform("count")
        df = df[counts >= min_history_days]

    df = df.sort_values(["stock_id", "date"])

    df["past_price"] = df.groupby("stock_id")["adj_close"].shift(lookback_days)

    valid_mask = (df["adj_close"] > 0) & (df["past_price"] > 0)
    df = df.loc[valid_mask].copy()

    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df["factor_value"] = np.log(df["adj_close"] / df["past_price"])

    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)


def _per_date_residual(
    group: pd.DataFrame,
) -> pd.DataFrame:
    """
    對單一交易日做 M6 ~ M12 的 cross-sectional 回歸，輸出殘差 z-score。
    期望輸入欄位：
      - factor_value_short
      - factor_value_long
    """
    x = group["factor_value_long"].to_numpy()
    y = group["factor_value_short"].to_numpy()

    mask = np.isfinite(x) & np.isfinite(y)
    n = int(mask.sum())
    if n < 3:
        # 樣本太少，直接回傳 NaN，之後會被整體 drop
        group = group.copy()
        group["factor_value"] = np.nan
        return group[["date", "stock_id", "factor_value"]]

    x_use = x[mask]
    y_use = y[mask]

    x_mean = x_use.mean()
    y_mean = y_use.mean()
    dx = x_use - x_mean
    dy = y_use - y_mean
    var_x = np.dot(dx, dx)

    if var_x <= 0:
        # 市場 beta 幾乎沒有變動，退而求其次用 demean 替代殘差
        resid = y_use - y_mean
    else:
        beta = float(np.dot(dx, dy) / var_x)
        alpha = float(y_mean - beta * x_mean)
        y_hat = alpha + beta * x_use
        resid = y_use - y_hat

    # 殘差 → winsorize + z-score
    vals = resid.astype(float)

    mean_resid = float(vals.mean())
    std_resid = float(vals.std(ddof=0))
    if std_resid > 0:
        z = (vals - mean_resid) / std_resid
    else:
        z = vals - mean_resid

    # clip 在 ±5σ 避免極端值
    z = np.clip(z, -5.0, 5.0)

    result = np.full_like(y, np.nan, dtype=float)
    result[mask] = z

    out = group.copy()
    out["factor_value"] = result
    return out[["date", "stock_id", "factor_value"]]


def compute_mom_short_resid(
    prices: pd.DataFrame,
    *,
    lookback_short_days: int,
    lookback_long_days: int,
) -> pd.DataFrame:
    """
    計算短期 vs 長期動能殘差（mom_short_resid）。

    作法：
      1) 用既有 compute_momentum 分別算出：
         - M_short: lookback_short_days
         - M_long : lookback_long_days
      2) 對每個交易日 t，在橫斷面上做：
         M_short(i, t) ~ a_t + b_t * M_long(i, t)
         取殘差 r(i, t) 作為「短期驚奇」。
      3) 對同一交易日內的 r(i, t) 做 winsorize + z-score。

    回傳欄位：
      - date
      - stock_id
      - factor_value (z-scored residual)
    """
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    # 1) 計算短期 / 長期動能
    mom_short = compute_momentum(prices, lookback_days=int(lookback_short_days))
    mom_long = compute_momentum(prices, lookback_days=int(lookback_long_days))

    if mom_short.empty or mom_long.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = mom_short.merge(
        mom_long,
        on=["date", "stock_id"],
        how="inner",
        suffixes=("_short", "_long"),
    )

    if df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = df.rename(
        columns={
            "factor_value_short": "factor_value_short",
            "factor_value_long": "factor_value_long",
        }
    )

    # 2) 逐日做 cross-sectional regression + z-score
    df = df.groupby("date", group_keys=False).apply(_per_date_residual)

    # 3) 移除 NaN，整理輸出
    df = df.dropna(subset=["factor_value"])
    df = df.reset_index(drop=True)

    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    return df[["date", "stock_id", "factor_value"]]


def run_mom_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 mom 引擎統一入口。
    Accepts window/end_date explicit context.

    行為：
      - 若 kwargs 中同時包含 lookback_short_days / lookback_long_days，
        則視為「殘差動能」（mom_short_resid）模式。
      - 否則維持舊有邏輯，依 window 或 lookback_days 決定單一 horizon。
    """
    params = dict(kwargs) if kwargs else {}

    # 殘差動能模式維持原樣
    if ("lookback_short_days" in params) or ("lookback_long_days" in params):
        default_short = 126
        default_long = 252

        lb_short = int(params.get("lookback_short_days", default_short))
        if lb_short <= 0:
            lb_short = default_short

        lb_long = int(params.get("lookback_long_days", max(lb_short, default_long)))
        if lb_long <= 0:
            lb_long = max(lb_short, default_long)

        df_resid = compute_mom_short_resid(
            prices,
            lookback_short_days=lb_short,
            lookback_long_days=lb_long,
        )
        if df_resid.empty:
            return df_resid

        wide = df_resid.pivot(index="date", columns="stock_id", values="factor_value")
        wide = apply_xsection_xform(wide, strategy="zscore")
        long = wide.stack(dropna=True).reset_index()
        long.columns = ["date", "stock_id", "factor_value"]
        return long

    # 一般 mom_6m：固定 lookback 為 126 交易日，排除最近 21 天雜訊
    lookback_days = 126
    skip_recent_days = int(params.get("skip_recent_days", 21))
    min_history_days = int(params.get("min_history_days", lookback_days + 63))

    df_mom = compute_momentum(
        prices,
        lookback_days=lookback_days,
        end_date=end_date,
        skip_recent_days=skip_recent_days,
        min_history_days=min_history_days,
    )
    if df_mom.empty:
        return df_mom

    # 橫斷面 winsorize + z-score，保留「越大越好」
    wide = df_mom.pivot(index="date", columns="stock_id", values="factor_value")
    wide = apply_xsection_xform(wide, strategy="zscore")
    long = wide.stack(dropna=True).reset_index()
    long.columns = ["date", "stock_id", "factor_value"]
    return long
