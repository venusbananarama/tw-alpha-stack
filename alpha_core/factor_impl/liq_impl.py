# -*- coding: utf-8 -*-
"""
alpha_core.factor_impl.liq_impl

Liquidity family (liq_turnover_20d, etc.)
Optimized by Gemini (Vectorized Implementation)
"""
from __future__ import annotations
from datetime import date
from typing import Any, Optional

import numpy as np
import pandas as pd

from alpha_core.factor_xform import apply_xsection_xform, winsorize_xsection


def _normalize_factor_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure factor frames use (date, stock_id, factor_value) with clean types."""
    if df is None or df.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    cols = df.columns
    stock_col = None
    for cand in ("stock_id", "stock", "code", "symbol"):
        if cand in cols:
            stock_col = cand
            break
    if stock_col is None:
        raise ValueError("factor frame missing stock_id column")

    value_col = None
    for cand in ("factor_value", "value", "factor"):
        if cand in cols:
            value_col = cand
            break
    if value_col is None:
        raise ValueError("factor frame missing factor_value column")

    out = df.copy()
    out.rename(columns={stock_col: "stock_id", value_col: "factor_value"}, inplace=True)
    out["date"] = pd.to_datetime(out["date"])
    out = out.dropna(subset=["date", "stock_id", "factor_value"])
    out["stock_id"] = out["stock_id"].astype(str)
    return out[["date", "stock_id", "factor_value"]].sort_values(["date", "stock_id"]).reset_index(drop=True)

def _extract_shares_outstanding(shareholding: Optional[pd.DataFrame]) -> pd.DataFrame:
    if shareholding is None or not isinstance(shareholding, pd.DataFrame) or shareholding.empty:
        return pd.DataFrame()

    df = shareholding.copy()
    if "date" not in df.columns:
        return pd.DataFrame()

    stock_col = None
    for cand in ("stock_id", "stock", "code", "symbol"):
        if cand in df.columns:
            stock_col = cand
            break
    if stock_col is None:
        return pd.DataFrame()

    share_cols = (
        "shares_outstanding",
        "Shares_outstanding",
        "outstanding_shares",
        "issued_shares",
        "total_shares",
        "shares",
        "NumberOfSharesIssued",
        "number_of_shares_issued",
        "capital",
        "share_capital",
    )
    shares_col = None
    for cand in share_cols:
        if cand in df.columns:
            shares_col = cand
            break
    if shares_col is None:
        return pd.DataFrame()

    df = df[["date", stock_col, shares_col]].copy()
    df.rename(columns={stock_col: "stock_id", shares_col: "shares_outstanding"}, inplace=True)
    df["date"] = pd.to_datetime(df["date"])
    df["stock_id"] = df["stock_id"].astype(str)
    df["shares_outstanding"] = pd.to_numeric(df["shares_outstanding"], errors="coerce")
    df.loc[df["shares_outstanding"] <= 0, "shares_outstanding"] = np.nan
    df = df.dropna(subset=["shares_outstanding"])
    df = df.sort_values(["stock_id", "date"])
    df = df.drop_duplicates(["date", "stock_id"], keep="last")
    return df


def compute_turnover(
    prices: pd.DataFrame,
    window_days: int,
    *,
    transform: str = "log1p",
    shareholding: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])
    
    # 檢查必要欄位
    cols = df.columns
    # 優先使用 turnover_rate-like 欄位，其次才用 shareholding-derived rate，再 fallback proxy
    rate_cols = ("turnover_rate", "Turnover_rate", "turnoverRatio", "turnover_ratio")
    value_cols = ("turnover", "Trading_turnover", "turnover_value", "total_turnover")

    target_col = None
    for cand in rate_cols:
        if cand in cols:
            target_col = cand
            break
    if target_col is None:
        shares_df = _extract_shares_outstanding(shareholding)
        if not shares_df.empty:
            df = df.sort_values(["stock_id", "date"])
            shares_df = shares_df.sort_values(["stock_id", "date"])
            df = pd.merge_asof(
                df,
                shares_df,
                by="stock_id",
                on="date",
                direction="backward",
                allow_exact_matches=True,
            )

            money_cols = ("Trading_money", "Trading_Money", "trading_money")
            close_cols = ("close", "Close")
            money_col = next((c for c in money_cols if c in cols), None)
            close_col = next((c for c in close_cols if c in cols), None)
            if money_col is not None and close_col is not None:
                money = pd.to_numeric(df[money_col], errors="coerce")
                money = money.where(money > 0)
                close_val = pd.to_numeric(df[close_col], errors="coerce")
                close_val = close_val.where(close_val > 0)
                mcap = df["shares_outstanding"] * close_val
                mcap = mcap.where(mcap > 0)
                df["turnover_rate_calc"] = money / mcap
                if df["turnover_rate_calc"].notna().any():
                    target_col = "turnover_rate_calc"
                else:
                    df = df.drop(columns=["turnover_rate_calc"], errors="ignore")

            if target_col is None:
                volume_cols = ("Trading_Volume", "Trading_volume", "trading_volume")
                volume_col = next((c for c in volume_cols if c in cols), None)
                if volume_col is not None:
                    vol = pd.to_numeric(df[volume_col], errors="coerce")
                    vol = vol.where(vol > 0)
                    df["turnover_rate_calc"] = vol / df["shares_outstanding"]
                    if df["turnover_rate_calc"].notna().any():
                        target_col = "turnover_rate_calc"
                    else:
                        df = df.drop(columns=["turnover_rate_calc"], errors="ignore")

    if target_col is None:
        for cand in value_cols:
            if cand in cols:
                target_col = cand
                break

    if target_col is None and "close" in cols and "volume" in cols:
        # 近似計算：收盤價 * 成交量
        df["turnover_proxy"] = pd.to_numeric(df["close"], errors="coerce") * pd.to_numeric(
            df["volume"], errors="coerce"
        )
        target_col = "turnover_proxy"
    elif target_col is None and "adj_close" in cols and "volume" in cols:
        # Fallback proxy
        df["turnover_proxy"] = pd.to_numeric(df["adj_close"], errors="coerce") * pd.to_numeric(
            df["volume"], errors="coerce"
        )
        target_col = "turnover_proxy"

    if target_col is None:
        # 缺資料，回傳空
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = df.sort_values(["stock_id", "date"])

    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")
    df.loc[df[target_col] <= 0, target_col] = np.nan

    # 計算滾動平均成交值 (Rolling Mean Turnover)
    min_periods = max(1, window_days // 2)
    df["liq"] = df.groupby("stock_id")[target_col].transform(
        lambda x: x.rolling(window=window_days, min_periods=min_periods).mean()
    )
    
    # transform
    liq = df["liq"]
    if transform == "log1p":
        liq = liq.where(liq > 0)
        df["factor_value"] = np.log1p(liq)
    elif transform == "log":
        liq = liq.where(liq > 0)
        df["factor_value"] = np.log(liq)
    elif transform in ("inv", "inverse"):
        liq = liq.where(liq > 0)
        df["factor_value"] = 1.0 / liq
    else:
        df["factor_value"] = liq

    df = df.dropna(subset=["factor_value"])
    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)


def run_liquidity_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    # 預留 shareholding/inst_total，目前未用
    shareholding: Optional[pd.DataFrame] = None,
    inst_total: Optional[pd.DataFrame] = None,
    **kwargs: Any,
) -> pd.DataFrame:
    """
    Phase-2 liq 引擎入口。
    Fix: Added window, end_date arguments to match __init__.py dispatch.
    """
    params = kwargs
    lookback = int(params.get("turnover_lookback_days", params.get("lookback_days", 20)))
    if lookback <= 0:
        lookback = 20

    transform = str(params.get("transform", "log1p")).lower()
    winsor_pctl = float(params.get("winsor_pctl", 0.0) or 0.0)
    do_zscore = bool(params.get("zscore", False))
    min_obs_per_day = int(params.get("min_obs_per_day", 50))
    direction = str(params.get("direction", "illiquid")).lower()

    shareholding_df = shareholding
    if shareholding_df is None and isinstance(params.get("shareholding"), pd.DataFrame):
        shareholding_df = params.get("shareholding")
    if shareholding_df is None and isinstance(params.get("_aux_factor_panels"), dict):
        aux_share = params.get("_aux_factor_panels", {}).get("shareholding")
        if isinstance(aux_share, pd.DataFrame):
            shareholding_df = aux_share

    df_liq = compute_turnover(
        prices,
        window_days=lookback,
        transform=transform,
        shareholding=shareholding_df,
    )
    if df_liq.empty:
        return df_liq

    df_liq = _normalize_factor_frame(df_liq)
    wide = df_liq.pivot(index="date", columns="stock_id", values="factor_value")
    wide = wide.sort_index()

    if winsor_pctl and winsor_pctl > 0:
        lower_q = max(0.0, min(winsor_pctl, 0.49))
        upper_q = 1.0 - lower_q
        wide = wide.apply(
            lambda row: winsorize_xsection(row, lower_q=lower_q, upper_q=upper_q),
            axis=1,
        )

    # size neutralization if provided / required
    neutralize_with = params.get("neutralize_with")
    aux_panels = params.get("_aux_factor_panels") if isinstance(params.get("_aux_factor_panels"), dict) else {}
    size_df = None

    require_size = False
    if neutralize_with:
        if isinstance(neutralize_with, str):
            require_size = neutralize_with == "size_log_mktcap"
        elif isinstance(neutralize_with, (list, tuple, set)):
            require_size = "size_log_mktcap" in set(map(str, neutralize_with))

    if require_size:
        if isinstance(aux_panels, dict):
            size_df = aux_panels.get("size_log_mktcap")
        if not isinstance(size_df, pd.DataFrame):
            raise ValueError(
                "liq_turnover_20d: required dependency size_log_mktcap not injected via _aux_factor_panels"
            )

    if isinstance(size_df, pd.DataFrame) and {"date", "stock_id", "factor_value"}.issubset(size_df.columns):
        size_df_norm = _normalize_factor_frame(size_df)
        size_wide = size_df_norm.pivot(index="date", columns="stock_id", values="factor_value")
        size_wide = size_wide.sort_index()

        common_dates = wide.index.intersection(size_wide.index)
        common_cols = wide.columns.intersection(size_wide.columns)
        wide = wide.loc[common_dates, common_cols]
        size_wide = size_wide.loc[common_dates, common_cols]

        resid_rows = []
        for dt, row in wide.iterrows():
            x = size_wide.loc[dt]
            y = row
            mask = x.notna() & y.notna()
            if mask.sum() < min_obs_per_day or x[mask].nunique() <= 1:
                resid_rows.append(pd.Series(np.nan, index=wide.columns, dtype=float))
                continue
            x1 = x[mask].astype(float)
            y1 = y[mask].astype(float)
            X = np.vstack([np.ones(len(x1)), x1.to_numpy()]).T
            try:
                beta, *_ = np.linalg.lstsq(X, y1.to_numpy(), rcond=None)
            except np.linalg.LinAlgError:
                resid_rows.append(pd.Series(np.nan, index=wide.columns, dtype=float))
                continue
            y_hat = X @ beta
            resid = pd.Series(np.nan, index=wide.columns, dtype=float)
            resid.loc[mask] = y1.to_numpy() - y_hat
            resid_rows.append(resid)
        wide = pd.DataFrame(resid_rows, index=wide.index)

    if do_zscore:
        wide = apply_xsection_xform(
            wide,
            strategy="zscore",
            winsor_limits=(0.0, 1.0),
        )

    long = wide.stack(dropna=True).reset_index()
    long.columns = ["date", "stock_id", "factor_value"]

    # direction handling: illiquid => larger values mean less liquid
    is_inverse = transform in ("inv", "inverse")
    if direction == "illiquid":
        if not is_inverse:
            long["factor_value"] = -long["factor_value"]
    elif direction == "liquid":
        if is_inverse:
            long["factor_value"] = -long["factor_value"]

    return long
