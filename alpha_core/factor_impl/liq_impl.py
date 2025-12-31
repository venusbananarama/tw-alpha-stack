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


_INVALID_VALUE_MARGIN = 1.0
_INVALID_VALUE_FALLBACK = -10.0
_INVALID_SPREAD_EPS = 1e-6


def _cs_soft_winsorized_zscore(
    x: pd.Series,
    lower_quantile: float = 0.01,
    upper_quantile: float = 0.99,
) -> pd.Series:
    arr = x.to_numpy(dtype="float64")
    if arr.size == 0:
        return pd.Series(np.nan, index=x.index)

    lo = np.nanquantile(arr, lower_quantile)
    hi = np.nanquantile(arr, upper_quantile)

    arr = arr.copy()
    if np.isfinite(lo):
        low_mask = arr < lo
        if np.any(low_mask):
            arr[low_mask] = lo - np.log1p(lo - arr[low_mask])
    if np.isfinite(hi):
        high_mask = arr > hi
        if np.any(high_mask):
            arr[high_mask] = hi + np.log1p(arr[high_mask] - hi)

    mu = np.nanmean(arr)
    sigma = np.nanstd(arr)
    if not np.isfinite(sigma) or sigma == 0.0:
        z = np.zeros_like(arr)
    else:
        z = (arr - mu) / sigma

    return pd.Series(z, index=x.index)


def _apply_invalid_floor_after_xform(
    values: pd.Series,
    dates: pd.Series,
    invalid_mask: pd.Series,
    *,
    stock_ids: Optional[pd.Series] = None,
    spread_eps: float = _INVALID_SPREAD_EPS,
    fallback: float = _INVALID_VALUE_FALLBACK,
) -> pd.Series:
    valid_values = values.where(~invalid_mask)
    min_by_date = valid_values.groupby(dates).transform("min")
    filled = values.copy()
    filled.loc[invalid_mask] = (min_by_date - _INVALID_VALUE_MARGIN).loc[invalid_mask]
    spread_max = spread_eps if np.isfinite(spread_eps) else 0.0
    spread_max = max(spread_max, min(_INVALID_VALUE_MARGIN * 0.5, 5e-4))
    spread_max = max(spread_max, 2e-4)
    spread_max = min(spread_max, _INVALID_VALUE_MARGIN * 0.9)
    if stock_ids is not None and spread_max > 0.0:
        invalid_idx = invalid_mask[invalid_mask].index
        if not invalid_idx.empty:
            tmp = pd.DataFrame(
                {
                    "date": dates.loc[invalid_idx],
                    "stock_id": stock_ids.loc[invalid_idx].astype(str),
                }
            )
            tmp = tmp.sort_values(["date", "stock_id"], kind="mergesort")
            tmp["rank"] = tmp.groupby("date").cumcount() + 1
            tmp["count"] = tmp.groupby("date")["stock_id"].transform("count")
            jitter = (tmp["rank"] / (tmp["count"] + 1.0)) * spread_max
            filled.loc[tmp.index] = filled.loc[tmp.index] - jitter
    filled = filled.replace([np.inf, -np.inf], np.nan)
    return filled.fillna(fallback)


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


def compute_adv(
    prices: pd.DataFrame,
    window_days: int,
    *,
    transform: str = "log1p",
    value_field_candidates: Optional[list[str]] = None,
) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    cols = df.columns
    if value_field_candidates:
        candidates = [str(c).strip() for c in value_field_candidates if str(c).strip()]
    else:
        candidates = [
            "Trading_money",
            "Trading_Money",
            "trading_money",
            "turnover_value",
            "Trading_turnover",
            "turnover",
        ]

    target_col = None
    for cand in candidates:
        if cand in cols:
            target_col = cand
            break

    if target_col is None and "close" in cols and "volume" in cols:
        df["adv_proxy"] = pd.to_numeric(df["close"], errors="coerce") * pd.to_numeric(
            df["volume"], errors="coerce"
        )
        target_col = "adv_proxy"
    elif target_col is None and "adj_close" in cols and "volume" in cols:
        df["adv_proxy"] = pd.to_numeric(df["adj_close"], errors="coerce") * pd.to_numeric(
            df["volume"], errors="coerce"
        )
        target_col = "adv_proxy"

    if target_col is None:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = df.sort_values(["stock_id", "date"])
    df[target_col] = pd.to_numeric(df[target_col], errors="coerce")
    df.loc[df[target_col] <= 0, target_col] = np.nan

    min_periods = max(1, window_days // 2)
    df["adv"] = df.groupby("stock_id")[target_col].transform(
        lambda x: x.rolling(window=window_days, min_periods=min_periods).mean()
    )

    adv = df["adv"]
    if transform == "log1p":
        adv = adv.where(adv > 0)
        df["factor_value"] = np.log1p(adv)
    elif transform == "log":
        adv = adv.where(adv > 0)
        df["factor_value"] = np.log(adv)
    elif transform in ("inv", "inverse"):
        adv = adv.where(adv > 0)
        df["factor_value"] = 1.0 / adv
    else:
        df["factor_value"] = adv

    df = df.dropna(subset=["factor_value"])
    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)


def compute_amihud(
    prices: pd.DataFrame,
    window_days: int,
    *,
    transform: str = "log1p",
) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    cols = df.columns
    price_cols = ("adj_close", "close", "Close", "price")
    price_col = next((c for c in price_cols if c in cols), None)
    if price_col is None:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = df.sort_values(["stock_id", "date"])
    df = df.drop_duplicates(subset=["stock_id", "date"], keep="last")

    price = pd.to_numeric(df[price_col], errors="coerce")
    price = price.where(price > 0)
    df["log_price"] = np.log(price)
    df["ret"] = df.groupby("stock_id")["log_price"].diff()

    money_cols = ("Trading_money", "Trading_Money", "trading_money")
    money_col = next((c for c in money_cols if c in cols), None)
    if money_col is not None:
        dollar_volume = pd.to_numeric(df[money_col], errors="coerce")
    else:
        volume_cols = ("volume", "Volume", "Trading_Volume", "Trading_volume", "trading_volume")
        volume_col = next((c for c in volume_cols if c in cols), None)
        if volume_col is None:
            return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

        if "close" in cols:
            price_for_dollar = pd.to_numeric(df["close"], errors="coerce")
        elif "adj_close" in cols:
            price_for_dollar = pd.to_numeric(df["adj_close"], errors="coerce")
        elif "Close" in cols:
            price_for_dollar = pd.to_numeric(df["Close"], errors="coerce")
        else:
            price_for_dollar = pd.to_numeric(df[price_col], errors="coerce")

        dollar_volume = price_for_dollar * pd.to_numeric(df[volume_col], errors="coerce")

    dollar_volume = dollar_volume.where(dollar_volume > 0)
    df["illiq"] = df["ret"].abs() / dollar_volume
    df["illiq"] = df["illiq"].replace([np.inf, -np.inf], np.nan)

    min_periods = max(1, window_days // 2)
    df["amihud"] = df.groupby("stock_id")["illiq"].transform(
        lambda x: x.rolling(window=window_days, min_periods=min_periods).mean()
    )

    amihud = df["amihud"]
    if transform == "log1p":
        amihud = amihud.where(amihud > 0)
        df["factor_value"] = np.log1p(amihud)
    elif transform == "log":
        amihud = amihud.where(amihud > 0)
        df["factor_value"] = np.log(amihud)
    elif transform in ("inv", "inverse"):
        amihud = amihud.where(amihud > 0)
        df["factor_value"] = 1.0 / amihud
    else:
        df["factor_value"] = amihud

    df = df.dropna(subset=["factor_value"])
    return df[["date", "stock_id", "factor_value"]].reset_index(drop=True)


def compute_liq_amihud_20d(
    prices: pd.DataFrame,
    *,
    window_days: int = 20,
    min_periods: int = 15,
) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    if "close" not in df.columns or "Trading_turnover" not in df.columns:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = df[["date", "stock_id", "close", "Trading_turnover"]].copy()
    df = df.dropna(subset=["date", "stock_id"])
    df["stock_id"] = df["stock_id"].astype(str)
    df = df.sort_values(["stock_id", "date"], kind="mergesort")
    df = df.drop_duplicates(subset=["date", "stock_id"], keep="last")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["Trading_turnover"] = pd.to_numeric(df["Trading_turnover"], errors="coerce")
    df.loc[df["close"] <= 0, "close"] = np.nan
    df.loc[df["Trading_turnover"] <= 0, "Trading_turnover"] = np.nan

    df["ret"] = df.groupby("stock_id")["close"].transform(
        lambda s: s / s.shift(1) - 1.0
    )
    df["illiq"] = df["ret"].abs() / (df["Trading_turnover"] + 1.0)
    df["illiq"] = df["illiq"].replace([np.inf, -np.inf], np.nan)

    min_periods = max(1, min(int(min_periods), int(window_days)))
    df["illiq_20d"] = df.groupby("stock_id")["illiq"].transform(
        lambda s: s.rolling(window=window_days, min_periods=min_periods).mean()
    )
    df["value_raw"] = np.log1p(df["illiq_20d"])
    df.loc[df["illiq_20d"] < 0, "value_raw"] = np.nan

    invalid = ~np.isfinite(df["value_raw"])
    df["factor_value"] = (
        df.groupby("date", group_keys=False)["value_raw"]
        .transform(_cs_soft_winsorized_zscore)
    )
    df["factor_value"] = _apply_invalid_floor_after_xform(
        df["factor_value"],
        df["date"],
        invalid,
        stock_ids=df["stock_id"],
    )

    out = df[["date", "stock_id", "factor_value"]].copy()
    return out.sort_values(["date", "stock_id"]).reset_index(drop=True)


def compute_liq_amihud_120d(
    prices: pd.DataFrame,
    *,
    window_days: int = 120,
    min_periods: int = 120,
    use_log_turnover: bool = True,
    rolling_method: str = "median",
) -> pd.DataFrame:
    if prices.empty:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = prices.copy()
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    if "close" not in df.columns or "Trading_turnover" not in df.columns:
        return pd.DataFrame(columns=["date", "stock_id", "factor_value"])

    df = df[["date", "stock_id", "close", "Trading_turnover"]].copy()
    df = df.dropna(subset=["date", "stock_id"])
    df["stock_id"] = df["stock_id"].astype(str)
    df = df.sort_values(["stock_id", "date"], kind="mergesort")
    df = df.drop_duplicates(subset=["date", "stock_id"], keep="last")

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["Trading_turnover"] = pd.to_numeric(df["Trading_turnover"], errors="coerce")
    df.loc[df["close"] <= 0, "close"] = np.nan
    df.loc[df["Trading_turnover"] <= 0, "Trading_turnover"] = np.nan

    df["ret"] = df.groupby("stock_id")["close"].transform(
        lambda s: s.pct_change(fill_method=None)
    )
    turnover = df["Trading_turnover"].where(df["Trading_turnover"] > 0)
    if use_log_turnover:
        turnover = np.log1p(turnover)
    turnover = turnover.where(turnover > 0)
    df["illiq"] = df["ret"].abs() / turnover
    df["illiq"] = df["illiq"].replace([np.inf, -np.inf], np.nan)

    min_periods = max(1, min(int(min_periods), int(window_days)))
    method = str(rolling_method or "median").lower()
    if method == "mean":
        df["illiq_120d"] = df.groupby("stock_id")["illiq"].transform(
            lambda s: s.rolling(window=window_days, min_periods=min_periods).mean()
        )
    else:
        df["illiq_120d"] = df.groupby("stock_id")["illiq"].transform(
            lambda s: s.rolling(window=window_days, min_periods=min_periods).median()
        )
    raw = df["illiq_120d"].where(df["illiq_120d"] >= 0)
    df["value_raw"] = -np.log1p(raw)

    invalid = ~np.isfinite(df["value_raw"])
    df["factor_value"] = (
        df.groupby("date", group_keys=False)["value_raw"]
        .transform(_cs_soft_winsorized_zscore)
    )
    df["factor_value"] = _apply_invalid_floor_after_xform(
        df["factor_value"],
        df["date"],
        invalid,
        stock_ids=df["stock_id"],
    )

    out = df[["date", "stock_id", "factor_value"]].copy()
    return out.sort_values(["date", "stock_id"]).reset_index(drop=True)


def run_liq_amihud_20d_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    factor_id: Optional[str] = None,  # noqa: ARG001
    **kwargs: Any,
) -> pd.DataFrame:
    window_days = int(kwargs.get("window_days", kwargs.get("turnover_lookback_days", 20)))
    min_periods = int(kwargs.get("min_periods", 15))
    return compute_liq_amihud_20d(
        prices,
        window_days=window_days,
        min_periods=min_periods,
    )


def run_liq_amihud_120d_factor(
    *,
    prices: pd.DataFrame,
    window: int,
    end_date: date,
    factor_id: Optional[str] = None,  # noqa: ARG001
    **kwargs: Any,
) -> pd.DataFrame:
    window_days = int(kwargs.get("window_days", 120))
    min_periods = int(kwargs.get("min_periods", 120))
    use_log_turnover = bool(kwargs.get("use_log_turnover", True))
    rolling_method = str(kwargs.get("rolling_method", "median"))
    return compute_liq_amihud_120d(
        prices,
        window_days=window_days,
        min_periods=min_periods,
        use_log_turnover=use_log_turnover,
        rolling_method=rolling_method,
    )


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
    factor_id = str(params.get("factor_id") or "").strip().lower()
    is_adv = factor_id == "adv_20d"
    is_amihud = factor_id == "amihud_20d"

    if is_adv or is_amihud:
        lookback = int(params.get("window_days", params.get("lookback_days", 20)))
    else:
        lookback = int(params.get("turnover_lookback_days", params.get("lookback_days", 20)))
    if lookback <= 0:
        lookback = 20

    transform = str(params.get("transform", "log1p")).lower()
    winsor_pctl = float(params.get("winsor_pctl", 0.0) or 0.0)
    do_zscore = bool(params.get("zscore", False))
    min_obs_per_day = int(params.get("min_obs_per_day", 50))
    direction = str(params.get("direction", "illiquid")).lower()

    if is_adv:
        value_field_candidates = params.get("value_field_candidates")
        if not isinstance(value_field_candidates, (list, tuple, set)):
            value_field_candidates = None
        df_liq = compute_adv(
            prices,
            window_days=lookback,
            transform=transform,
            value_field_candidates=value_field_candidates,
        )
    elif is_amihud:
        df_liq = compute_amihud(
            prices,
            window_days=lookback,
            transform=transform,
        )
    else:
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
    illiquid_native = is_amihud
    if direction == "illiquid":
        if not is_inverse and not illiquid_native:
            long["factor_value"] = -long["factor_value"]
    elif direction == "liquid":
        if is_inverse or illiquid_native:
            long["factor_value"] = -long["factor_value"]

    return long
