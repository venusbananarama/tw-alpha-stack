from __future__ import annotations

from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd

from .schemas import REF_PRICE_MODE_LAST, REF_PRICE_MODE_VWAP


def _ensure_datetime(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        return series
    return pd.to_datetime(series, errors="coerce")


def align_exec_to_market(
    exec_trades_df: pd.DataFrame,
    market_trades_df: pd.DataFrame,
    *,
    mode: str = REF_PRICE_MODE_LAST,
    window_sec: int = 5,
    tolerance_ms: Optional[int] = None,
) -> pd.DataFrame:
    if exec_trades_df.empty:
        return pd.DataFrame()
    if market_trades_df.empty:
        out = exec_trades_df.copy()
        out["ref_price"] = np.nan
        out["ref_ts"] = pd.NaT
        out["ref_mode"] = mode
        out["window_sec"] = int(window_sec)
        out["mkt_window_qty"] = np.nan
        out["mkt_window_notional"] = np.nan
        out["missing_ref"] = True
        return out

    exec_df = exec_trades_df.copy()
    mkt_df = market_trades_df.copy()
    exec_df["exec_ts"] = _ensure_datetime(exec_df["exec_ts"] if "exec_ts" in exec_df.columns else exec_df["ts"])
    mkt_df["mkt_ts"] = _ensure_datetime(mkt_df["ts"])

    exec_df = exec_df.sort_values(["symbol", "exec_ts", "trade_id"], kind="mergesort")
    mkt_df = mkt_df.sort_values(["symbol", "mkt_ts"], kind="mergesort")

    mode = mode or REF_PRICE_MODE_LAST
    if mode == REF_PRICE_MODE_LAST:
        mkt_df = mkt_df.rename(columns={"price": "mkt_price", "qty": "mkt_qty"})
        if "side" in mkt_df.columns:
            mkt_df = mkt_df.rename(columns={"side": "mkt_side"})
        tol = None
        if tolerance_ms is not None:
            tol = pd.Timedelta(milliseconds=int(tolerance_ms))
        merged = pd.merge_asof(
            exec_df,
            mkt_df,
            left_on="exec_ts",
            right_on="mkt_ts",
            by="symbol",
            direction="backward",
            tolerance=tol,
        )
        merged["ref_price"] = merged["mkt_price"]
        merged["ref_ts"] = merged["mkt_ts"]
        merged["ref_mode"] = REF_PRICE_MODE_LAST
        merged["window_sec"] = int(window_sec)
        merged["mkt_window_qty"] = np.nan
        merged["mkt_window_notional"] = np.nan
        merged["missing_ref"] = merged["ref_price"].isna()
        return merged

    if mode != REF_PRICE_MODE_VWAP:
        raise ValueError(f"unsupported ref price mode: {mode}")

    rows = []
    for symbol, exec_group in exec_df.groupby("symbol", sort=False):
        mkt_group = mkt_df[mkt_df["symbol"] == symbol]
        if mkt_group.empty:
            for _, row in exec_group.iterrows():
                out = row.to_dict()
                out["ref_price"] = np.nan
                out["ref_ts"] = pd.NaT
                out["ref_mode"] = REF_PRICE_MODE_VWAP
                out["window_sec"] = int(window_sec)
                out["mkt_window_qty"] = np.nan
                out["mkt_window_notional"] = np.nan
                out["missing_ref"] = True
                rows.append(out)
            continue

        mkt_ts = mkt_group["mkt_ts"].to_numpy()
        mkt_qty = pd.to_numeric(mkt_group["qty"], errors="coerce").fillna(0.0).to_numpy()
        mkt_px = pd.to_numeric(mkt_group["price"], errors="coerce").fillna(0.0).to_numpy()
        mkt_notional = mkt_qty * mkt_px
        cum_qty = np.cumsum(mkt_qty)
        cum_notional = np.cumsum(mkt_notional)

        for _, row in exec_group.iterrows():
            end_ts = row["exec_ts"]
            start_ts = end_ts - timedelta(seconds=int(window_sec))
            left = np.searchsorted(mkt_ts, start_ts, side="left")
            right = np.searchsorted(mkt_ts, end_ts, side="right")
            if right <= left:
                out = row.to_dict()
                out["ref_price"] = np.nan
                out["ref_ts"] = pd.NaT
                out["ref_mode"] = REF_PRICE_MODE_VWAP
                out["window_sec"] = int(window_sec)
                out["mkt_window_qty"] = 0.0
                out["mkt_window_notional"] = 0.0
                out["missing_ref"] = True
                rows.append(out)
                continue

            qty_sum = float(cum_qty[right - 1] - (cum_qty[left - 1] if left > 0 else 0.0))
            notional_sum = float(cum_notional[right - 1] - (cum_notional[left - 1] if left > 0 else 0.0))
            if qty_sum <= 0:
                ref_px = np.nan
            else:
                ref_px = notional_sum / qty_sum
            out = row.to_dict()
            out["ref_price"] = ref_px
            out["ref_ts"] = end_ts
            out["ref_mode"] = REF_PRICE_MODE_VWAP
            out["window_sec"] = int(window_sec)
            out["mkt_window_qty"] = qty_sum
            out["mkt_window_notional"] = notional_sum
            out["missing_ref"] = not pd.notna(ref_px)
            rows.append(out)

    out_df = pd.DataFrame(rows)
    return out_df
