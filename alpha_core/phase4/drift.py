from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def compute_daily_drift_metrics(
    replay_stats_df: pd.DataFrame,
    exec_trades_df: pd.DataFrame | None,
    market_trades_df: pd.DataFrame | None,
    *,
    mode: str = "slippage_vs_pred",
) -> pd.DataFrame:
    if replay_stats_df is None or replay_stats_df.empty:
        return pd.DataFrame()

    df = replay_stats_df.copy()
    ts_col = None
    for cand in ("exec_ts", "ts", "timestamp", "mkt_ts"):
        if cand in df.columns:
            ts_col = cand
            break
    if ts_col is None and "as_of" in df.columns:
        df["date"] = pd.to_datetime(df["as_of"], errors="coerce").dt.date
    else:
        df["date"] = pd.to_datetime(df[ts_col], errors="coerce").dt.date

    if "pred_slippage_bps" in df.columns:
        diff = pd.to_numeric(df["slippage_bps"], errors="coerce") - pd.to_numeric(df["pred_slippage_bps"], errors="coerce")
        drift_bps = diff.abs()
    elif "slippage_bps" in df.columns:
        drift_bps = pd.to_numeric(df["slippage_bps"], errors="coerce").abs()
    elif "slippage_bps_p50" in df.columns:
        drift_bps = pd.to_numeric(df["slippage_bps_p50"], errors="coerce").abs()
    else:
        drift_bps = pd.Series([], dtype=float)

    df["drift_bps"] = drift_bps
    df = df[df["date"].notna()]
    if df.empty:
        return pd.DataFrame()

    rows = []
    for day, grp in df.groupby("date", sort=True):
        vals = pd.to_numeric(grp["drift_bps"], errors="coerce").dropna()
        if vals.empty:
            continue
        drift_value_bps = float(np.median(vals))
        drift_value_pct = drift_value_bps / 100.0
        rows.append(
            {
                "date": day,
                "drift_value_pct": drift_value_pct,
                "n_trades": int(len(vals)),
            }
        )
    return pd.DataFrame(rows)


def aggregate_monthly_drift(drift_df: pd.DataFrame) -> pd.DataFrame:
    if drift_df is None or drift_df.empty:
        return pd.DataFrame()

    df = drift_df.copy()
    df["month"] = pd.to_datetime(df["date"], errors="coerce").dt.to_period("M").astype(str)
    rows = []
    for month, grp in df.groupby("month", sort=True):
        vals = pd.to_numeric(grp["drift_value_pct"], errors="coerce").dropna()
        if vals.empty:
            continue
        rows.append(
            {
                "month": month,
                "drift_value_pct": float(np.median(vals)),
                "drift_median_pct": float(np.median(vals)),
                "n_days": int(len(vals)),
                "status": "pass",
            }
        )
    return pd.DataFrame(rows)


def evaluate_drift_gate(monthly_df: pd.DataFrame, *, median_threshold_pct: float = 0.3, min_days: int = 5) -> Dict[str, object]:
    if monthly_df is None or monthly_df.empty:
        return {
            "pass": False,
            "status": "insufficient_data",
            "median_pct": None,
            "threshold_pct": float(median_threshold_pct),
            "n_months": 0,
        }

    df = monthly_df.copy()
    total_days = int(df["n_days"].sum()) if "n_days" in df.columns else 0
    if total_days < min_days:
        return {
            "pass": False,
            "status": "insufficient_data",
            "median_pct": None,
            "threshold_pct": float(median_threshold_pct),
            "n_months": int(len(df)),
        }

    median_val = float(df["drift_median_pct"].median())
    ok = median_val <= float(median_threshold_pct)
    return {
        "pass": bool(ok),
        "status": "pass" if ok else "fail",
        "median_pct": median_val,
        "threshold_pct": float(median_threshold_pct),
        "n_months": int(len(df)),
    }
