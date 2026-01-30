from __future__ import annotations

from typing import Dict, Mapping, Optional

import pandas as pd


def compute_risk_budget(
    returns_df: pd.DataFrame,
    weights: Mapping[str, float],
    topk_k: int,
    window: Optional[int] = None,
) -> Dict[str, object]:
    if returns_df.empty:
        return {
            "rc_by_symbol": {},
            "obs_count": 0,
            "port_var": None,
            "port_vol": None,
        }
    frame = returns_df.tail(int(window)) if window else returns_df
    frame = frame.dropna(how="all")
    if frame.empty:
        return {
            "rc_by_symbol": {},
            "obs_count": 0,
            "port_var": None,
            "port_vol": None,
        }
    cov = frame.cov(min_periods=2).fillna(0.0)
    w = pd.Series(weights, dtype="float64").reindex(cov.columns).fillna(0.0)
    port_var = float(w.T @ cov @ w)
    if port_var <= 0:
        return {
            "rc_by_symbol": {},
            "obs_count": int(len(frame)),
            "port_var": port_var,
            "port_vol": None,
        }
    mrc = cov @ w
    rc = (w * mrc) / port_var
    rc_abs = rc.abs().sort_values(ascending=False)
    topk = rc_abs.head(max(int(topk_k), 0))
    return {
        "rc_by_symbol": rc_abs.to_dict(),
        "obs_count": int(len(frame)),
        "port_var": port_var,
        "port_vol": float(port_var**0.5) * (252.0**0.5),
        "max_single_rc": float(rc_abs.max()) if not rc_abs.empty else 0.0,
        "topk_rc": float(topk.sum()) if not topk.empty else 0.0,
        "topk_symbols": list(topk.index),
    }
