from __future__ import annotations

from typing import Dict

import pandas as pd


def build_target_snapshot(target_df: pd.DataFrame, prices: Dict[str, float], nav: float) -> pd.DataFrame:
    out = target_df.copy()
    out["price"] = out["symbol"].map(prices)
    if out["price"].isna().any():
        missing = out.loc[out["price"].isna(), "symbol"].tolist()
        raise ValueError(f"missing prices for symbols: {missing[:10]}")
    out["target_notional"] = out["target_qty"] * out["price"]
    out["weight"] = out["target_notional"] / float(nav)
    return out


def scale_targets(target_df: pd.DataFrame, scale_factor: float) -> pd.DataFrame:
    out = target_df.copy()
    out["target_qty"] = out["target_qty"].apply(lambda x: int(float(x) * scale_factor))
    return out


def summarize_targets(target_df: pd.DataFrame, prices: Dict[str, float], nav: float) -> Dict[str, object]:
    snap = build_target_snapshot(target_df, prices, nav)
    gross = float(snap["target_notional"].abs().sum())
    net = float(snap["target_notional"].sum())
    return {
        "symbol_count": int(snap["symbol"].nunique()),
        "gross_notional": gross,
        "net_notional": net,
        "nav": float(nav),
        "gross_to_nav": float(gross / nav) if nav else None,
        "net_to_nav": float(net / nav) if nav else None,
    }
