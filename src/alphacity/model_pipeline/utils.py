\
import pandas as pd
import numpy as np

def ensure_sorted(df: pd.DataFrame) -> pd.DataFrame:
    if not df.index.is_monotonic_increasing or ("date" in df.columns and not df["date"].is_monotonic_increasing):
        df = df.sort_values(["symbol", "date"]).reset_index(drop=True)
    return df

def add_dollar_volume(df: pd.DataFrame) -> pd.DataFrame:
    df["dollar_volume"] = df["close"] * df["volume"]
    return df

def winsorize(s: pd.Series, p: float = 0.01) -> pd.Series:
    lo, hi = s.quantile([p, 1.0 - p])
    return s.clip(lower=lo, upper=hi)

def zscore_by_day(df: pd.DataFrame, col: str) -> pd.Series:
    # Cross-sectional z-score per day
    def _z(g):
        v = g[col]
        return (v - v.mean()) / (v.std(ddof=0) + 1e-9)
    return df.groupby("date", sort=False, group_keys=False).apply(_z)

def business_reindex(g: pd.DataFrame) -> pd.DataFrame:
    # Reindex each symbol to business days to compute gaps/forward returns robustly
    g = g.set_index("date").sort_index()
    idx = pd.date_range(g.index.min(), g.index.max(), freq="B")
    g = g.reindex(idx)
    g.index.name = "date"
    g["symbol"] = g["symbol"].ffill()
    return g.reset_index()
