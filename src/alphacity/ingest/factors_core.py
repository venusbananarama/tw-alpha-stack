
import numpy as np
import pandas as pd

REQUIRED_COLS = ["date","symbol","adj_close","close","volume"]

def ensure_schema(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure required columns exist and have correct dtypes."""
    for c in REQUIRED_COLS:
        if c not in df.columns:
            raise ValueError(f"missing required column: {c}")
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out = out.sort_values(["symbol","date"]).reset_index(drop=True)
    return out

def add_returns(df: pd.DataFrame) -> pd.DataFrame:
    """Add daily simple returns based on adj_close by symbol."""
    df = df.copy()
    df["ret"] = df.groupby("symbol")["adj_close"].pct_change()
    return df

def momentum_lookback(df: pd.DataFrame, lookback=252, skip=21, col="adj_close", outname=None) -> pd.DataFrame:
    """
    Classic 12M-1M momentum: price(t-skip) / price(t-skip-lookback) - 1.
    """
    if outname is None:
        outname = f"mom_{lookback}_{skip}"
    g = df.groupby("symbol")
    p = g[col].apply(lambda s: s.shift(skip) / s.shift(skip+lookback) - 1.0)
    out = df.copy()
    out[outname] = p.values
    return out

def volatility(df: pd.DataFrame, window=20, ret_col="ret", outname=None, ann_factor=252):
    """Rolling realized volatility of returns."""
    if outname is None:
        outname = f"vol_{window}"
    g = df.groupby("symbol")[ret_col].rolling(window).std().reset_index(level=0, drop=True)
    out = df.copy()
    out[outname] = g.values * np.sqrt(ann_factor)
    return out

def liquidity(df: pd.DataFrame, window=20, outname=None):
    """Rolling average log dollar volume; robust liquidity proxy."""
    if outname is None:
        outname = f"liq_{window}"
    dv = np.log1p(df["close"] * df["volume"].astype(float))
    g = df.assign(_dv=dv).groupby("symbol")["_dv"].rolling(window).mean().reset_index(level=0, drop=True)
    out = df.copy()
    out[outname] = g.values
    return out

def xsec_zscore(df: pd.DataFrame, col: str, clip=5.0):
    """Cross-sectional z-score by date with optional clipping."""
    def _z(g):
        s = g[col]
        mu, sd = s.mean(), s.std()
        if sd == 0 or np.isnan(sd):
            return pd.Series(0.0, index=s.index)
        z = (s - mu) / sd
        if clip:
            z = z.clip(-clip, clip)
        return z
    return df.groupby("date", group_keys=False).apply(_z)

def composite_from_config(df: pd.DataFrame, weights: dict, clip=5.0) -> pd.Series:
    """Combine factor columns by cross-sectional z-scores and weights."""
    comp = pd.Series(0.0, index=df.index)
    for col, w in weights.items():
        z = xsec_zscore(df, col, clip=clip)
        comp = comp.add(w * z, fill_value=0.0)
    return comp
