from __future__ import annotations
import numpy as np
import pandas as pd

def _winsorize(s: pd.Series, p: float = 0.01) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    lo, hi = s.quantile(p), s.quantile(1-p)
    return s.clip(lo, hi)

def _zscore(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce").fillna(0.0)
    m, sd = s.mean(), s.std(ddof=0)
    if not np.isfinite(sd) or sd == 0:
        return pd.Series(np.zeros(len(s)), index=s.index)
    return (s - m) / (sd + 1e-12)

def _minmax01(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    mn, mx = s.min(), s.max()
    if not np.isfinite(mn) or not np.isfinite(mx) or mx - mn < 1e-12:
        return pd.Series(np.full(len(s), 0.5), index=s.index)
    return (s - mn) / (mx - mn)

def calibrate_columns(df: pd.DataFrame, cols: list[str], p: float = 0.01) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        if c in out:
            out[c] = _minmax01(_zscore(_winsorize(out[c], p)))
    return out

def _layer_mean(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    used = [c for c in cols if c in df.columns]
    if not used:
        return pd.Series(0.5, index=df.index)
    layer = df[used].replace([np.inf, -np.inf], np.nan).fillna(0.5)
    return layer.mean(axis=1)

def layered_scores(df: pd.DataFrame, schema: dict) -> pd.DataFrame:
    out = df.copy()
    for layer, cols in schema.items():
        out[f"score_{layer}"] = _layer_mean(out, cols)
    return out

def combine_layers(df: pd.DataFrame, weights: dict) -> pd.Series:
    score = pd.Series(0.0, index=df.index)
    for layer, w in weights.items():
        col = f"score_{layer}"
        if col in df.columns:
            score = score + w * df[col].fillna(0.5)
    mn, mx = score.min(), score.max()
    if mx - mn < 1e-12:
        return pd.Series(np.full(len(score), 0.5), index=df.index)
    return (score - mn) / (mx - mn)

def risk_flags(df: pd.DataFrame) -> pd.Series:
    flags = []
    for _, r in df.iterrows():
        f = []
        if r.get('ema_fast', np.nan) < r.get('ema_slow', np.inf):
            f.append('EMA_down')
        if r.get('macd', 0.0) < 0:
            f.append('MACD<0')
        if r.get('choch', 0) < 0:
            f.append('CHOCH-')
        flags.append(','.join(f))
    return pd.Series(flags, index=df.index)
